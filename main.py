import asyncio
import os
import traceback
import time
import random
from dotenv import load_dotenv
load_dotenv()

import base64
session_b64 = os.getenv("SESSION_BASE64")
if session_b64 and not os.path.exists("session_wave_alpha.session"):
    with open("session_wave_alpha.session", "wb") as f:
        f.write(base64.b64decode(session_b64))

from fastapi import FastAPI, Response
from telethon import TelegramClient, events
import uvicorn
import threading

from alpha_parser import parse_message
from storage import save_event, refresh_r2_snapshot
from scheduler import start_scheduler

# ── Config ───────────────────────────────────────────
API_ID   = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS = [
    "binance_wallet_announcements",
    "binance_announcements",
]

# Tự động catch-up lại mỗi bao nhiêu giây — đây là fix chính cho việc phải
# tự tay gọi /catchup. Không cần con người can thiệp nữa.
AUTO_CATCHUP_INTERVAL_SECONDS = 120  # 2 phút

# ── FastAPI ───────────────────────────────────────────
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok", "service": "wave-alpha-listener"}

@app.head("/health")
def head_health():
    return Response(status_code=200)

@app.get("/")
def root():
    return {"status": "running"}

@app.head("/")
def head_root():
    return Response(status_code=200)

# ── Debug endpoints ───────────────────────────────────
telegram_status = {
    "connected": False,
    "last_error": None,
    "restarts": 0,
    "last_auto_catchup": None,
    "auto_catchup_count": 0,
}

@app.get("/telegram-status")
def tg_status():
    return telegram_status

@app.get("/refresh")
def refresh():
    try:
        refresh_r2_snapshot()
        return {"success": True, "message": "R2 snapshot refreshed"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/run/expire")
def run_expire():
    """Chạy auto_expire thủ công."""
    try:
        from scheduler import job_auto_expire
        job_auto_expire()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/run/enrich")
def run_enrich():
    """Chạy enrich_prices thủ công."""
    try:
        from scheduler import job_enrich_prices
        job_enrich_prices()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/run/poll")
def run_poll():
    """Chạy announcement poller thủ công."""
    try:
        from scheduler import job_poll_announcements
        job_poll_announcements()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _process_one_message(channel: str, msg_id: int, text: str, msg_date=None):
    """
    Xử lý 1 tin nhắn: check đã xử lý chưa → relevant → parse → save.
    LUÔN đánh dấu processed + cập nhật checkpoint sau khi check xong,
    để tránh gọi Gemini lặp lại vô hạn và đảm bảo catch-up không bỏ sót tin.
    msg_date: thời gian THẬT tin được đăng trên Telegram (dùng để quy đổi
    các mốc thời gian tương đối kiểu "today at 9:00 UTC" thành ngày giờ
    tuyệt đối trong parser).
    """
    from alpha_parser import is_relevant
    from storage import _is_message_processed, _mark_message_processed, update_channel_checkpoint

    if _is_message_processed(channel, msg_id):
        return None

    if not is_relevant(text):
        _mark_message_processed(channel, msg_id)
        update_channel_checkpoint(channel, msg_id)
        return None

    parsed = parse_message(text, msg_date=msg_date)

    _mark_message_processed(channel, msg_id)
    update_channel_checkpoint(channel, msg_id)

    if parsed:
        save_event(parsed, text, channel, msg_id, msg_date=msg_date)
        return parsed
    return None


async def _do_catchup_scan(source: str = "manual"):
    """
    Logic catch-up dùng chung cho: endpoint /catchup (gọi tay), lúc khởi
    động (_run_catchup_on_start), VÀ job tự động lặp lại mỗi 2 phút
    (_auto_catchup_loop). Trước đây 3 nơi này viết code trùng lặp riêng —
    giờ gộp lại 1 chỗ duy nhất, sửa 1 lần là cả 3 chỗ đều đúng.
    """
    from storage import get_channel_checkpoint
    from alpha_parser import is_relevant

    results = []
    scanned_preview = []
    for ch in CHANNELS:
        try:
            entity = await client.get_entity(ch)
            checkpoint = get_channel_checkpoint(ch)

            if checkpoint > 0:
                messages = await client.get_messages(entity, min_id=checkpoint, limit=200)
            else:
                messages = await client.get_messages(entity, limit=15)

            for m in reversed(messages):
                if not m.message:
                    continue
                if is_relevant(m.message):
                    scanned_preview.append({
                        "channel": ch, "msg_id": m.id,
                        "date": m.date.isoformat() if m.date else None,
                        "preview": m.message[:200]
                    })
                parsed = await _process_one_message(ch, m.id, m.message, msg_date=m.date)
                if parsed:
                    results.append({
                        "channel": ch, "msg_id": m.id,
                        "date": m.date.isoformat() if m.date else None,
                        "parsed": parsed
                    })
        except Exception as e:
            results.append({"channel": ch, "error": str(e)})
            print(f"[catchup:{source}] Error scanning {ch}: {e}")

    if results:
        print(f"[catchup:{source}] Saved {len(results)} event(s): "
              f"{[r.get('parsed', {}).get('symbol') or 'TBA' for r in results if 'parsed' in r]}")

    return {"saved_events": results, "all_relevant_scanned": scanned_preview}


@app.get("/catchup")
def catchup():
    """Quét lại tin nhắn thủ công (vẫn giữ lại để debug/kiểm tra khi cần,
    nhưng KHÔNG còn là cách duy nhất để hệ thống bắt tin nữa — xem
    _auto_catchup_loop() chạy tự động mỗi 2 phút bên dưới)."""
    import asyncio

    if telegram_loop is None:
        return {"error": "Telegram loop not ready yet, try again in a few seconds"}

    try:
        future = asyncio.run_coroutine_threadsafe(_do_catchup_scan("manual"), telegram_loop)
        result = future.result(timeout=90)
        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e) or type(e).__name__}


@app.get("/debug/channels")
def debug_channels():
    """
    Kiểm tra: account session có đang join channel không,
    và lấy 10 tin nhắn gần nhất để verify bot thực sự nhận được gì.
    Chạy trên đúng event loop của Telegram client (bắt buộc với Telethon).
    """
    import asyncio

    if telegram_loop is None:
        return {"error": "Telegram loop not ready yet, try again in a few seconds"}

    async def _check():
        result = {"channels": {}}
        for ch in CHANNELS:
            try:
                entity = await client.get_entity(ch)
                messages = await client.get_messages(entity, limit=10)
                result["channels"][ch] = {
                    "joined": True,
                    "entity_title": getattr(entity, "title", str(entity)),
                    "recent_messages": [
                        {
                            "id": m.id,
                            "date": m.date.isoformat() if m.date else None,
                            "text_preview": (m.message or "")[:150],
                        }
                        for m in messages if m.message
                    ]
                }
            except Exception as e:
                result["channels"][ch] = {
                    "joined": False,
                    "error": str(e)
                }
        return result

    try:
        future = asyncio.run_coroutine_threadsafe(_check(), telegram_loop)
        result = future.result(timeout=20)
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/run/blindbox")
def run_blindbox():
    """Chạy blind box detector thủ công."""
    try:
        from scheduler import job_blind_box_detect
        job_blind_box_detect()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── Test endpoint ─────────────────────────────────────
@app.get("/test")
def test():
    text = """
Please get ready to claim the Binance Alpha airdrop and trade today at 10:00 (UTC).
Users with at least 224 Binance Alpha Points can claim the token on a first-come,
first-served basis until the airdrop pool is fully distributed or the airdrop event expires.
Further details will be announced soon. Please stay tuned to Binance's official channels
for the specific airdrop tokens and the latest updates.
"""
    print("=" * 60)
    print("[TEST] Running parser (dry-run, không lưu DB)...")
    parsed = parse_message(text)
    print("[PARSED]", parsed)

    return {"success": parsed is not None, "parsed": parsed, "note": "dry-run, không ghi vào Supabase"}


@app.get("/test-fix")
def test_fix():
    """
    Verify fix bug AEON: symbol đã tồn tại (status upcoming) nhưng thiếu
    points_threshold/amount_per_user/event_time → tin thứ 2 phải ENRICH
    thêm, không được bị "Skip duplicate symbol" bỏ qua như trước.
    Tạm thời, xoá route này sau khi verify xong.
    """
    from datetime import datetime, timezone
    from alpha_parser import parse_message
    from storage import save_event
    import time as _time

    now = datetime.now(timezone.utc)
    sym = f"ZTEST{int(_time.time())}"  # symbol duy nhất mỗi lần gọi, tránh đụng data cũ

    tin1 = f"Binance Alpha will be the first platform to feature ZTest ({sym}) on July 27."
    tin2 = (f"Binance Alpha is the first platform to feature ZTest ({sym}), with Alpha debut and "
        "trading starting on July 27, 2026, at 10:00 (UTC). Users with at least 245 Binance "
        f"Alpha Points can claim an airdrop of 250 {sym} tokens on a first-come, first-served "
        "basis. If the reward pool is not fully distributed, the score threshold will "
        "automatically decrease by 5 points every 5 minutes. Please note that claiming the "
        "airdrop will consume 15 Binance Alpha Points.")

    p1 = parse_message(tin1, msg_date=now)
    save_event(p1, tin1, "test_channel", int(_time.time() * 1000) % 999999999, msg_date=now)

    p2 = parse_message(tin2, msg_date=now)
    save_event(p2, tin2, "test_channel", int(_time.time() * 1000) % 999999999 + 1, msg_date=now)

    from supabase import create_client
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    row = sb.table("alpha_events").select("*").eq("symbol", sym).execute().data
    if row:
        sb.table("alpha_events").delete().eq("symbol", sym).execute()  # dọn ngay sau khi đọc
    return {"symbol": sym, "final_row": row}


# ── Telegram Listener ─────────────────────────────────
client = TelegramClient("session_wave_alpha", API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNELS))
async def on_message(event):
    text = event.message.message
    if not text:
        return

    channel = event.chat.username or str(event.chat_id)
    msg_id  = event.message.id

    print(f"\n[MSG] #{msg_id} from @{channel}")
    print(f"[TEXT] {text[:300]}")

    parsed = await _process_one_message(channel, msg_id, text, msg_date=event.message.date)
    if parsed:
        print(f"[PARSED] {parsed}")
    else:
        print("[skip] Không liên quan Alpha hoặc thiếu event_type")


async def _auto_catchup_loop():
    """
    QUAN TRỌNG — đây là fix chính cho việc phải tự tay gọi /catchup.
    Tự động quét lại tin nhắn mỗi AUTO_CATCHUP_INTERVAL_SECONDS (2 phút),
    dùng CHUNG logic với /catchup và _run_catchup_on_start (qua
    _do_catchup_scan). Bắt được cả:
      - Tin đến đúng lúc bot đang restart/mất kết nối (realtime listener
        không kịp bắt)
      - Tin bị lỡ vì bất kỳ lý do gì khác (mạng chập chờn, Telethon miss
        event...)
    Chạy song song với client.run_until_disconnected() trên cùng event loop.
    """
    while True:
        await asyncio.sleep(AUTO_CATCHUP_INTERVAL_SECONDS)
        try:
            result = await _do_catchup_scan("auto")
            telegram_status["last_auto_catchup"] = time.time()
            telegram_status["auto_catchup_count"] += 1
            if result.get("saved_events"):
                print(f"[auto_catchup] ⚡ Bắt được {len(result['saved_events'])} sự kiện bị lỡ!")
        except Exception as e:
            print(f"[auto_catchup] Error: {e}")


async def _run_catchup_on_start():
    """Quét lại tin từ checkpoint ngay khi bot khởi động/reconnect."""
    try:
        await _do_catchup_scan("startup")
        print("[Telegram] Catch-up scan complete ✓")
    except Exception as e:
        print(f"[Telegram] Catch-up error: {e}")


async def start_telegram():
    await client.start(phone=os.getenv("TELEGRAM_PHONE"))
    telegram_status["connected"] = True
    telegram_status["last_error"] = None
    print("[Telegram] Connected ✓")
    print(f"[Telegram] Monitoring: {CHANNELS}")

    # Cơ chế catch-up chính thức của Telethon (dùng update state đã lưu) —
    # lớp bảo vệ đầu tiên, bổ sung cho _run_catchup_on_start bên dưới.
    try:
        await client.catch_up()
    except Exception as e:
        print(f"[Telegram] client.catch_up() error (bỏ qua, không nghiêm trọng): {e}")

    await _run_catchup_on_start()

    # Tự động lặp lại catch-up mỗi 2 phút — CHẠY SONG SONG với
    # run_until_disconnected(), không cần con người tự gọi /catchup nữa.
    asyncio.create_task(_auto_catchup_loop())

    await client.run_until_disconnected()

telegram_loop = None

def run_telegram_in_thread():
    global telegram_loop
    while True:
        try:
            print(f"[Telegram] Starting... (attempt #{telegram_status['restarts'] + 1})")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            telegram_loop = loop
            loop.run_until_complete(start_telegram())
        except Exception as e:
            telegram_status["connected"] = False
            telegram_status["last_error"] = str(e)
            telegram_status["restarts"] += 1
            print(f"[Telegram] ❌ CRASHED: {e}")
            traceback.print_exc()
            print(f"[Telegram] Reconnecting in 30s...")
            time.sleep(30)

# ── Start ─────────────────────────────────────────────
if __name__ == "__main__":
    # 1. APScheduler (poll + enrich + expire)
    start_scheduler()

    # 2. Telegram listener
    tg_thread = threading.Thread(target=run_telegram_in_thread, daemon=True)
    tg_thread.start()

    # 3. FastAPI
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)