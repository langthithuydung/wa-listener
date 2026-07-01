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
telegram_status = {"connected": False, "last_error": None, "restarts": 0}

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

# ── Test endpoint ─────────────────────────────────────
@app.get("/test")
def test():
    text = """
Binance Alpha's second wave of Collect on Fanable (COLLECT) airdrop rewards are here!
Users with at least 224 Binance Alpha Points can claim an airdrop of 800 COLLECT tokens
on a first-come, first-served basis. If the reward pool is not fully distributed, the score
threshold will automatically decrease by 5 points every 5 minutes. Please note that claiming
the airdrop will consume 15 Binance Alpha Points.
"""
    print("=" * 60)
    print("[TEST] Running parser...")
    parsed = parse_message(text)
    print("[PARSED]", parsed)

    if parsed:
        test_msg_id = random.randint(100000000, 999999998)
        save_event(
            parsed=parsed,
            raw_text=text,
            source_channel="binance_wallet_announcements",
            msg_id=test_msg_id
        )
        print(f"[TEST] Saved (msg_id={test_msg_id})")

    return {"success": parsed is not None, "parsed": parsed}

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

    parsed = parse_message(text)
    if parsed:
        print(f"[PARSED] {parsed}")
        save_event(parsed, text, channel, msg_id)
    else:
        print("[skip] Không liên quan Alpha hoặc thiếu event_type")

async def start_telegram():
    await client.start(phone=os.getenv("TELEGRAM_PHONE"))
    telegram_status["connected"] = True
    telegram_status["last_error"] = None
    print("[Telegram] Connected ✓")
    print(f"[Telegram] Monitoring: {CHANNELS}")
    await client.run_until_disconnected()

def run_telegram_in_thread():
    while True:
        try:
            print(f"[Telegram] Starting... (attempt #{telegram_status['restarts'] + 1})")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
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