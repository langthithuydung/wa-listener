import asyncio
import os
import traceback
import time
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

# ── Config ──────────────────────────────────────────
API_ID   = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS = [
    "binance_wallet_announcements",  # Priority 1
    "binance_announcements",         # Priority 2
]

# ── FastAPI (để Render không kill service) ──────────
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

# ── Telegram status (để debug) ──────────────────────
telegram_status = {"connected": False, "last_error": None, "restarts": 0}

@app.get("/telegram-status")
def tg_status():
    return telegram_status

# ===========================
# TEST ENDPOINT
# ===========================

@app.get("/test")
def test():
    text = """
Please get ready to claim the Binance Alpha airdrop and trade today at 10:00 (UTC).
Users with at least 224 Binance Alpha Points can claim the token on a first-come,
first-served basis until the airdrop pool is fully distributed or the airdrop event expires.
Further details will be announced soon. Please stay tuned to Binance's official channels
for the specific airdrop tokens and the latest updates.
"""

    print("=" * 80)
    print("[TEST] Running parser...")

    parsed = parse_message(text)

    print("[PARSED]", parsed)

    if parsed:
        save_event(
            parsed=parsed,
            raw_text=text,
            source_channel="binance_wallet_announcements",
            msg_id=999999999
        )
        print("[TEST] Saved to Supabase + R2")

    return {
        "success": parsed is not None,
        "parsed": parsed
    }

# ── Telegram Listener ────────────────────────────────
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
        print("[skip] Không liên quan đến Alpha hoặc thiếu event_type")

async def start_telegram():
    """Khởi động Telegram listener"""
    await client.start(phone=os.getenv("TELEGRAM_PHONE"))
    telegram_status["connected"] = True
    telegram_status["last_error"] = None
    print("[Telegram] Connected ✓")
    print(f"[Telegram] Monitoring: {CHANNELS}")
    await client.run_until_disconnected()

def run_telegram_in_thread():
    """Chạy Telegram với auto-restart khi crash"""
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
            print(f"[Telegram] Reconnecting in 30s... (total restarts: {telegram_status['restarts']})")
            time.sleep(30)

# ── Start ────────────────────────────────────────────
if __name__ == "__main__":
    # Chạy Telegram listener trong background thread
    tg_thread = threading.Thread(target=run_telegram_in_thread, daemon=True)
    tg_thread.start()

    # Chạy FastAPI trên main thread
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)