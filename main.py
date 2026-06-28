import asyncio
import os
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

from parser import parse_message
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
    print(f"[TEXT] {text[:200]}...")

    parsed = parse_message(text)
    if parsed:
        print(f"[PARSED] {parsed}")
        save_event(parsed, text, channel, msg_id)
    else:
        print("[skip] Không liên quan đến Alpha")

async def start_telegram():
    """Khởi động Telegram listener"""
    await client.start(phone=os.getenv("TELEGRAM_PHONE"))
    print("[Telegram] Connected ✓")
    print(f"[Telegram] Monitoring: {CHANNELS}")
    await client.run_until_disconnected()

def run_telegram_in_thread():
    """Chạy Telegram trong thread riêng song song với FastAPI"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_telegram())

# ── Start ────────────────────────────────────────────
if __name__ == "__main__":
    # Chạy Telegram listener trong background thread
    tg_thread = threading.Thread(target=run_telegram_in_thread, daemon=True)
    tg_thread.start()

    # Chạy FastAPI trên main thread
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)