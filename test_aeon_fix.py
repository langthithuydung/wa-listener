from dotenv import load_dotenv
load_dotenv()
import os
print("SUPABASE_URL set:", bool(os.getenv("SUPABASE_URL")))
print("SUPABASE_KEY set:", bool(os.getenv("SUPABASE_KEY")))
from datetime import datetime, timezone
from alpha_parser import parse_message
from storage import save_event

now = datetime.now(timezone.utc)
tin1 = "Binance Alpha will be the first platform to feature ZTest (ZTEST) on July 27."
tin2 = ("Binance Alpha is the first platform to feature ZTest (ZTEST), with Alpha debut and "
    "trading starting on July 27, 2026, at 10:00 (UTC). Users with at least 245 Binance "
    "Alpha Points can claim an airdrop of 250 ZTEST tokens on a first-come, first-served "
    "basis. If the reward pool is not fully distributed, the score threshold will "
    "automatically decrease by 5 points every 5 minutes. Please note that claiming the "
    "airdrop will consume 15 Binance Alpha Points. Users must confirm their claim on the "
    "Alpha Events page within 24 hours; otherwise, it will be deemed that users have given "
    "up claiming the airdrop.")

p1 = parse_message(tin1, msg_date=now)
print("P1:", p1)
save_event(p1, tin1, "test_channel", 90001, msg_date=now)

p2 = parse_message(tin2, msg_date=now)
print("P2:", p2)
save_event(p2, tin2, "test_channel", 90002, msg_date=now)
