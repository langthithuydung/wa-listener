import os
import json
import boto3
from botocore.config import Config
from supabase import create_client
from datetime import datetime, timezone

# ── Supabase ─────────────────────────────────────────
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# ── R2 ───────────────────────────────────────────────
def get_r2_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version='s3v4')
    )

BUCKET = os.getenv("R2_BUCKET_NAME")

# ── Lưu event mới vào Supabase ───────────────────────
def save_event(parsed: dict, raw_text: str, source_channel: str, msg_id: int):
    symbol = parsed.get("symbol") or None

    # Dedupe: có symbol → check trùng theo symbol
    if symbol:
        try:
            existing = supabase.table("alpha_events") \
                .select("id") \
                .eq("symbol", symbol) \
                .execute()
            if existing.data:
                print(f"[storage] Skip duplicate symbol: {symbol}")
                return
        except Exception as e:
            print(f"[storage] Dedupe check error: {e}")
    else:
        # Không có symbol → check trùng theo source_msg_id
        try:
            existing = supabase.table("alpha_events") \
                .select("id") \
                .eq("source_msg_id", msg_id) \
                .execute()
            if existing.data:
                print(f"[storage] Skip duplicate msg_id: {msg_id}")
                return
        except Exception as e:
            print(f"[storage] Dedupe check error: {e}")

    # Status: chưa có symbol → "pending" (chờ Binance công bố token)
    status = "upcoming" if symbol else "pending"

    data = {
        "project_name":     parsed.get("project_name"),
        "symbol":           symbol,
        "event_type":       parsed.get("event_type"),
        "points_threshold": parsed.get("points_threshold"),
        "amount_per_user":  parsed.get("amount_per_user"),
        "decay_rule":       parsed.get("decay_rule"),
        "event_time":       parsed.get("event_time_utc"),
        "status":           status,
        "source_channel":   source_channel,
        "source_msg_id":    msg_id,
        "raw_text":         raw_text,
        "created_at":       datetime.now(timezone.utc).isoformat()
    }

    try:
        supabase.table("alpha_events").insert(data).execute()
        print(f"[storage] Saved: symbol={symbol or 'TBA'}, status={status} → Supabase ✓")
        refresh_r2_snapshot()
    except Exception as e:
        print(f"[storage] Insert error: {e}")


# ── Ghi snapshot JSON lên R2 ─────────────────────────
def refresh_r2_snapshot():
    try:
        r2 = get_r2_client()
        all_events = supabase.table("alpha_events") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute().data

        pending  = [e for e in all_events if e["status"] == "pending"]
        upcoming = [e for e in all_events if e["status"] == "upcoming"]
        live     = [e for e in all_events if e["status"] == "live"]
        history  = [e for e in all_events if e["status"] == "ended"]

        files = {
            "alpha-events/pending.json":  pending,
            "alpha-events/upcoming.json": upcoming,
            "alpha-events/live.json":     live,
            "alpha-events/history.json":  history,
            "alpha-events/all.json":      all_events,
        }

        for key, data in files.items():
            r2.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=json.dumps(data, default=str, ensure_ascii=False,
                                separators=(',', ':')).encode('utf-8'),
                ContentType='application/json',
                CacheControl='max-age=60'
            )

        print(f"[storage] R2 snapshot updated — "
              f"pending={len(pending)}, upcoming={len(upcoming)}, "
              f"live={len(live)}, ended={len(history)} ✓")

    except Exception as e:
        print(f"[storage] R2 error: {e}")
