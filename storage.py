import os
import json
import boto3
from botocore.config import Config
from supabase import create_client
from datetime import datetime, timezone, timedelta

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


def _find_pending_match(parsed: dict) -> dict | None:
    """
    Tìm row 'pending' trong 48h gần nhất có thể match với tin mới.
    Match khi: cùng event_type VÀ (cùng points_threshold HOẶC tin mới có symbol).
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        rows = supabase.table("alpha_events") \
            .select("*") \
            .eq("status", "pending") \
            .gte("created_at", cutoff) \
            .order("created_at", desc=True) \
            .execute().data

        if not rows:
            return None

        event_type = parsed.get("event_type")
        points     = parsed.get("points_threshold")
        symbol     = parsed.get("symbol")

        for row in rows:
            if row.get("event_type") != event_type:
                continue
            if symbol or (points and row.get("points_threshold") == points):
                return row

        return None
    except Exception as e:
        print(f"[storage] find_pending error: {e}")
        return None


def save_event(parsed: dict, raw_text: str, source_channel: str, msg_id: int):
    symbol     = parsed.get("symbol") or None
    event_type = parsed.get("event_type")

    # ── Bước 1: Nếu có symbol → thử update row pending trước ─────────
    if symbol:
        pending_row = _find_pending_match(parsed)
        if pending_row:
            try:
                update_data = {
                    "symbol":           symbol,
                    "project_name":     parsed.get("project_name") or pending_row.get("project_name"),
                    "points_threshold": parsed.get("points_threshold") or pending_row.get("points_threshold"),
                    "points_cost":      parsed.get("points_cost") or pending_row.get("points_cost"),
                    "amount_per_user":  parsed.get("amount_per_user") or pending_row.get("amount_per_user"),
                    "total_amount":     parsed.get("total_amount") or pending_row.get("total_amount"),
                    "decay_rule":       parsed.get("decay_rule") or pending_row.get("decay_rule"),
                    "event_time":       parsed.get("event_time_utc") or pending_row.get("event_time"),
                    "chain_id":         parsed.get("chain_id") or pending_row.get("chain_id") or "56",
                    "chain_name":       parsed.get("chain_name") or pending_row.get("chain_name") or "BSC",
                    "fdv":              parsed.get("fdv") or pending_row.get("fdv"),
                    "phase":            parsed.get("phase") or pending_row.get("phase"),
                    "spot_listed":      parsed.get("spot_listed") or pending_row.get("spot_listed") or False,
                    "futures_listed":   parsed.get("futures_listed") or pending_row.get("futures_listed") or False,
                    "completed":        parsed.get("completed") or pending_row.get("completed") or False,
                    "pretge":           parsed.get("pretge") or pending_row.get("pretge") or False,
                    "status":           "upcoming",
                    "source_msg_id":    msg_id,
                    "raw_text":         raw_text,
                }
                supabase.table("alpha_events") \
                    .update(update_data) \
                    .eq("id", pending_row["id"]) \
                    .execute()
                print(f"[storage] Updated pending→upcoming: id={pending_row['id']} symbol={symbol} ✓")
                refresh_r2_snapshot()
                return
            except Exception as e:
                print(f"[storage] Update pending error: {e}")

    # ── Bước 2: Dedupe trước khi insert mới ──────────────────────────
    if symbol:
        try:
            existing = supabase.table("alpha_events") \
                .select("id").eq("symbol", symbol).execute()
            if existing.data:
                print(f"[storage] Skip duplicate symbol: {symbol}")
                return
        except Exception as e:
            print(f"[storage] Dedupe check error: {e}")
    else:
        try:
            existing = supabase.table("alpha_events") \
                .select("id").eq("source_msg_id", msg_id).execute()
            if existing.data:
                print(f"[storage] Skip duplicate msg_id: {msg_id}")
                return
        except Exception as e:
            print(f"[storage] Dedupe check error: {e}")

    # ── Bước 3: Insert mới ───────────────────────────────────────────
    status = "upcoming" if symbol else "pending"

    data = {
        "project_name":   parsed.get("project_name"),
        "symbol":         symbol,
        "event_type":     event_type,
        "points_threshold": parsed.get("points_threshold"),
        "points_cost":    parsed.get("points_cost"),
        "amount_per_user": parsed.get("amount_per_user"),
        "total_amount":   parsed.get("total_amount"),
        "decay_rule":     parsed.get("decay_rule"),
        "event_time":     parsed.get("event_time_utc"),
        "chain_id":       parsed.get("chain_id") or "56",
        "chain_name":     parsed.get("chain_name") or "BSC",
        "contract_address": parsed.get("contract_address"),
        "fdv":            parsed.get("fdv"),
        "price_snapshot": parsed.get("price_snapshot"),
        "value_usd":      parsed.get("value_usd"),
        "market_cap":     parsed.get("market_cap"),
        "phase":          parsed.get("phase"),
        "spot_listed":    parsed.get("spot_listed") or False,
        "futures_listed": parsed.get("futures_listed") or False,
        "completed":      parsed.get("completed") or False,
        "pretge":         parsed.get("pretge") or False,
        "status":         status,
        "source_channel": source_channel,
        "source_msg_id":  msg_id,
        "raw_text":       raw_text,
        "created_at":     datetime.now(timezone.utc).isoformat()
    }

    try:
        supabase.table("alpha_events").insert(data).execute()
        print(f"[storage] Inserted: symbol={symbol or 'TBA'}, status={status} ✓")
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

        # KHÔNG ghi đè history.json — do sync_alpha_history.py quản lý
        files = {
            "alpha-events/pending.json":  pending,
            "alpha-events/upcoming.json": upcoming,
            "alpha-events/live.json":     live,
        }

        def put(key, data):
            r2.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=json.dumps(data, default=str, ensure_ascii=False,
                                separators=(',', ':')).encode('utf-8'),
                ContentType='application/json',
                CacheControl='max-age=60'
            )

        for key, data in files.items():
            put(key, data)

        print(f"[storage] R2 updated — pending={len(pending)}, upcoming={len(upcoming)}, live={len(live)} ✓")

    except Exception as e:
        print(f"[storage] R2 error: {e}")