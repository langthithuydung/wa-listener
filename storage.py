import os
import json
import boto3
from supabase import create_client
from datetime import datetime, timezone

# Supabase
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# R2 (dùng boto3 với S3-compatible endpoint)
r2 = boto3.client(
    "s3",
    endpoint_url=f"https://{os.getenv('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
    aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    region_name="auto"
)
BUCKET = os.getenv("R2_BUCKET_NAME")

def save_event(parsed: dict, raw_text: str, source_channel: str, msg_id: int):
    """Lưu event mới vào Supabase"""
    data = {
        "project_name":     parsed.get("project_name"),
        "symbol":           parsed.get("symbol"),
        "event_type":       parsed.get("event_type"),
        "points_threshold": parsed.get("points_threshold"),
        "amount_per_user":  parsed.get("amount_per_user"),
        "decay_rule":       parsed.get("decay_rule"),
        "event_time":       parsed.get("event_time_utc"),
        "status":           "upcoming",
        "source_channel":   source_channel,
        "source_msg_id":    msg_id,
        "raw_text":         raw_text,
        "created_at":       datetime.now(timezone.utc).isoformat()
    }
    
    # Dedupe: bỏ qua nếu symbol + ngày đã có
    existing = supabase.table("alpha_events")\
        .select("id")\
        .eq("symbol", parsed.get("symbol", ""))\
        .execute()
    
    if existing.data:
        print(f"[skip] {parsed.get('symbol')} đã tồn tại")
        return
    
    result = supabase.table("alpha_events").insert(data).execute()
    print(f"[saved] {parsed.get('symbol')} → Supabase")
    
    # Sau khi lưu, refresh R2 snapshot
    refresh_r2_snapshot()

def refresh_r2_snapshot():
    """Đọc DB, ghi lại JSON lên R2"""
    try:
        all_events = supabase.table("alpha_events")\
            .select("*")\
            .order("event_time", desc=True)\
            .execute().data
        
        upcoming = [e for e in all_events if e["status"] == "upcoming"]
        live     = [e for e in all_events if e["status"] == "live"]
        history  = [e for e in all_events if e["status"] == "ended"]
        
        for key, data in [
            ("events/upcoming.json", upcoming),
            ("events/live.json",     live),
            ("events/history.json",  history),
            ("events/all.json",      all_events)
        ]:
            r2.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=json.dumps(data, default=str),
                ContentType="application/json"
            )
        print("[R2] Snapshot updated")
    except Exception as e:
        print(f"[R2 error] {e}")
