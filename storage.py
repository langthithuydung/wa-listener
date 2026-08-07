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

        now = datetime.now(timezone.utc)
        for row in rows:
            if row.get("event_type") != event_type:
                continue

            # Ràng buộc thời gian: reveal phải đến trong vòng 12h sau khi pending được tạo
            # (Binance Alpha Box thường reveal trong vài giờ, không bao giờ để cách ngày)
            try:
                row_created = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
                hours_since_pending = (now - row_created).total_seconds() / 3600
                if hours_since_pending > 12:
                    continue  # pending đã quá cũ, không phải cùng 1 sự kiện
            except Exception:
                continue

            # Match CHẶT: bắt buộc điểm số trùng khớp (cho phép sai lệch do decay)
            row_points = row.get("points_threshold")
            if points is not None and row_points is not None:
                if abs(row_points - points) <= 20:
                    return row
                continue
            continue

        return None
    except Exception as e:
        print(f"[storage] find_pending error: {e}")
        return None


def _is_message_processed(source_channel: str, msg_id: int) -> bool:
    """Check bảng processed_messages riêng, độc lập với alpha_events mutations."""
    try:
        rows = supabase.table("processed_messages") \
            .select("id") \
            .eq("source_channel", source_channel) \
            .eq("msg_id", msg_id) \
            .execute().data
        return len(rows) > 0
    except Exception as e:
        print(f"[storage] processed_messages check error: {e}")
        return False


def _mark_message_processed(source_channel: str, msg_id: int):
    try:
        supabase.table("processed_messages").insert({
            "source_channel": source_channel,
            "msg_id": msg_id,
        }).execute()
    except Exception:
        pass  # đã tồn tại (unique constraint) → bỏ qua


def get_channel_checkpoint(source_channel: str) -> int:
    """Lấy msg_id lớn nhất đã xử lý của channel — dùng làm min_id khi quét tiếp."""
    try:
        rows = supabase.table("channel_checkpoints") \
            .select("last_msg_id") \
            .eq("source_channel", source_channel) \
            .execute().data
        if rows:
            return rows[0]["last_msg_id"]
    except Exception as e:
        print(f"[storage] get_checkpoint error: {e}")
    return 0


def update_channel_checkpoint(source_channel: str, msg_id: int):
    """Cập nhật checkpoint nếu msg_id mới lớn hơn."""
    try:
        current = get_channel_checkpoint(source_channel)
        if msg_id > current:
            supabase.table("channel_checkpoints").upsert({
                "source_channel": source_channel,
                "last_msg_id": msg_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }, on_conflict="source_channel").execute()
    except Exception as e:
        print(f"[storage] update_checkpoint error: {e}")


def _confirm_matching_candidates(alpha_event_id: int, symbol: str, symbols_all: str = None):
    """
    Khi Binance công bố chính thức, tìm blind_box_candidates đã phát hiện on-chain
    trước đó (giống alpha123.uk) khớp symbol → đánh dấu confirmed + link event.
    """
    all_syms = (symbols_all or symbol or "").split(",")
    all_syms = [s.strip().upper() for s in all_syms if s.strip()]
    if not all_syms:
        return

    try:
        for sym in all_syms:
            rows = supabase.table("blind_box_candidates") \
                .select("id, symbol") \
                .eq("symbol", sym) \
                .eq("status", "candidate") \
                .execute().data
            for row in rows:
                supabase.table("blind_box_candidates") \
                    .update({
                        "status": "confirmed",
                        "alpha_event_id": alpha_event_id,
                        "confirmed_at": datetime.now(timezone.utc).isoformat(),
                    }) \
                    .eq("id", row["id"]) \
                    .execute()
                print(f"[storage] Confirmed prediction: {sym} → event id={alpha_event_id} ✓")
    except Exception as e:
        print(f"[storage] Confirm candidates error: {e}")


def save_event(parsed: dict, raw_text: str, source_channel: str, msg_id: int, msg_date=None):
    """
    LƯU Ý QUAN TRỌNG: hàm này KHÔNG tự check/đánh dấu processed_messages nữa.
    Việc dedupe theo msg_id đã được _process_one_message() (main.py) lo TRƯỚC
    khi gọi hàm này rồi — nếu save_event tự check lại ở đây, nó sẽ luôn thấy
    "đã processed" (vì _process_one_message vừa mark xong ngay trước khi gọi
    save_event) và tự thoát, KHÔNG BAO GIỜ lưu được gì vào Supabase. Đây
    chính là bug đã khiến hệ thống không lưu được sự kiện nào suốt từ 30/6.
    """
    symbol     = parsed.get("symbol") or None
    event_type = parsed.get("event_type")
    # Dùng thời gian THẬT của tin nhắn Telegram nếu có, không dùng giờ hiện tại
    # (quan trọng cho catch-up: tránh tin cũ bị coi là "mới" trong tính expire)
    effective_created_at = msg_date.isoformat() if msg_date else datetime.now(timezone.utc).isoformat()

    # ── Bước 1: Nếu có symbol → thử update row pending trước ─────────
    if symbol:
        pending_row = _find_pending_match(parsed)
        if pending_row:
            try:
                update_data = {
                    "symbol":           symbol,
                    "symbols_all":      parsed.get("symbols_all") or pending_row.get("symbols_all"),
                    "tokens_detail":    parsed.get("tokens_detail") or pending_row.get("tokens_detail"),
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

                # Confirm blind box candidates đã phát hiện on-chain trước đó khớp với event này
                _confirm_matching_candidates(pending_row["id"], symbol, parsed.get("symbols_all"))

                refresh_r2_snapshot()
                return
            except Exception as e:
                print(f"[storage] Update pending error: {e}")

    # ── Bước 2: Dedupe THỰC SỰ trước khi insert mới (business dedupe, khác
    #    với message-dedupe ở main.py) ─────────────────────────────────
    if symbol:
        try:
            existing = supabase.table("alpha_events") \
                .select("*").eq("symbol", symbol).neq("status", "ended").execute()
            if existing.data:
                existing_row = existing.data[0]

                # Nếu event đã có đủ info cốt lõi rồi → đúng là tin trùng lặp
                # (VD Binance đăng lại), bỏ qua như cũ.
                has_full_info = (
                    existing_row.get("points_threshold")
                    and existing_row.get("amount_per_user")
                    and existing_row.get("event_time")
                )
                if has_full_info:
                    print(f"[storage] Skip duplicate symbol: {symbol}")
                    return

                # Ngược lại: symbol đã được công bố trước (VD Tin 1 chỉ có tên
                # token, "further details announced soon") nhưng còn thiếu
                # points/amount/event_time → tin mới (Tin 2) bổ sung phần còn
                # thiếu. Đây chính là case AEON: Tin 1 insert upcoming với
                # symbol nhưng toàn field khác NULL, _find_pending_match ở
                # Bước 1 không bắt được vì nó chỉ tìm status="pending", còn
                # row này đã là "upcoming" rồi → phải merge riêng ở đây.
                update_data = {
                    "symbols_all":      parsed.get("symbols_all") or existing_row.get("symbols_all"),
                    "tokens_detail":    parsed.get("tokens_detail") or existing_row.get("tokens_detail"),
                    "project_name":     parsed.get("project_name") or existing_row.get("project_name"),
                    "points_threshold": parsed.get("points_threshold") or existing_row.get("points_threshold"),
                    "points_cost":      parsed.get("points_cost") or existing_row.get("points_cost"),
                    "amount_per_user":  parsed.get("amount_per_user") or existing_row.get("amount_per_user"),
                    "total_amount":     parsed.get("total_amount") or existing_row.get("total_amount"),
                    "decay_rule":       parsed.get("decay_rule") or existing_row.get("decay_rule"),
                    "event_time":       parsed.get("event_time_utc") or existing_row.get("event_time"),
                    "chain_id":         parsed.get("chain_id") or existing_row.get("chain_id") or "56",
                    "chain_name":       parsed.get("chain_name") or existing_row.get("chain_name") or "BSC",
                    "phase":            parsed.get("phase") or existing_row.get("phase"),
                    "raw_text":         raw_text,
                    "source_msg_id":    msg_id,
                }
                supabase.table("alpha_events") \
                    .update(update_data) \
                    .eq("id", existing_row["id"]) \
                    .execute()
                print(f"[storage] Enriched existing event: id={existing_row['id']} symbol={symbol} ✓")
                refresh_r2_snapshot()
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
        "symbols_all":    parsed.get("symbols_all"),
        "tokens_detail":  parsed.get("tokens_detail"),
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
        "created_at":     effective_created_at
    }

    try:
        supabase.table("alpha_events").insert(data).execute()
        print(f"[storage] Inserted: symbol={symbol or 'TBA'}, status={status} ✓")
        refresh_r2_snapshot()
    except Exception as e:
        print(f"[storage] Insert error: {e}")


def _upsert_ended_to_all_events(ended_events: list):
    """
    Đẩy các event vừa 'ended' vào alpha-events/all.json — file mà
    sync_listing_prices.py (GitHub Actions) đọc để tự động fetch
    listing_price/VWAP/ATH qua Binance internal klines API.

    LÝ DO CẦN HÀM NÀY: all.json trước giờ chỉ được nạp bởi pipeline
    backfill cũ (fetch_alpha.py chạy 1 lần cho data lịch sử), hoàn toàn
    không biết gì về event realtime mà wa-listener bắt qua Telegram/
    Supabase. Hậu quả: các event mới (VD AEON, TRUTH...) không bao giờ
    được sync_listing_prices.py xử lý → mãi không có giá claim/đỉnh
    ("đang đồng bộ..." vĩnh viễn), và tệ hơn — lần tới script đó chạy,
    nó ghi đè history.json bằng list derive TỪ all.json, nên các event
    không có trong all.json sẽ bị XOÁ khỏi history.json luôn.

    Idempotent bằng source_msg_id, giống _append_ended_to_history.
    """
    if not ended_events:
        return
    try:
        r2 = get_r2_client()
        try:
            obj = r2.get_object(Bucket=BUCKET, Key="alpha-events/all.json")
            all_events = json.loads(obj["Body"].read())
            if not isinstance(all_events, list):
                all_events = []
        except Exception:
            all_events = []

        existing_by_msg_id = {
            e.get("source_msg_id"): e for e in all_events
            if e.get("source_msg_id") is not None
        }

        upserted = 0
        updated = 0
        for e in ended_events:
            msg_id = e.get("source_msg_id")
            existing = existing_by_msg_id.get(msg_id) if msg_id is not None else None

            if existing is not None:
                # [SỬA — TỔNG QUÁT HOÁ] Trước đây chỉ so sánh/ghi đè riêng
                # contract_address — nhưng CÙNG một kiểu bug (Supabase sửa
                # đúng rồi, R2 vẫn giữ giá trị cũ mãi mãi) cũng xảy ra với
                # amount_per_user/value_usd (case Alpha Box ON: bị lưu tier
                # Super Rare 1125 thay vì tier Common 315 do bug parser cũ,
                # backfill lại Supabase xong mà R2 không tự nhận). Giờ so
                # sánh một danh sách field "có thể tự sửa tay/backfill",
                # ghi đè bất cứ field nào khác với bản trên R2.
                changed_fields = []
                for field in ("contract_address", "amount_per_user", "value_usd",
                              "price_snapshot", "market_cap", "fdv", "tokens_detail"):
                    new_val = e.get(field)
                    if new_val is not None and new_val != existing.get(field):
                        existing[field] = new_val
                        changed_fields.append(field)

                # contract_address đổi → giá VWAP cũ tính theo contract SAI,
                # phải reset để sync_listing_prices.py bắt buộc tính lại.
                if "contract_address" in changed_fields:
                    existing["listing_price"] = None
                    for flag in ("_vwap_daybound_checked", "_tge_date_checked", "_multiround_checked"):
                        existing.pop(flag, None)

                if changed_fields:
                    updated += 1
                continue

            all_events.append(e)
            upserted += 1

        if upserted or updated:
            r2.put_object(
                Bucket=BUCKET,
                Key="alpha-events/all.json",
                Body=json.dumps(all_events, default=str).encode("utf-8"),
                ContentType="application/json"
            )
            print(f"[storage] all.json: {upserted} event(s) mới, {updated} event(s) đồng bộ lại field đã sửa trên Supabase ✓")
    except Exception as e:
        print(f"[storage] Upsert all.json error: {e}")


def _append_ended_to_history(ended_events: list):
    """
    Tự động thêm các event vừa chuyển 'ended' vào history.json trên R2 —
    KHÔNG cần chạy tay sync_alpha_history.py nữa. Script đó giờ đúng nghĩa
    "dự phòng": chỉ dùng khi cần nạp dữ liệu lịch sử CŨ (trước khi bot tồn
    tại) từ file airdrops.json bên ngoài, không còn liên quan gì tới luồng
    realtime nữa.

    Chống trùng bằng source_msg_id (mỗi tin Telegram chỉ có 1 msg_id duy
    nhất) — vì hàm này được gọi lại nhiều lần (mỗi lần refresh_r2_snapshot),
    phải đảm bảo không append lại event đã có sẵn trong history.json.
    """
    if not ended_events:
        return
    try:
        r2 = get_r2_client()
        try:
            obj = r2.get_object(Bucket=BUCKET, Key="alpha-events/history.json")
            history = json.loads(obj["Body"].read())
            if not isinstance(history, list):
                history = []
        except Exception:
            history = []  # chưa có file (lần đầu) → bắt đầu từ rỗng

        existing_msg_ids = {
            h.get("source_msg_id") for h in history
            if h.get("source_msg_id") is not None
        }
        # [MỚI] Index theo source_msg_id để có thể SỬA lại entry đã tồn tại
        # (trước đây chỉ có set id để check trùng, không tra ngược lại được
        # object nên không thể cập nhật contract_address khi Supabase sửa).
        existing_by_msg_id = {
            h.get("source_msg_id"): h for h in history
            if h.get("source_msg_id") is not None
        }

        appended = 0
        updated = 0
        for e in ended_events:
            msg_id = e.get("source_msg_id")
            if msg_id is not None and msg_id in existing_msg_ids:
                # [SỬA — TỔNG QUÁT HOÁ] Cùng logic với _upsert_ended_to_all_events:
                # so sánh 1 danh sách field "có thể tự sửa tay/backfill" trên
                # Supabase, ghi đè vào entry đã có sẵn trong history.json nếu
                # khác — bao trùm cả case contract_address (AEON) lẫn
                # amount_per_user/value_usd (case Alpha Box tier — ON bị lưu
                # nhầm tier Super Rare 1125 thay vì tier Common 315).
                existing_entry = existing_by_msg_id.get(msg_id)
                if existing_entry:
                    changed_fields = []
                    for field in ("contract_address", "amount_per_user", "value_usd",
                                  "price_snapshot", "market_cap", "fdv", "tokens_detail"):
                        new_val = e.get(field)
                        if new_val is not None and new_val != existing_entry.get(field):
                            existing_entry[field] = new_val
                            changed_fields.append(field)

                    if "contract_address" in changed_fields:
                        existing_entry["listing_price"] = None
                        for flag in ("_vwap_daybound_checked", "_tge_date_checked", "_multiround_checked"):
                            existing_entry.pop(flag, None)

                    if changed_fields:
                        updated += 1
                continue  # đã có trong history rồi
            history.append(e)
            if msg_id is not None:
                existing_msg_ids.add(msg_id)
            appended += 1

        # Luôn sort lại toàn bộ history theo thời gian MỚI NHẤT lên đầu,
        # bất kể lần này có append thêm event mới hay không — vì các event
        # đã append từ TRƯỚC KHI có fix sort này vẫn đang nằm sai vị trí
        # (cuối mảng) trong file history.json hiện có trên R2. Nếu chỉ sort
        # trong nhánh "if appended" thì các lần gọi sau (appended=0 vì đã
        # tồn tại) sẽ không bao giờ sắp xếp lại được data cũ.
        def _sort_key(ev):
            ts = ev.get("event_time") or ev.get("created_at") or ""
            return str(ts)
        history.sort(key=_sort_key, reverse=True)

        r2.put_object(
            Bucket=BUCKET,
            Key="alpha-events/history.json",
            Body=json.dumps(history, default=str).encode("utf-8"),
            ContentType="application/json"
        )
        if appended or updated:
            print(f"[storage] history.json: {appended} appended, {updated} event(s) đồng bộ lại field đã sửa trên Supabase, re-sorted ✓")
        else:
            print(f"[storage] history.json re-sorted (no new/updated events) ✓")
    except Exception as e:
        print(f"[storage] Append history error: {e}")


# ── Xoá HẲN 1 event khỏi mọi nơi (dùng cho /admin/remove-event) ──────
def remove_event_by_msg_id(source_msg_id: int) -> dict:
    """
    [MỚI] Xoá 1 event khỏi TOÀN BỘ nơi lưu — không chỉ Supabase.

    LÝ DO CẦN HÀM NÀY: _upsert_ended_to_all_events() và
    _append_ended_to_history() ở trên CHỈ CÓ upsert/update, hoàn toàn
    KHÔNG CÓ logic xoá. Nên nếu chỉ xoá dòng trong Supabase (DELETE FROM
    alpha_events), event rác vẫn còn NGUYÊN VẸN trong all.json VÀ
    history.json trên R2 mãi mãi — vì 2 file đó chỉ được đồng bộ khi có
    event trùng source_msg_id để cập nhật field, không có khái niệm "dòng
    này đã bị xoá bên Supabase, gỡ nó ra". Case thật đã xảy ra: 1 tin
    "Trading Competition" bị parser cũ nhận nhầm thành airdrop, xoá ở
    Supabase xong vẫn thấy trùng trong history.json.

    Xoá theo source_msg_id (mỗi tin Telegram/API chỉ có 1 msg_id duy
    nhất, dùng để chống trùng xuyên suốt hệ thống — nhất quán với cách
    _upsert_ended_to_all_events/_append_ended_to_history định danh event).
    """
    result = {
        "supabase_deleted": False,
        "all_json_removed": False,
        "history_json_removed": False,
    }

    try:
        resp = supabase.table("alpha_events").delete().eq("source_msg_id", source_msg_id).execute()
        result["supabase_deleted"] = True
        result["supabase_rows_deleted"] = len(resp.data or [])
    except Exception as e:
        result["supabase_error"] = str(e)

    try:
        r2 = get_r2_client()
    except Exception as e:
        result["r2_error"] = str(e)
        return result

    for key, result_key in (
        ("alpha-events/all.json", "all_json_removed"),
        ("alpha-events/history.json", "history_json_removed"),
    ):
        try:
            obj = r2.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            if not isinstance(data, list):
                continue
            before = len(data)
            data = [e for e in data if e.get("source_msg_id") != source_msg_id]
            if len(data) < before:
                r2.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=json.dumps(data, default=str, ensure_ascii=False).encode("utf-8"),
                    ContentType="application/json",
                    CacheControl="max-age=60",
                )
                result[result_key] = True
        except Exception as e:
            result[f"{result_key}_error"] = str(e)

    return result


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
        ended    = [e for e in all_events if e["status"] == "ended"]

        # Blind box candidates — sort by confidence_score
        try:
            candidates = supabase.table("blind_box_candidates") \
                .select("*") \
                .eq("status", "candidate") \
                .order("confidence_score", desc=True) \
                .execute().data
        except Exception:
            candidates = []

        files = {
            "alpha-events/pending.json":    pending,
            "alpha-events/upcoming.json":   upcoming,
            "alpha-events/live.json":       live,
            "alpha-events/blindbox.json":   candidates,
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

        # Tự động merge event vừa 'ended' vào history.json — thay thế hoàn
        # toàn việc phải chạy tay sync_alpha_history.py mỗi khi có sự kiện mới.
        _upsert_ended_to_all_events(ended)
        _append_ended_to_history(ended)

        print(f"[storage] R2 updated — pending={len(pending)}, upcoming={len(upcoming)}, live={len(live)}, ended_synced={len(ended)}, blindbox={len(candidates)} ✓")

    except Exception as e:
        print(f"[storage] R2 error: {e}")
    try:
        r2 = get_r2_client()
        all_events = supabase.table("alpha_events") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute().data

        pending  = [e for e in all_events if e["status"] == "pending"]
        upcoming = [e for e in all_events if e["status"] == "upcoming"]
        live     = [e for e in all_events if e["status"] == "live"]
        ended    = [e for e in all_events if e["status"] == "ended"]

        # Blind box candidates — sort by confidence_score
        try:
            candidates = supabase.table("blind_box_candidates") \
                .select("*") \
                .eq("status", "candidate") \
                .order("confidence_score", desc=True) \
                .execute().data
        except Exception:
            candidates = []

        files = {
            "alpha-events/pending.json":    pending,
            "alpha-events/upcoming.json":   upcoming,
            "alpha-events/live.json":       live,
            "alpha-events/blindbox.json":   candidates,
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

        # Tự động merge event vừa 'ended' vào history.json — thay thế hoàn
        # toàn việc phải chạy tay sync_alpha_history.py mỗi khi có sự kiện mới.
        _upsert_ended_to_all_events(ended)
        _append_ended_to_history(ended)

        print(f"[storage] R2 updated — pending={len(pending)}, upcoming={len(upcoming)}, live={len(live)}, ended_synced={len(ended)}, blindbox={len(candidates)} ✓")

    except Exception as e:
        print(f"[storage] R2 error: {e}")