"""
scheduler.py
────────────
APScheduler jobs chạy trong Render 24/7.
Thay thế toàn bộ GitHub Actions scheduled jobs.

Jobs:
- every 3 min:  poll Binance Announcement API (bắt TGE/Pre-TGE miss)
- every 5 min:  enrich_upcoming (cập nhật giá + contract cho upcoming/pending)
- every 30 min: auto_expire (chuyển status tự động)
"""

import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

from enricher import enrich_token, compute_value_usd

# Lazy import để tránh circular
def _get_supabase():
    from supabase import create_client
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def _get_storage():
    from storage import save_event, refresh_r2_snapshot
    return save_event, refresh_r2_snapshot

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Accept": "application/json",
    "Referer": "https://www.binance.com/",
})

# ── JOB 1: Poll Binance Announcement API ─────────────────────────────
POLLER_CHANNEL = "binance_announcement_api"

def job_poll_announcements():
    """
    Gọi Binance Announcement API mỗi 3 phút.
    Bắt TGE/Pre-TGE/Alpha listing mà Telegram có thể miss.

    Dedupe dùng bảng processed_messages BỀN VỮNG (Supabase), KHÔNG dùng
    biến RAM — biến RAM sẽ mất mỗi lần Render restart, khiến job coi bài
    cũ là "mới" và tự tạo lại pending rác liên tục (bug đã từng xảy ra).
    """
    try:
        r = SESSION.get(
            "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
            params={"type": 1, "pageNo": 1, "pageSize": 20, "catalogId": 48},
            timeout=10
        )
        r.raise_for_status()
        articles = r.json().get("data", {}).get("articles", [])

        from alpha_parser import parse_message, is_relevant
        from storage import save_event, _is_message_processed, _mark_message_processed

        new_count = 0
        for article in articles:
            aid = str(article.get("id", ""))
            if not aid:
                continue
            msg_id = int(aid) if aid.isdigit() else abs(hash(aid)) % 999999999

            if _is_message_processed(POLLER_CHANNEL, msg_id):
                continue

            title = article.get("title", "")
            body  = article.get("body", "") or title
            full_text = title + "\n" + body

            if not is_relevant(full_text):
                _mark_message_processed(POLLER_CHANNEL, msg_id)
                continue

            parsed = parse_message(full_text)
            _mark_message_processed(POLLER_CHANNEL, msg_id)

            if parsed:
                save_event(
                    parsed=parsed,
                    raw_text=full_text,
                    source_channel=POLLER_CHANNEL,
                    msg_id=msg_id
                )
                new_count += 1
                print(f"[poller] New from API: {title[:80]}")

        if new_count:
            print(f"[poller] {new_count} new events from Binance Announcement API")

    except Exception as e:
        print(f"[poller] Announcement API error: {e}")


# ── JOB 2: Enrich upcoming/pending events với giá + contract ─────────
def _get_confirmed_blind_box_contract(supabase, alpha_event_id: int, symbol: str) -> dict | None:
    """
    Tra bảng blind_box_candidates cho đúng event + symbol này — dùng làm
    nguồn ƯU TIÊN CAO NHẤT khi enrich contract cho token đó. Contract ở
    đây đã được XÁC NHẬN qua on-chain thật (ví Binance Alpha Router thật
    sự nhận đúng token này cho đúng event này) — đáng tin hơn hẳn tìm
    theo symbol/ticker qua enrich_token() (dễ dính ticker collision).

    Case thật đã xảy ra: event Alpha Box AGT+AIA — enrich theo symbol
    "AIA" lấy NHẦM contract 0x48a18a47... của 1 token AIA khác, trong
    khi blind_box_detect đã tự xác nhận đúng contract 0x53ec33cd... từ
    trước đó (cả 2 ví router đều thấy, confidence 100%) nhưng job enrich
    tokens_detail chưa từng tham khảo bảng này.
    """
    if not alpha_event_id or not symbol:
        return None
    try:
        rows = supabase.table("blind_box_candidates") \
            .select("contract_address, confidence_score, confirmed_both, in_alpha_list") \
            .eq("alpha_event_id", alpha_event_id) \
            .ilike("symbol", symbol) \
            .order("confidence_score", desc=True) \
            .limit(1) \
            .execute().data
        if rows and rows[0].get("contract_address"):
            return rows[0]
    except Exception as e:
        print(f"[enrich] blind_box lookup error ({symbol}): {e}")
    return None


def job_enrich_prices():
    """
    Mỗi 5 phút: lấy tất cả upcoming + pending có symbol
    → update contract_address, price_snapshot, value_usd, market_cap
    → ĐỒNG THỜI enrich giá/contract cho TỪNG TOKEN trong tokens_detail
      (Alpha Box nhiều token, VD: ON + MPLX cùng lúc) — trước đây chỉ
      enrich symbol chính, các token phụ trong tokens_detail bị bỏ trống
      contract/giá hoàn toàn.
    """
    try:
        supabase = _get_supabase()

        # [SỬA — BUG] Trước đây chỉ lấy status upcoming/live/pending — một
        # khi event chuyển "ended" (2h sau event_time, xem job_auto_expire),
        # nó biến mất khỏi query này VĨNH VIỄN, dù contract_address bị sai
        # (case AEON đã xảy ra thật). Giờ thêm "ended" nhưng giới hạn trong
        # 14 ngày gần nhất — đủ thời gian để enrich lại/tự sửa, mà không làm
        # job quét lại API vô hạn với hàng nghìn event ended cũ về sau.
        cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
        rows = supabase.table("alpha_events") \
            .select("id, symbol, project_name, amount_per_user, contract_address, price_snapshot, tokens_detail, status, event_time, created_at") \
            .in_("status", ["upcoming", "live", "pending", "ended"]) \
            .not_.is_("symbol", "null") \
            .execute().data
        rows = [
            r for r in rows
            if r.get("status") != "ended"
            or (r.get("event_time") or r.get("created_at") or "") >= cutoff
        ]

        if not rows:
            return

        updated = 0
        for row in rows:
            symbol  = row.get("symbol")
            if not symbol:
                continue

            # [MỚI] Chỉ cho phép fallback GeckoTerminal/DexScreener khi
            # token đã "live" (Binance chính thức xác nhận) — tránh lấy
            # nhầm contract của token khác trùng ticker khi còn upcoming/
            # pending (case thật đã xảy ra: GRVT bị lấy nhầm contract).
            allow_dex = row.get("status") not in ("upcoming", "pending")
            enriched = enrich_token(symbol, row.get("project_name"), allow_dex_fallback=allow_dex)

            # [MỚI] Nếu blind_box_detect đã xác nhận contract cho đúng
            # event + symbol này, ưu tiên dùng nó — ghi đè kết quả tìm
            # theo symbol ở trên (xem docstring _get_confirmed_blind_box_contract).
            blind_box_hit = _get_confirmed_blind_box_contract(supabase, row["id"], symbol)
            if blind_box_hit:
                from enricher import enrich_by_contract_address
                addr_data = enrich_by_contract_address(blind_box_hit["contract_address"], row.get("chain_id"))
                enriched = dict(enriched) if enriched else {}
                enriched["contract_address"] = blind_box_hit["contract_address"]
                if addr_data.get("price_snapshot"):
                    enriched["price_snapshot"] = addr_data["price_snapshot"]
                if addr_data.get("market_cap"):
                    enriched["market_cap"] = addr_data["market_cap"]
                if addr_data.get("fdv"):
                    enriched["fdv"] = addr_data["fdv"]
                print(f"[enrich] {symbol} ✓ contract từ blind_box_candidates (on-chain xác nhận, event #{row['id']}): {blind_box_hit['contract_address']}")

            update_data = {}
            val_usd = None
            if enriched:
                price   = enriched.get("price_snapshot") or row.get("price_snapshot")
                amount  = row.get("amount_per_user")
                val_usd = compute_value_usd(amount, price)

                # [SỬA — BUG] Điều kiện cũ "not row.get('contract_address')"
                # CHỈ điền khi đang TRỐNG — nếu contract cũ đã bị enrich sai
                # (ticker collision lúc mới "live"), nó sẽ bị kẹt sai vĩnh
                # viễn vì điều kiện này luôn False sau đó. Giờ so sánh khác
                # nhau thì ghi đè. Nguồn enrich_token() đã tự bảo vệ khỏi
                # collision (allow_dex_fallback=False khi upcoming/pending),
                # nên tin tưởng ghi đè an toàn ở đây.
                if enriched.get("contract_address") and enriched["contract_address"] != row.get("contract_address"):
                    update_data["contract_address"] = enriched["contract_address"]
                if enriched.get("price_snapshot"):
                    update_data["price_snapshot"] = enriched["price_snapshot"]
                if enriched.get("market_cap"):
                    update_data["market_cap"] = enriched["market_cap"]
                if enriched.get("fdv"):
                    update_data["fdv"] = enriched["fdv"]
                if enriched.get("chain_id"):
                    update_data["chain_id"] = enriched["chain_id"]
                if enriched.get("chain_name"):
                    update_data["chain_name"] = enriched["chain_name"]
                if val_usd:
                    update_data["value_usd"] = val_usd

            # ── Enrich RIÊNG cho từng token trong tokens_detail (Alpha Box
            # nhiều token) — mỗi token có contract/giá/value_usd của chính nó,
            # tính theo tier_common (giá trị đại diện phổ biến nhất). ──
            tokens_detail = row.get("tokens_detail")
            if isinstance(tokens_detail, list) and tokens_detail:
                new_tokens_detail = []
                for t in tokens_detail:
                    t_symbol = t.get("symbol")
                    if not t_symbol:
                        new_tokens_detail.append(t)
                        continue

                    # [MỚI] Ưu tiên contract đã được blind_box_detect xác
                    # nhận on-chain cho đúng event+symbol này, thay vì tin
                    # thẳng kết quả tìm theo symbol (dễ dính ticker
                    # collision — case AIA thật đã xảy ra, xem docstring
                    # _get_confirmed_blind_box_contract ở trên).
                    blind_box_hit = _get_confirmed_blind_box_contract(supabase, row["id"], t_symbol)
                    if blind_box_hit:
                        from enricher import enrich_by_contract_address
                        addr_data = enrich_by_contract_address(blind_box_hit["contract_address"], row.get("chain_id"))
                        t_enriched = {
                            "contract_address": blind_box_hit["contract_address"],
                            "price_snapshot": addr_data.get("price_snapshot"),
                        }
                        print(f"[enrich] {t_symbol} (sub-token) ✓ contract từ blind_box_candidates: {blind_box_hit['contract_address']}")
                    else:
                        t_enriched = enrich_token(t_symbol, None, allow_dex_fallback=allow_dex)
                        time.sleep(0.4)  # tránh rate limit khi enrich nhiều token liên tiếp

                    t_updated = dict(t)  # copy, giữ nguyên tier_common/tier_rare/tier_super_rare
                    if t_enriched:
                        t_price = t_enriched.get("price_snapshot")
                        if t_enriched.get("contract_address"):
                            t_updated["contract_address"] = t_enriched["contract_address"]
                        if t_price:
                            t_updated["price_snapshot"] = t_price
                            # value_usd ước tính theo tier phổ biến nhất (Common)
                            common = t.get("tier_common")
                            if common is not None:
                                t_updated["value_usd"] = round(common * t_price, 4)
                    new_tokens_detail.append(t_updated)
                update_data["tokens_detail"] = new_tokens_detail

            if update_data:
                supabase.table("alpha_events") \
                    .update(update_data) \
                    .eq("id", row["id"]) \
                    .execute()
                updated += 1
                detail_syms = [t.get("symbol") for t in tokens_detail] if tokens_detail else [symbol]
                print(f"[enrich] {'+'.join(detail_syms)}: price=${enriched.get('price_snapshot','?') if enriched else '?'}, value=${val_usd or '?'}")

            time.sleep(0.5)  # Tránh rate limit

        if updated:
            from storage import refresh_r2_snapshot
            refresh_r2_snapshot()
            print(f"[enrich] Updated {updated} events ✓")

    except Exception as e:
        print(f"[enrich] Error: {e}")


# ── JOB 3: Auto expire ────────────────────────────────────────────────
def job_auto_expire():
    """
    Mỗi 30 phút: chuyển status tự động.
    upcoming → live → ended
    pending (48h) → ended
    """
    try:
        supabase = _get_supabase()
        now = datetime.now(timezone.utc)

        rows = supabase.table("alpha_events") \
            .select("id, status, event_time, created_at, symbol") \
            .neq("status", "ended") \
            .execute().data

        expired  = []
        go_live  = []

        for row in rows:
            rid    = row["id"]
            status = row["status"]

            # Xác định event_time
            et = None
            if row.get("event_time"):
                try:
                    et = datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
                except Exception:
                    pass

            if et:
                expire_at = et + timedelta(hours=2)
                is_expired = now >= expire_at
                is_live    = (et - timedelta(hours=1)) <= now < expire_at
            else:
                # Không có event_time:
                #   status "upcoming" (đã có symbol, đang claim) → expire sau 24h
                #   status "pending"  (chưa có symbol/blindbox) → expire sau 48h
                try:
                    created = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
                except Exception:
                    continue
                timeout_hours = 24 if status == "upcoming" else 48
                expire_at  = created + timedelta(hours=timeout_hours)
                is_expired = now >= expire_at
                is_live    = False

            if is_expired and status != "ended":
                expired.append(rid)
                print(f"[expire] → ended: {row.get('symbol') or f'id={rid}'}")
            elif is_live and status == "upcoming":
                go_live.append(rid)
                print(f"[expire] → live: {row.get('symbol') or f'id={rid}'}")

        changed = False
        if expired:
            supabase.table("alpha_events").update({"status": "ended"}).in_("id", expired).execute()
            changed = True
        if go_live:
            supabase.table("alpha_events").update({"status": "live"}).in_("id", go_live).execute()
            changed = True

        if changed:
            from storage import refresh_r2_snapshot
            refresh_r2_snapshot()
            print(f"[expire] Done: {len(expired)} ended, {len(go_live)} live ✓")

    except Exception as e:
        print(f"[expire] Error: {e}")


# ── Enrich nhanh 1 symbol (dùng cho /admin/set-contract) ─────────────
def enrich_symbol_now(symbol: str) -> int:
    """
    [MỚI] Enrich lại NGAY LẬP TỨC chỉ những event có đúng symbol này —
    KHÔNG chạy job_enrich_prices() đầy đủ (job đó loop hết cả bảng, có
    sleep giữa mỗi dòng, có thể mất vài phút → treo lâu nếu gọi trực
    tiếp từ 1 HTTP request). Dùng ngay sau khi vừa set contract override
    thủ công, để thấy kết quả cập nhật tức thì thay vì đợi tối đa 5 phút.

    Trả về số event đã cập nhật.
    """
    supabase = _get_supabase()
    rows = supabase.table("alpha_events") \
        .select("id, symbol, project_name, amount_per_user") \
        .eq("symbol", symbol) \
        .execute().data

    if not rows:
        return 0

    updated = 0
    for row in rows:
        enriched = enrich_token(row["symbol"], row.get("project_name"), allow_dex_fallback=True)
        if not enriched:
            continue
        update_data = {}
        if enriched.get("contract_address"):
            update_data["contract_address"] = enriched["contract_address"]
        if enriched.get("price_snapshot"):
            update_data["price_snapshot"] = enriched["price_snapshot"]
            val = compute_value_usd(row.get("amount_per_user"), enriched["price_snapshot"])
            if val:
                update_data["value_usd"] = val
        if enriched.get("market_cap"):
            update_data["market_cap"] = enriched["market_cap"]
        if enriched.get("fdv"):
            update_data["fdv"] = enriched["fdv"]
        if enriched.get("chain_id"):
            update_data["chain_id"] = enriched["chain_id"]
        if enriched.get("chain_name"):
            update_data["chain_name"] = enriched["chain_name"]

        if update_data:
            supabase.table("alpha_events").update(update_data).eq("id", row["id"]).execute()
            updated += 1

    if updated:
        from storage import refresh_r2_snapshot
        refresh_r2_snapshot()

    return updated


# ── Khởi động scheduler ───────────────────────────────────────────────
def start_scheduler():
    scheduler = BackgroundScheduler(timezone="UTC")

    # Poll announcement API mỗi 3 phút
    scheduler.add_job(job_poll_announcements, 'interval', minutes=3,
                      id='poll_announcements', max_instances=1)

    # Enrich giá mỗi 5 phút
    scheduler.add_job(job_enrich_prices, 'interval', minutes=5,
                      id='enrich_prices', max_instances=1)

    # Auto expire mỗi 30 phút
    scheduler.add_job(job_auto_expire, 'interval', minutes=30,
                      id='auto_expire', max_instances=1)

    # Blind box detection mỗi 5 phút
    scheduler.add_job(job_blind_box_detect, 'interval', minutes=5,
                      id='blind_box_detect', max_instances=1)

    scheduler.start()
    print("[scheduler] Started ✓")
    print("[scheduler] Jobs: poll(3m), enrich(5m), expire(30m), blindbox(5m)")

    # Chạy ngay lần đầu sau 10 giây
    import threading
    def _run_initial():
        time.sleep(10)
        print("[scheduler] Running initial jobs...")
        job_auto_expire()
        job_enrich_prices()
    threading.Thread(target=_run_initial, daemon=True).start()

    return scheduler


# ── JOB 4: Blind Box Detection ───────────────────────────────────────
def job_blind_box_detect():
    """
    Mỗi 5 phút khi có pending event:
    Quét 2 Binance Alpha Router wallet trên BSC
    → phát hiện token mới → candidate blind box
    """
    print("[blind_box] Running...")
    try:
        from blind_box_detect import run_detection
        supabase = _get_supabase()
        candidates = run_detection(supabase)
        if candidates:
            print(f"[blind_box] {len(candidates)} new candidate(s) found ✓")
    except Exception as e:
        print(f"[blind_box] Job error: {e}")