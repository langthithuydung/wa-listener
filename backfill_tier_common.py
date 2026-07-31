"""
backfill_tier_common.py
────────────────────────
Migration MỘT LẦN — sửa lại các event Alpha Box (nhiều tier: Common/Rare/
Super Rare) đã bị lưu SAI amount_per_user từ trước khi parser có logic
tách tier (_parse_multi_token_tiers trong alpha_parser.py).

Bug cũ: parser fallback regex generic từng khớp nhầm SỐ CUỐI trong câu
kiểu "receive one of the following rewards: 315, 395, or 1125 ON tokens"
(khớp "1125 ON tokens" thay vì "315") → amount_per_user bị lưu = tier
Super Rare (1125) thay vì tier Common (315) — trong khi thực tế ~85%
người dùng chỉ nhận đúng mức Common, Rare/Super Rare chỉ là thưởng CỘNG
THÊM ngẫu nhiên (315 + 80 = 395, 315 + 810 = 1125 — khớp với ảnh chụp
app Binance chính chủ).

Parser hiện tại (parse_with_regex, dòng ~207) đã tự lấy đúng tier_common
cho event MỚI — script này CHỈ backfill lại các event CŨ đã lỡ lưu sai.

Cách chạy:
    python backfill_tier_common.py            # dry-run, chỉ in ra, KHÔNG ghi
    python backfill_tier_common.py --apply     # ghi thật vào Supabase
    python backfill_tier_common.py --apply --refresh   # ghi xong tự gọi
                                                        # refresh_r2_snapshot()
"""

import os
import sys
import json
from supabase import create_client


def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def main():
    apply = "--apply" in sys.argv
    do_refresh = "--refresh" in sys.argv

    supabase = get_supabase()

    rows = supabase.table("alpha_events") \
        .select("id, symbol, project_name, amount_per_user, price_snapshot, value_usd, tokens_detail, source_msg_id") \
        .not_.is_("tokens_detail", "null") \
        .execute().data

    print(f"Tìm thấy {len(rows)} event có tokens_detail (Alpha Box nhiều tier)\n")

    to_fix = []
    for row in rows:
        td = row.get("tokens_detail")
        if isinstance(td, str):
            try:
                td = json.loads(td)
            except Exception:
                continue
        if not isinstance(td, list) or not td:
            continue

        main_token = td[0]
        tier_common = main_token.get("tier_common")
        if tier_common is None:
            continue

        current_amt = row.get("amount_per_user")
        try:
            current_amt_f = float(current_amt) if current_amt is not None else None
        except Exception:
            current_amt_f = None

        if current_amt_f is not None and abs(current_amt_f - float(tier_common)) < 1e-9:
            continue  # đã đúng rồi, bỏ qua

        price = row.get("price_snapshot")
        new_value_usd = None
        if price:
            try:
                new_value_usd = round(float(tier_common) * float(price), 4)
            except Exception:
                pass

        to_fix.append({
            "id": row["id"],
            "symbol": row.get("symbol"),
            "old_amount": current_amt,
            "new_amount": tier_common,
            "old_value_usd": row.get("value_usd"),
            "new_value_usd": new_value_usd,
        })

    if not to_fix:
        print("Không có event nào cần sửa — mọi thứ đã đúng chuẩn tier_common ✓")
        return

    print(f"{'DRY-RUN — sẽ sửa' if not apply else 'ĐANG SỬA'} {len(to_fix)} event:\n")
    for f in to_fix:
        print(f"  id={f['id']:<6} {f['symbol']:<8} amount_per_user: {f['old_amount']} → {f['new_amount']}"
              f"   |  value_usd: {f['old_value_usd']} → {f['new_value_usd']}")

    if not apply:
        print("\n(Đây chỉ là dry-run — chạy lại với --apply để ghi thật vào Supabase)")
        return

    updated = 0
    for f in to_fix:
        update_data = {"amount_per_user": f["new_amount"]}
        if f["new_value_usd"] is not None:
            update_data["value_usd"] = f["new_value_usd"]
        supabase.table("alpha_events").update(update_data).eq("id", f["id"]).execute()
        updated += 1

    print(f"\n✓ Đã sửa {updated}/{len(to_fix)} event trên Supabase")

    if do_refresh:
        from storage import refresh_r2_snapshot
        refresh_r2_snapshot()
        print("✓ Đã refresh_r2_snapshot() — R2 (all.json/history.json) đã đồng bộ lại")
    else:
        print("\nLƯU Ý: R2 (all.json/history.json) CHƯA được đồng bộ lại.")
        print("Chạy lại với --refresh, hoặc tự gọi endpoint GET /refresh trên app Render.")


if __name__ == "__main__":
    main()
