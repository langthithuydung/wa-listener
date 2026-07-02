"""
blind_box_detect.py - v3
────────────────────────
Approach mới: thay vì query wallet (bị giới hạn vì router có triệu tx),
dùng Binance Alpha token list API để detect token mới.

Logic:
1. Lấy danh sách token hiện tại từ Binance Alpha API
2. So với lần trước (cache) → token nào mới xuất hiện = candidate
3. Score dựa trên: thời gian xuất hiện, amount pattern, market data
4. Kết hợp với pending event để rank

Không cần BSCScan hay Moralis wallet query nữa.
"""

import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "")
MORALIS_BASE    = "https://deep-index.moralis.io/api/v2.2"

BINANCE_ALPHA_API = (
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/"
    "buw/wallet/cex/alpha/all/token/list"
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.binance.com/",
})

# Cache token list theo thời gian
_prev_token_snapshot: dict = {}   # contract → token_info (lần trước)
_curr_token_snapshot: dict = {}   # contract → token_info (hiện tại)
_snapshot_ts: float = 0
SNAPSHOT_TTL = 60  # refresh mỗi 60 giây


def _fetch_alpha_token_list() -> dict:
    """Lấy toàn bộ token list từ Binance Alpha. Return dict contract → info."""
    try:
        r = SESSION.get(BINANCE_ALPHA_API, timeout=15)
        r.raise_for_status()
        tokens = r.json().get("data", [])
        result = {}
        for t in tokens:
            contract = (t.get("contractAddress") or "").lower().strip()
            symbol   = (t.get("symbol") or "").strip()
            if contract and len(contract) > 10:
                result[contract] = {
                    "symbol":    symbol,
                    "name":      t.get("name") or symbol,
                    "price":     float(t.get("price") or 0),
                    "market_cap": float(t.get("marketCap") or 0),
                    "fdv":       float(t.get("fdv") or 0),
                    "chain_id":  str(t.get("chainId") or "56"),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
        return result
    except Exception as e:
        print(f"[blind_box] Alpha API error: {e}")
        return {}


def _get_token_recent_transfers(contract: str, limit: int = 20) -> list:
    """
    Lấy transfers gần nhất của 1 token cụ thể từ Moralis.
    Dễ hơn nhiều so với query wallet có triệu tx.
    """
    if not MORALIS_API_KEY:
        return []
    try:
        r = SESSION.get(
            f"{MORALIS_BASE}/erc20/{contract}/transfers",
            params={"chain": "bsc", "limit": limit, "order": "DESC"},
            headers={"X-API-Key": MORALIS_API_KEY},
            timeout=10
        )
        if r.ok:
            return r.json().get("result", [])
        return []
    except Exception:
        return []


def _check_router_involvement(contract: str) -> dict:
    """
    Kiểm tra token có được transfer vào/ra router wallet không.
    Dùng Moralis token transfer API (query theo contract, không theo wallet).
    """
    ROUTER_WALLETS = {
        "0x6aba0315493b7e6989041c91181337b662fb1b90",
        "0x73d8bd54f7cf5fab43fe4ef40a62d390644946db",
    }

    result = {
        "router_involved": False,
        "router_wallet":   None,
        "transfer_amount": 0,
        "transfer_time":   None,
        "hours_ago":       None,
    }

    txns = _get_token_recent_transfers(contract, limit=50)
    now  = datetime.now(timezone.utc)

    for tx in txns:
        to_addr   = (tx.get("to_address") or "").lower()
        from_addr = (tx.get("from_address") or "").lower()

        if to_addr in ROUTER_WALLETS or from_addr in ROUTER_WALLETS:
            result["router_involved"] = True
            result["router_wallet"]   = to_addr if to_addr in ROUTER_WALLETS else from_addr

            # Parse amount
            try:
                decimals = int(tx.get("token_decimals") or 18)
                val = float(tx.get("value_decimal") or tx.get("value") or "0")
                if "value_decimal" not in tx:
                    val = val / (10 ** decimals)
                result["transfer_amount"] = val
            except Exception:
                pass

            # Parse time
            ts = tx.get("block_timestamp", "")
            if ts:
                try:
                    tx_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    result["transfer_time"] = tx_dt.isoformat()
                    result["hours_ago"] = (now - tx_dt).total_seconds() / 3600
                except Exception:
                    pass
            break  # Chỉ cần tx đầu tiên liên quan router

    return result


def _score_token(token_info: dict, router_info: dict, is_new: bool) -> int:
    """
    Tính confidence score 0-100.
    """
    score = 30  # base

    price = token_info.get("price", 0)
    mc    = token_info.get("market_cap", 0)

    # Token mới xuất hiện trong Alpha list
    if is_new:
        score += 25

    # Có liên quan đến router wallet
    if router_info.get("router_involved"):
        score += 30
        hours_ago = router_info.get("hours_ago")
        if hours_ago is not None:
            if hours_ago <= 3:   score += 15
            elif hours_ago <= 6: score += 10
            elif hours_ago <= 12: score += 5

    # Market cap thấp = token mới, chưa pump = potential
    if 0 < mc < 5_000_000:    score += 10
    elif 5_000_000 <= mc < 50_000_000: score += 5

    # Có giá
    if price > 0:
        score += 5

    return max(0, min(100, score))


def run_detection(supabase) -> list:
    """Entry point."""
    global _prev_token_snapshot, _curr_token_snapshot, _snapshot_ts

    # Kiểm tra có pending event không
    try:
        pending = supabase.table("alpha_events") \
            .select("id, created_at") \
            .eq("status", "pending") \
            .execute().data
    except Exception as e:
        print(f"[blind_box] Fetch pending error: {e}")
        return []

    if not pending:
        print("[blind_box] No pending events, skipping scan")
        return []

    print(f"[blind_box] {len(pending)} pending event(s) → scanning Alpha token list...")

    # Snapshot trước
    _prev_token_snapshot = dict(_curr_token_snapshot)

    # Fetch token list mới
    new_snapshot = _fetch_alpha_token_list()
    if not new_snapshot:
        print("[blind_box] Alpha API returned empty, skip")
        return []

    _curr_token_snapshot = new_snapshot
    _snapshot_ts = time.time()
    print(f"[blind_box] Alpha list: {len(new_snapshot)} tokens")

    # Load known contracts
    known = set()
    try:
        rows1 = supabase.table("alpha_events").select("contract_address").execute().data
        rows2 = supabase.table("blind_box_candidates").select("contract_address").execute().data
        for r in rows1 + rows2:
            addr = r.get("contract_address") or ""
            if len(addr) > 10:
                known.add(addr.lower())
    except Exception as e:
        print(f"[blind_box] Load known error: {e}")

    print(f"[blind_box] Known contracts: {len(known)}")

    # Tìm token mới (chưa có trong known + chưa có trong snapshot trước)
    candidates = []
    now = datetime.now(timezone.utc)

    for contract, info in new_snapshot.items():
        if contract in known:
            continue

        is_new_in_list = contract not in _prev_token_snapshot

        # Check router involvement qua Moralis token transfer API
        router_info = {}
        if MORALIS_API_KEY:
            router_info = _check_router_involvement(contract)
            time.sleep(0.2)  # rate limit

        score = _score_token(info, router_info, is_new_in_list)

        candidates.append({
            "contract":        contract,
            "symbol":          info["symbol"],
            "name":            info["name"],
            "price":           info["price"],
            "market_cap":      info["market_cap"],
            "fdv":             info["fdv"],
            "chain_id":        info["chain_id"],
            "is_new":          is_new_in_list,
            "router_involved": router_info.get("router_involved", False),
            "router_wallet":   router_info.get("router_wallet"),
            "transfer_amount": router_info.get("transfer_amount", 0),
            "transfer_time":   router_info.get("transfer_time"),
            "hours_ago":       router_info.get("hours_ago"),
            "confidence_score": score,
            "event_id":        pending[0]["id"] if pending else None,
        })

    if not candidates:
        print("[blind_box] No new candidates detected")
        return []

    # Sort by score
    candidates.sort(key=lambda x: x["confidence_score"], reverse=True)

    print(f"\n[blind_box] === CANDIDATES RANKED ===")
    for c in candidates[:10]:  # top 10
        router_tag = "🔗router" if c["router_involved"] else "      "
        new_tag    = "🆕" if c["is_new"] else "  "
        hours_str  = f"{c['hours_ago']:.1f}h ago" if c["hours_ago"] else "      "
        print(f"  [{c['confidence_score']:3d}%] {new_tag} {router_tag} {c['symbol']:10s} | ${c['price']:.6f} | MC=${c['market_cap']:>12,.0f} | {hours_str}")

    # Save top candidates vào Supabase
    saved = []
    for c in candidates[:20]:  # save top 20
        try:
            supabase.table("blind_box_candidates").upsert({
                "contract_address":  c["contract"],
                "symbol":            c["symbol"],
                "name":              c["name"],
                "amount_received":   c.get("transfer_amount") or 0,
                "detected_wallet":   c.get("router_wallet") or "alpha_list",
                "confirmed_both":    False,
                "price_usd":         c["price"] or None,
                "market_cap":        c["market_cap"] or None,
                "status":            "candidate",
                "confidence_score":  c["confidence_score"],
                "alpha_event_id":    c.get("event_id"),
                "detected_at":       datetime.now(timezone.utc).isoformat(),
            }, on_conflict="contract_address").execute()
            saved.append(c)
        except Exception as e:
            print(f"[blind_box] Save error {c['symbol']}: {e}")

    print(f"[blind_box] Saved {len(saved)} candidates ✓")
    return saved