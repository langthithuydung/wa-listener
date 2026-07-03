"""
blind_box_detect.py - v4 (stable)
──────────────────────────────────
Kết hợp 2 tín hiệu:
  1. Router wallet monitoring (Moralis, đã proven hoạt động)
     → bắt token TRƯỚC khi Binance công bố (early signal)
  2. Binance Alpha official list cross-check
     → xác nhận token đã/sắp chính thức (confidence bonus)

Áp dụng cho mọi loại event: airdrop thường, blind box, alpha box.
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "")
MORALIS_BASE    = "https://deep-index.moralis.io/api/v2.2"

try:
    from enricher import enrich_token
except Exception:
    enrich_token = None

ROUTER_WALLETS = [
    "0x6aba0315493b7e6989041c91181337b662fb1b90",  # Alpha 2.0 Router
    "0x73d8bd54f7cf5fab43fe4ef40a62d390644946db",  # Alpha 2.0 Router Proxy
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.binance.com/",
})

_alpha_token_map: dict = {}
_alpha_ts: float = 0
ALPHA_TTL = 300

TYPICAL_AMOUNTS = [50, 100, 160, 200, 226, 250, 300, 400, 500, 800, 1000, 2000, 5000]


def _refresh_alpha_list():
    global _alpha_token_map, _alpha_ts
    if time.time() - _alpha_ts < ALPHA_TTL:
        return
    try:
        r = SESSION.get(
            "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list",
            timeout=15
        )
        r.raise_for_status()
        tokens = r.json().get("data", [])
        _alpha_token_map = {}
        for t in tokens:
            sym = (t.get("symbol") or "").upper()
            contract = (t.get("contractAddress") or "").lower()
            if sym:
                _alpha_token_map[sym] = contract
        _alpha_ts = time.time()
        print(f"[blind_box] Alpha list refreshed: {len(_alpha_token_map)} tokens")
    except Exception as e:
        print(f"[blind_box] Alpha list error: {e}")


def _in_alpha_list(symbol: str, contract: str) -> bool:
    c = _alpha_token_map.get(symbol.upper())
    if c and contract and c == contract.lower():
        return True
    return symbol.upper() in _alpha_token_map


def _load_known_contracts(supabase) -> set:
    contracts = set()
    try:
        rows = supabase.table("alpha_events").select("contract_address").execute().data
        for r in rows:
            addr = r.get("contract_address") or ""
            if len(addr) > 10:
                contracts.add(addr.lower())
    except Exception as e:
        print(f"[blind_box] Load alpha_events error: {e}")
    try:
        rows = supabase.table("blind_box_candidates").select("contract_address").execute().data
        for r in rows:
            addr = r.get("contract_address") or ""
            if len(addr) > 10:
                contracts.add(addr.lower())
    except Exception as e:
        print(f"[blind_box] Load candidates error: {e}")
    return contracts


def _get_wallet_transfers(wallet: str, limit: int = 100) -> list:
    """Lấy transfers gần nhất — KHÔNG dùng from_date (gây lỗi 400)."""
    try:
        r = SESSION.get(
            f"{MORALIS_BASE}/{wallet}/erc20/transfers",
            params={"chain": "bsc", "limit": limit, "order": "DESC"},
            headers={"X-API-Key": MORALIS_API_KEY},
            timeout=15
        )
        r.raise_for_status()
        result = r.json().get("result", [])
        print(f"[blind_box] Moralis {wallet[:10]}...: {len(result)} transfers")
        return result
    except Exception as e:
        print(f"[blind_box] Moralis error ({wallet[:10]}...): {e}")
        return []


def _score_candidate(symbol: str, contract: str, amount: float,
                      tx_time_str: str, is_spam: bool, verified: bool,
                      in_both: bool, now: datetime) -> int:
    score = 40

    if is_spam:
        score -= 30
    if amount > 100_000_000:
        score -= 20
    try:
        symbol.encode('ascii')
    except UnicodeEncodeError:
        score -= 15

    if _in_alpha_list(symbol, contract):
        score += 35
    if in_both:
        score += 15
    if verified:
        score += 5

    if tx_time_str:
        try:
            tx_dt = datetime.fromisoformat(tx_time_str.replace("Z", "+00:00"))
            hours_ago = (now - tx_dt).total_seconds() / 3600
            if hours_ago <= 3:    score += 15
            elif hours_ago <= 6:  score += 10
            elif hours_ago <= 12: score += 5
        except Exception:
            pass

    for typical in TYPICAL_AMOUNTS:
        n_users = amount / typical if typical else 0
        if 5_000 <= n_users <= 2_000_000:
            score += 10
            break

    return max(0, min(100, score))


def run_detection(supabase) -> list:
    if not MORALIS_API_KEY:
        print("[blind_box] MORALIS_API_KEY not set, skipping")
        return []

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

    print(f"[blind_box] {len(pending)} pending event(s) → scanning routers...")

    _refresh_alpha_list()
    known = _load_known_contracts(supabase)
    print(f"[blind_box] Known contracts: {len(known)}")

    now = datetime.now(timezone.utc)
    event_id = pending[0]["id"]

    wallet_txns = {}
    all_candidates = {}

    for wallet in ROUTER_WALLETS:
        txns = _get_wallet_transfers(wallet, limit=100)
        time.sleep(0.3)
        wallet_txns[wallet] = txns

        for tx in txns:
            contract = (tx.get("address") or "").lower()
            symbol   = tx.get("token_symbol") or ""
            name     = tx.get("token_name") or ""
            to_addr  = (tx.get("to_address") or "").lower()
            decimals = int(tx.get("token_decimals") or 18)
            is_spam  = tx.get("possible_spam", False)
            verified = tx.get("verified_contract", False)
            tx_time  = tx.get("block_timestamp")

            if to_addr not in [w.lower() for w in ROUTER_WALLETS]:
                continue
            if contract in known or len(contract) < 10:
                continue

            try:
                val = float(tx.get("value_decimal") or "0")
                if not tx.get("value_decimal"):
                    val = float(tx.get("value") or "0") / (10 ** decimals)
            except Exception:
                val = 0

            if is_spam or val > 1_000_000_000 or val < 500:
                continue
            try:
                symbol.encode('ascii')
            except UnicodeEncodeError:
                continue
            if not (2 <= len(symbol) <= 12):
                continue
            skip = {"USDT","USDC","BUSD","BNB","WBNB","ETH","WETH","CAKE","DAI"}
            if symbol.upper() in skip:
                continue

            if contract not in all_candidates:
                all_candidates[contract] = {
                    "contract": contract, "symbol": symbol, "name": name,
                    "amount": val, "tx_time": tx_time,
                    "is_spam": is_spam, "verified": verified,
                    "wallets": {wallet},
                }
            else:
                all_candidates[contract]["wallets"].add(wallet)
                if val > all_candidates[contract]["amount"]:
                    all_candidates[contract]["amount"] = val

    if not all_candidates:
        print("[blind_box] No new candidates detected")
        return []

    scored = []
    for contract, info in all_candidates.items():
        in_both = len(info["wallets"]) >= 2
        score = _score_candidate(
            info["symbol"], contract, info["amount"], info["tx_time"],
            info["is_spam"], info["verified"], in_both, now
        )
        info["confidence_score"] = score
        info["in_both"] = in_both
        info["in_alpha_list"] = _in_alpha_list(info["symbol"], contract)
        scored.append(info)

    scored.sort(key=lambda x: x["confidence_score"], reverse=True)

    print(f"\n[blind_box] === CANDIDATES RANKED ===")
    for c in scored[:15]:
        both = "✓✓" if c["in_both"] else "✓ "
        alpha = "🔥ALPHA" if c["in_alpha_list"] else "      "
        print(f"  [{c['confidence_score']:3d}%] {both} {alpha} {c['symbol']:10s} | {c['name'][:20]:20s} | {c['amount']:>15,.0f}")

    saved = []
    for c in scored[:20]:
        # Chỉ enrich giá cho candidate có confidence cao (tiết kiệm API call)
        price = None
        value_usd = None
        if enrich_token and c["confidence_score"] >= 60:
            try:
                enriched = enrich_token(c["symbol"], c["name"])
                price = enriched.get("price_snapshot")
                if price and c["amount"]:
                    value_usd = round(c["amount"] * price, 4)
            except Exception as e:
                print(f"[blind_box] Price enrich error {c['symbol']}: {e}")

        try:
            supabase.table("blind_box_candidates").upsert({
                "contract_address":  c["contract"],
                "symbol":            c["symbol"],
                "name":              c["name"],
                "amount_received":   c["amount"],
                "detected_wallet":   "both" if c["in_both"] else list(c["wallets"])[0],
                "confirmed_both":    c["in_both"],
                "status":            "candidate",
                "confidence_score":  c["confidence_score"],
                "in_alpha_list":     c["in_alpha_list"],
                "price_usd":         price,
                "predicted_value_usd": value_usd,
                "alpha_event_id":    event_id,
                "detected_at":       now.isoformat(),
            }, on_conflict="contract_address").execute()
            saved.append(c)
        except Exception as e:
            print(f"[blind_box] Save error {c['symbol']}: {e}")

    print(f"[blind_box] Saved {len(saved)} candidates ✓")
    return saved