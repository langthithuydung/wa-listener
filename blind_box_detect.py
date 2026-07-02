"""
blind_box_detect.py - v2
────────────────────────
On-chain detection cho Binance Alpha blind box / alpha box.

Logic theo từng loại event:
  NEW_LISTING   → Binance đã công bố symbol → chỉ cần tìm contract + giá
  TGE/PRE-TGE   → Tương tự, có symbol từ announcement
  BLINDBOX      → Không có symbol → scan router wallet trong window thời gian
  ALPHA_BOX     → Nhiều token, tier system → scan cả 2 wallet, rank tất cả

Confidence score (0-100):
  +40  Token có trong Binance Alpha official list
  +20  Xuất hiện ở CẢ 2 router wallet (confirmed_both)
  +15  Transfer trong 3h trước giờ pending event
  +10  Transfer trong 3-6h trước giờ pending
  +5   Transfer trong 6-12h trước giờ pending
  +10  Amount pattern hợp lý (total / typical_amount = số user hợp lý)
  +5   verified_contract = true (Moralis verified)
  -30  possible_spam = true
  -20  Amount > 100M (meme/spam)
  -15  Symbol không phải ASCII
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "")
MORALIS_BASE    = "https://deep-index.moralis.io/api/v2.2"

ROUTER_WALLETS = [
    "0x6aba0315493b7e6989041c91181337b662fb1b90",  # Alpha 2.0 Router
    "0x73d8bd54f7cf5fab43fe4ef40a62d390644946db",  # Alpha 2.0 Router Proxy
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

# Cache
_known_contracts: set = set()
_alpha_token_map: dict = {}   # symbol.upper() → {contract, price, marketCap}
_alpha_cache_ts: float = 0
ALPHA_CACHE_TTL = 300  # 5 phút

# Typical airdrop amounts per user để estimate số user
TYPICAL_AMOUNTS = [50, 100, 160, 200, 250, 300, 400, 500, 800, 1000, 2000, 5000]


# ── Binance Alpha Token List ──────────────────────────────────────────
def _refresh_alpha_list():
    global _alpha_token_map, _alpha_cache_ts
    if time.time() - _alpha_cache_ts < ALPHA_CACHE_TTL:
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
            if sym:
                _alpha_token_map[sym] = {
                    "contract": (t.get("contractAddress") or "").lower(),
                    "price":    float(t.get("price") or 0),
                    "mc":       float(t.get("marketCap") or 0),
                    "chain_id": str(t.get("chainId") or "56"),
                }
        _alpha_cache_ts = time.time()
        print(f"[blind_box] Alpha list refreshed: {len(_alpha_token_map)} tokens")
    except Exception as e:
        print(f"[blind_box] Alpha list error: {e}")


def _is_in_alpha_list(symbol: str, contract: str) -> bool:
    """Kiểm tra token có trong Binance Alpha official list không."""
    sym_match = symbol.upper() in _alpha_token_map
    if sym_match:
        return True
    # Check theo contract address
    for v in _alpha_token_map.values():
        if v.get("contract") and contract and v["contract"] == contract.lower():
            return True
    return False


# ── Known contracts ───────────────────────────────────────────────────
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


# ── Fetch transfers với time filter ──────────────────────────────────
def _get_transfers_since(wallet: str, since_dt: datetime, limit: int = 200) -> list:
    """Lấy token transfers từ wallet kể từ since_dt."""
    now = datetime.now(timezone.utc)

    # Moralis free tier: chỉ query trong 24h gần nhất
    # Nếu since_dt quá cũ → clamp về 24h trước
    max_lookback = now - timedelta(hours=24)
    if since_dt < max_lookback:
        since_dt = max_lookback

    # Moralis dùng Unix timestamp (seconds)
    since_ts = int(since_dt.timestamp())

    try:
        r = SESSION.get(
            f"{MORALIS_BASE}/{wallet}/erc20/transfers",
            params={
                "chain":      "bsc",
                "limit":      limit,
                "order":      "DESC",
                "from_date":  since_ts,
            },
            headers={"X-API-Key": MORALIS_API_KEY},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("result", [])
    except Exception as e:
        print(f"[blind_box] Moralis error ({wallet[:10]}...): {e}")
        return []


# ── Confidence scoring ────────────────────────────────────────────────
def _score_candidate(tx_info: dict, pending_event: dict, now: datetime) -> int:
    score = 50  # base

    symbol   = tx_info.get("symbol", "")
    contract = tx_info.get("contract", "")
    amount   = tx_info.get("amount", 0)
    tx_time  = tx_info.get("tx_time")
    is_spam  = tx_info.get("possible_spam", False)
    verified = tx_info.get("verified", False)
    in_both  = tx_info.get("in_both_wallets", False)

    # ── Penalty ──────────────────────────────────────────────────────
    if is_spam:
        score -= 30
    if amount > 100_000_000:
        score -= 20
    try:
        symbol.encode('ascii')
    except UnicodeEncodeError:
        score -= 15

    # ── Bonus: Binance Alpha official list ───────────────────────────
    if _is_in_alpha_list(symbol, contract):
        score += 40

    # ── Bonus: Cả 2 wallet ───────────────────────────────────────────
    if in_both:
        score += 20

    # ── Bonus: Verified contract ─────────────────────────────────────
    if verified:
        score += 5

    # ── Bonus: Timing (gần giờ pending = score cao) ──────────────────
    if tx_time:
        try:
            if isinstance(tx_time, str):
                tx_dt = datetime.fromisoformat(tx_time.replace("Z", "+00:00"))
            else:
                tx_dt = tx_time
            hours_before = (now - tx_dt).total_seconds() / 3600
            if hours_before <= 3:
                score += 15
            elif hours_before <= 6:
                score += 10
            elif hours_before <= 12:
                score += 5
        except Exception:
            pass

    # ── Bonus: Amount pattern (ước tính số user hợp lý) ─────────────
    for typical in TYPICAL_AMOUNTS:
        if typical > 0:
            n_users = amount / typical
            if 10_000 <= n_users <= 2_000_000:
                score += 10
                break

    return max(0, min(100, score))


# ── Main detection ────────────────────────────────────────────────────
def run_detection(supabase) -> list:
    """
    Entry point. Trả về list candidates đã được score và save.
    """
    if not MORALIS_API_KEY:
        print("[blind_box] MORALIS_API_KEY not set, skipping")
        return []

    # Refresh Alpha list
    _refresh_alpha_list()

    # Load known contracts
    known = _load_known_contracts(supabase)
    print(f"[blind_box] Known contracts: {len(known)} (events+candidates)")

    # Lấy pending events
    try:
        pending_events = supabase.table("alpha_events") \
            .select("*") \
            .eq("status", "pending") \
            .execute().data
    except Exception as e:
        print(f"[blind_box] Fetch pending error: {e}")
        return []

    if not pending_events:
        print("[blind_box] No pending events, skipping scan")
        return []

    print(f"[blind_box] {len(pending_events)} pending event(s) → scanning routers...")

    now = datetime.now(timezone.utc)
    all_candidates = {}  # contract → info

    for event in pending_events:
        # Xác định window thời gian scan
        # Scan từ lúc event được tạo - 12h
        try:
            created = datetime.fromisoformat(
                event["created_at"].replace("Z", "+00:00")
            )
        except Exception:
            created = now - timedelta(hours=12)

        scan_from = created - timedelta(hours=12)
        print(f"[blind_box] Event id={event['id']} | scan from {scan_from.strftime('%H:%M')} UTC")

        # Scan từng wallet
        wallet_results = {}  # contract → list of wallets
        for wallet in ROUTER_WALLETS:
            txns = _get_transfers_since(wallet, scan_from, limit=200)
            time.sleep(0.3)
            print(f"[blind_box] Wallet {wallet[:10]}...: {len(txns)} transfers since {scan_from.strftime('%H:%M')}")

            for tx in txns:
                contract = (tx.get("address") or "").lower()
                symbol   = tx.get("token_symbol") or tx.get("symbol") or ""
                name     = tx.get("token_name") or tx.get("name") or ""
                to_addr  = (tx.get("to_address") or "").lower()
                decimals = int(tx.get("token_decimals") or 18)
                is_spam  = tx.get("possible_spam", False)
                verified = tx.get("verified_contract", False)

                # Chỉ quan tâm token VÀO router
                if to_addr not in [w.lower() for w in ROUTER_WALLETS]:
                    continue

                # Skip known
                if contract in known:
                    continue

                # Skip nếu không có contract
                if len(contract) < 10:
                    continue

                # Parse amount
                try:
                    raw_val = tx.get("value_decimal") or tx.get("value") or "0"
                    amount = float(str(raw_val).replace(",", ""))
                    if "value_decimal" not in tx or not tx.get("value_decimal"):
                        amount = amount / (10 ** decimals)
                except Exception:
                    amount = 0

                # Skip spam và amount cực lớn
                if is_spam or amount > 1_000_000_000 or amount < 1_000:
                    continue

                # Skip non-ASCII symbol
                try:
                    symbol.encode('ascii')
                except UnicodeEncodeError:
                    continue

                # Skip symbol không hợp lệ
                if not (2 <= len(symbol) <= 12):
                    continue

                # Skip stablecoin/native
                skip = {"USDT","USDC","BUSD","BNB","WBNB","ETH","WETH","CAKE","DAI"}
                if symbol.upper() in skip:
                    continue

                # Parse tx time
                tx_time = tx.get("block_timestamp")

                if contract not in wallet_results:
                    wallet_results[contract] = []
                wallet_results[contract].append({
                    "wallet":   wallet,
                    "amount":   amount,
                    "tx_time":  tx_time,
                    "verified": verified,
                    "is_spam":  is_spam,
                })

                if contract not in all_candidates:
                    all_candidates[contract] = {
                        "contract":  contract,
                        "symbol":    symbol,
                        "name":      name,
                        "amount":    amount,
                        "tx_time":   tx_time,
                        "verified":  verified,
                        "possible_spam": is_spam,
                        "in_both_wallets": False,
                        "event_id":  event["id"],
                    }
                else:
                    # Update amount nếu lớn hơn
                    if amount > all_candidates[contract]["amount"]:
                        all_candidates[contract]["amount"] = amount

        # Mark confirmed_both
        for contract, wallets in wallet_results.items():
            unique_wallets = set(w["wallet"] for w in wallets)
            if len(unique_wallets) >= 2 and contract in all_candidates:
                all_candidates[contract]["in_both_wallets"] = True

    if not all_candidates:
        print("[blind_box] No new candidates detected")
        return []

    # Score tất cả candidates
    scored = []
    for contract, info in all_candidates.items():
        score = _score_candidate(info, {}, now)
        info["confidence_score"] = score
        scored.append(info)

    # Sort by score
    scored.sort(key=lambda x: x["confidence_score"], reverse=True)

    print(f"\n[blind_box] === CANDIDATES RANKED ===")
    for c in scored:
        in_alpha = _is_in_alpha_list(c["symbol"], c["contract"])
        both = "✓✓" if c.get("in_both_wallets") else "✓ "
        alpha_tag = "🔥ALPHA" if in_alpha else "     "
        print(f"  [{c['confidence_score']:3d}%] {both} {alpha_tag} {c['symbol']:10s} | {c['name'][:20]:20s} | {c['amount']:>15,.0f} tokens")

    # Save vào Supabase
    saved = []
    for c in scored:
        try:
            supabase.table("blind_box_candidates").upsert({
                "contract_address":  c["contract"],
                "symbol":            c["symbol"],
                "name":              c["name"],
                "amount_received":   c["amount"],
                "detected_wallet":   ROUTER_WALLETS[0] if not c.get("in_both_wallets") else "both",
                "confirmed_both":    c.get("in_both_wallets", False),
                "status":            "candidate",
                "detected_at":       datetime.now(timezone.utc).isoformat(),
                "confidence_score":  c["confidence_score"],
                "alpha_event_id":    c.get("event_id"),
            }, on_conflict="contract_address").execute()
            saved.append(c)
        except Exception as e:
            print(f"[blind_box] Save error {c['symbol']}: {e}")

    print(f"[blind_box] Saved {len(saved)} candidates ✓")
    return saved