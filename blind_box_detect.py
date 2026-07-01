"""
blind_box_detect.py
───────────────────
Monitor 2 Binance Alpha Router wallets trên BSC.
Phát hiện token mới xuất hiện → candidate blind box airdrop.

Chạy mỗi 5 phút khi có pending event trong Supabase.
"""

import os
import time
import requests
from datetime import datetime, timezone

# Moralis API — free tier, hỗ trợ BSC, 40k calls/ngày
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "")
MORALIS_BASE    = "https://deep-index.moralis.io/api/v2.2"

# 2 wallet router Binance Alpha
ROUTER_WALLETS = [
    "0x6aba0315493b7e6989041c91181337b662fb1b90",  # Alpha 2.0 Router
    "0x73d8bd54f7cf5fab43fe4ef40a62d390644946db",  # Alpha 2.0 Router Proxy
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

# Cache token đã biết để tránh alert lặp
_known_contracts: set = set()
_cache_loaded = False


def _load_known_contracts(supabase) -> set:
    """Load tất cả contract đã có trong Supabase."""
    global _known_contracts, _cache_loaded
    if _cache_loaded:
        return _known_contracts
    try:
        rows = supabase.table("alpha_events") \
            .select("contract_address, symbol") \
            .not_.is_("contract_address", "null") \
            .execute().data
        _known_contracts = {r["contract_address"].lower() for r in rows if r.get("contract_address")}
        _cache_loaded = True
        print(f"[blind_box] Loaded {len(_known_contracts)} known contracts")
    except Exception as e:
        print(f"[blind_box] Load known contracts error: {e}")
    return _known_contracts


def _get_token_transfers(wallet: str, limit: int = 100) -> list:
    """Lấy token transfers gần nhất của wallet từ Moralis API."""
    try:
        # Endpoint đúng của Moralis cho ERC20 transfers
        r = SESSION.get(
            f"{MORALIS_BASE}/{wallet}/erc20/transfers",
            params={"chain": "bsc", "limit": limit, "order": "DESC"},
            headers={"X-API-Key": MORALIS_API_KEY},
            timeout=15
        )
        r.raise_for_status()
        data = r.json()
        raw = data.get("result", [])

        # Normalize Moralis format → format tương tự BSCScan
        normalized = []
        for tx in raw:
            normalized.append({
                "contractAddress": tx.get("token_address", "").lower(),
                "tokenSymbol":     tx.get("token_symbol", ""),
                "tokenName":       tx.get("token_name", ""),
                "to":              tx.get("to_address", "").lower(),
                "from":            tx.get("from_address", "").lower(),
                "value":           tx.get("value", "0"),
                "tokenDecimal":    str(tx.get("token_decimals", 18)),
                "hash":            tx.get("transaction_hash", ""),
            })
        print(f"[blind_box] Moralis {wallet[:10]}...: {len(normalized)} transfers")
        return normalized
    except Exception as e:
        print(f"[blind_box] Moralis API error ({wallet[:10]}...): {e}")
        return []


def _enrich_candidate(contract: str, symbol: str, token_name: str) -> dict:
    """Lấy thêm thông tin token candidate từ GeckoTerminal."""
    try:
        r = SESSION.get(
            f"https://api.geckoterminal.com/api/v2/networks/bsc/tokens/{contract}",
            headers={"Accept": "application/json;version=20230302"},
            timeout=10
        )
        if r.ok:
            attr = r.json().get("data", {}).get("attributes", {})
            return {
                "price_usd":    attr.get("price_usd"),
                "market_cap":   attr.get("market_cap_usd"),
                "fdv":          attr.get("fdv_usd"),
                "name":         attr.get("name") or token_name,
                "symbol":       attr.get("symbol") or symbol,
            }
    except Exception:
        pass
    return {"name": token_name, "symbol": symbol}


def detect_blind_box_candidates(supabase) -> list:
    """
    Main function: quét 2 router wallet, trả về list token candidate mới.
    [{"contract": "0x...", "symbol": "XYZ", "name": "...", "wallet": "0x...", ...}]
    """
    if not MORALIS_API_KEY:
        print("[blind_box] MORALIS_API_KEY not set, skipping")
        return []

    known = _load_known_contracts(supabase)
    candidates = {}  # contract → info

    for wallet in ROUTER_WALLETS:
        print(f"[blind_box] Scanning wallet {wallet[:10]}...")
        txns = _get_token_transfers(wallet, limit=100)
        time.sleep(0.3)  # BSCScan rate limit: 5 calls/sec

        for tx in txns:
            contract = tx.get("contractAddress", "").lower()
            symbol   = tx.get("tokenSymbol", "")
            name     = tx.get("tokenName", "")
            to_addr  = tx.get("to", "").lower()
            value    = int(tx.get("value", "0") or "0")
            decimals = int(tx.get("tokenDecimal", "18") or "18")
            amount   = value / (10 ** decimals)

            # Chỉ quan tâm token đi VÀO router (router là receiver)
            if to_addr not in [w.lower() for w in ROUTER_WALLETS]:
                continue

            # Bỏ qua token đã biết
            if contract in known:
                continue

            # Bỏ qua token số lượng quá nhỏ (< 1000)
            if amount < 1000:
                continue

            # Bỏ qua stablecoin / BNB
            skip_symbols = {"USDT", "USDC", "BUSD", "BNB", "WBNB", "ETH", "WETH"}
            if symbol.upper() in skip_symbols:
                continue

            if contract not in candidates:
                candidates[contract] = {
                    "contract":   contract,
                    "symbol":     symbol,
                    "name":       name,
                    "amount":     amount,
                    "wallet":     wallet,
                    "tx_hash":    tx.get("hash", ""),
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }
                print(f"[blind_box] 🔍 Candidate: {symbol} ({name}) | amount={amount:,.0f} | contract={contract[:12]}...")
            else:
                # Token xuất hiện ở cả 2 wallet → khả năng cao hơn
                candidates[contract]["confirmed_in_both"] = True

        time.sleep(0.25)

    result = list(candidates.values())
    if result:
        print(f"[blind_box] Found {len(result)} candidate(s): {[c['symbol'] for c in result]}")
    else:
        print("[blind_box] No new candidates detected")

    return result


def save_candidates_to_supabase(supabase, candidates: list):
    """
    Lưu candidates vào bảng blind_box_candidates.
    Tạo bảng nếu chưa có (qua insert với upsert).
    """
    if not candidates:
        return

    for c in candidates:
        try:
            # Upsert theo contract_address
            supabase.table("blind_box_candidates").upsert({
                "contract_address": c["contract"],
                "symbol":           c.get("symbol"),
                "name":             c.get("name"),
                "amount_received":  c.get("amount"),
                "detected_wallet":  c.get("wallet"),
                "tx_hash":          c.get("tx_hash"),
                "confirmed_both":   c.get("confirmed_in_both", False),
                "price_usd":        c.get("price_usd"),
                "market_cap":       c.get("market_cap"),
                "status":           "candidate",
                "detected_at":      c.get("detected_at"),
            }, on_conflict="contract_address").execute()
            print(f"[blind_box] Saved candidate: {c['symbol']} ✓")
        except Exception as e:
            print(f"[blind_box] Save candidate error: {e}")


def run_detection(supabase) -> list:
    """Entry point gọi từ scheduler."""
    # Chỉ chạy khi có pending event (tiết kiệm API calls)
    try:
        pending = supabase.table("alpha_events") \
            .select("id") \
            .eq("status", "pending") \
            .execute().data
        if not pending:
            print("[blind_box] No pending events, skipping scan")
            return []
        print(f"[blind_box] {len(pending)} pending event(s) → scanning routers...")
    except Exception as e:
        print(f"[blind_box] Check pending error: {e}")
        return []

    candidates = detect_blind_box_candidates(supabase)
    if candidates:
        # Enrich với giá từ GeckoTerminal
        for c in candidates:
            enriched = _enrich_candidate(c["contract"], c["symbol"], c["name"])
            c.update(enriched)
        save_candidates_to_supabase(supabase, candidates)

    return candidates