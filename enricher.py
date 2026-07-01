"""
enricher.py
───────────
Tự động tìm contract address + giá cho token Alpha.

Nguồn theo thứ tự ưu tiên:
1. Binance Alpha token list API  (chính xác nhất, free)
2. GeckoTerminal API             (free, không cần key)
3. DexScreener API               (free, không cần key)
"""

import json
import os
import time
import requests

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.binance.com/",
})

CHAIN_NAMES = {
    "56": "BSC", "1": "ETH", "8453": "Base",
    "501": "SOL", "42161": "ARB", "146": "SONIC",
}

# Cache token list để không gọi API liên tục
_alpha_token_cache: dict = {}
_alpha_cache_ts: float = 0
CACHE_TTL = 180  # 3 phút


def _get_alpha_token_list() -> list:
    """Lấy toàn bộ token list từ Binance Alpha API, có cache."""
    global _alpha_token_cache, _alpha_cache_ts
    now = time.time()
    if now - _alpha_cache_ts < CACHE_TTL and _alpha_token_cache:
        return list(_alpha_token_cache.values())

    try:
        r = SESSION.get(
            "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list",
            timeout=15
        )
        r.raise_for_status()
        tokens = r.json().get("data", [])
        _alpha_token_cache = {t["symbol"].upper(): t for t in tokens if t.get("symbol")}
        _alpha_cache_ts = now
        print(f"[enricher] Binance Alpha token list: {len(_alpha_token_cache)} tokens cached")
        return tokens
    except Exception as e:
        print(f"[enricher] Binance Alpha API error: {e}")
        return list(_alpha_token_cache.values())  # trả cache cũ nếu có


def _from_binance_alpha(symbol: str) -> dict:
    """Tìm token trong Binance Alpha token list."""
    _get_alpha_token_list()  # refresh cache nếu cần
    t = _alpha_token_cache.get(symbol.upper())
    if not t:
        return {}

    chain_id = str(t.get("chainId") or "56")
    return {
        "contract_address": t.get("contractAddress"),
        "price_snapshot":   float(t.get("price") or 0) or None,
        "market_cap":       float(t.get("marketCap") or 0) or None,
        "fdv":              float(t.get("fdv") or 0) or None,
        "chain_id":         chain_id,
        "chain_name":       CHAIN_NAMES.get(chain_id, t.get("chainName", "BSC")),
        "source":           "binance_alpha",
    }


def _from_geckoterminal(symbol: str, project_name: str = None) -> dict:
    """Tìm qua GeckoTerminal — free, không cần key."""
    query = project_name or symbol
    try:
        r = SESSION.get(
            "https://api.geckoterminal.com/api/v2/search/pools",
            params={"query": query, "network": "bsc"},
            headers={"Accept": "application/json;version=20230302"},
            timeout=10
        )
        r.raise_for_status()
        pools = r.json().get("data", [])
        if not pools:
            return {}

        # Ưu tiên pool có tên khớp symbol
        best = None
        for p in pools:
            attr = p.get("attributes", {})
            rel  = p.get("relationships", {})
            base_sym = attr.get("name", "").split("/")[0].strip().upper()
            if base_sym == symbol.upper():
                best = p
                break
        if not best:
            best = pools[0]

        attr = best.get("attributes", {})
        # base token address từ relationships
        base_addr = None
        try:
            base_addr = best["relationships"]["base_token"]["data"]["id"].split("_")[-1]
        except Exception:
            pass

        price = attr.get("base_token_price_usd")
        mc    = attr.get("market_cap_usd")
        fdv   = attr.get("fdv_usd")

        return {
            "contract_address": base_addr,
            "price_snapshot":   float(price) if price else None,
            "market_cap":       float(mc) if mc else None,
            "fdv":              float(fdv) if fdv else None,
            "chain_id":         "56",
            "chain_name":       "BSC",
            "source":           "geckoterminal",
        }
    except Exception as e:
        print(f"[enricher] GeckoTerminal error: {e}")
        return {}


def _from_dexscreener(symbol: str, project_name: str = None) -> dict:
    """Fallback: DexScreener search."""
    query = project_name or symbol
    try:
        r = SESSION.get(
            f"https://api.dexscreener.com/latest/dex/search?q={query}",
            timeout=10
        )
        r.raise_for_status()
        pairs = r.json().get("pairs", [])

        # Lọc BSC, base token khớp symbol
        bsc_pairs = [
            p for p in pairs
            if p.get("chainId") == "bsc"
            and p.get("baseToken", {}).get("symbol", "").upper() == symbol.upper()
        ]
        if not bsc_pairs:
            bsc_pairs = [p for p in pairs if p.get("chainId") == "bsc"]
        if not bsc_pairs:
            return {}

        best = sorted(bsc_pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0), reverse=True)[0]
        bt = best.get("baseToken", {})

        return {
            "contract_address": bt.get("address"),
            "price_snapshot":   float(best.get("priceUsd") or 0) or None,
            "market_cap":       float(best.get("marketCap") or 0) or None,
            "fdv":              float(best.get("fdv") or 0) or None,
            "chain_id":         "56",
            "chain_name":       "BSC",
            "source":           "dexscreener",
        }
    except Exception as e:
        print(f"[enricher] DexScreener error: {e}")
        return {}


def enrich_token(symbol: str, project_name: str = None) -> dict:
    """
    Main function: tìm contract + giá cho token.
    Trả về dict với các field để update Supabase.
    """
    if not symbol:
        return {}

    print(f"[enricher] Enriching {symbol}...")

    # 1. Binance Alpha API (chính xác nhất)
    result = _from_binance_alpha(symbol)
    if result.get("contract_address") and result.get("price_snapshot"):
        print(f"[enricher] {symbol} ✓ from Binance Alpha: ${result['price_snapshot']:.6f}")
        return result

    # 2. GeckoTerminal
    result2 = _from_geckoterminal(symbol, project_name)
    if result2.get("contract_address"):
        # Nếu Binance Alpha có giá nhưng không có contract → merge
        if result.get("price_snapshot") and not result2.get("price_snapshot"):
            result2["price_snapshot"] = result["price_snapshot"]
        print(f"[enricher] {symbol} ✓ from GeckoTerminal: ${result2.get('price_snapshot','?')}")
        return result2

    # 3. DexScreener
    result3 = _from_dexscreener(symbol, project_name)
    if result3.get("contract_address"):
        print(f"[enricher] {symbol} ✓ from DexScreener: ${result3.get('price_snapshot','?')}")
        return result3

    # Có trong Binance Alpha nhưng không có pool DEX (token mới)
    if result:
        print(f"[enricher] {symbol} - in Alpha but no DEX pool yet")
        return result

    print(f"[enricher] {symbol} - not found anywhere")
    return {}


def compute_value_usd(amount_per_user, price_snapshot) -> float | None:
    """Tính tổng giá trị airdrop per user."""
    if amount_per_user and price_snapshot:
        try:
            return round(float(amount_per_user) * float(price_snapshot), 4)
        except Exception:
            pass
    return None
