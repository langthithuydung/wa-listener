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


def _from_binance_web3_search(symbol: str, project_name: str = None) -> dict:
    """
    [MỚI] Tìm qua chính API của web3.binance.com (Binance Wallet token
    search) — CÙNG NGUỒN DỮ LIỆU với trang mà người dùng tự search thủ
    công để tìm contract đúng (web3.binance.com/en/token/...). Đáng tin
    hơn hẳn GeckoTerminal/DexScreener vì đây là data GỐC của Binance, có
    gắn tag "Alpha" trong tagsInfo cho token đã/đang lên Alpha — không
    phải suy đoán qua DEX aggregator bên thứ 3.

    Vẫn phải cẩn thận ticker collision: keyword search có thể trả về
    NHIỀU token trùng symbol trên các chain khác nhau → lọc ưu tiên:
    1. symbol khớp CHÍNH XÁC (không phân biệt hoa/thường)
    2. Có tag "Alpha" trong tagsInfo (dấu hiệu mạnh đây đúng là token Alpha)
    3. Nếu vẫn nhiều kết quả — chọn thanh khoản (liquidity) cao nhất
    """
    query = symbol  # search theo symbol chính xác, KHÔNG dùng project_name
                     # (tên dự án dài dễ ra kết quả tìm kiếm mờ/không liên quan)
    try:
        r = SESSION.get(
            "https://web3.binance.com/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai",
            params={"keyword": query, "chainIds": "56,1,8453,501,42161,784,146", "orderBy": "volume24h"},
            timeout=10
        )
        r.raise_for_status()
        candidates = r.json().get("data", [])
        if not candidates:
            return {}

        # Bước 1: chỉ giữ những candidate symbol khớp CHÍNH XÁC
        exact = [c for c in candidates if (c.get("symbol") or "").upper() == symbol.upper()]
        pool = exact if exact else candidates

        # Bước 2: ưu tiên candidate có tag "Alpha" (Binance tự đánh dấu)
        def _has_alpha_tag(c):
            tags = c.get("tagsInfo") or {}
            recog = tags.get("Community Recognition Level") or []
            return any((t.get("tagName") or "").lower() == "alpha" for t in recog)

        alpha_tagged = [c for c in pool if _has_alpha_tag(c)]
        pool = alpha_tagged if alpha_tagged else pool

        if not pool:
            return {}

        # Bước 3: thanh khoản cao nhất trong số còn lại
        best = sorted(pool, key=lambda c: float(c.get("liquidity") or 0), reverse=True)[0]

        chain_id = str(best.get("chainId") or "56")
        return {
            "contract_address": best.get("contractAddress"),
            "price_snapshot":   float(best.get("price") or 0) or None,
            "market_cap":       float(best.get("marketCap") or 0) or None,
            "fdv":              None,
            "chain_id":         chain_id,
            "chain_name":       CHAIN_NAMES.get(chain_id, chain_id),
            "source":           "binance_web3_search",
        }
    except Exception as e:
        print(f"[enricher] Binance Web3 search error: {e}")
        return {}


def enrich_token(symbol: str, project_name: str = None, allow_dex_fallback: bool = True) -> dict:
    """
    Main function: tìm contract + giá cho token.
    Trả về dict với các field để update Supabase.

    allow_dex_fallback: [MỚI] Khi False, CHỈ tin nguồn Binance Alpha official
    (bước 1) — không rơi xuống GeckoTerminal/DexScreener (bước 2/3).

    LÝ DO: GeckoTerminal/DexScreener search theo symbol/tên rất dễ bị
    "ticker collision" — nhiều token không liên quan nhau vẫn đặt trùng
    ký hiệu ngắn (VD "GRVT") trên các chain khác nhau. Khi event còn ở
    trạng thái upcoming/pending (Binance CHƯA chính thức đưa token vào
    Alpha token list), bước 1 sẽ luôn thất bại → dễ rơi xuống bước 2/3 và
    lấy NHẦM contract của 1 token trùng ticker hoàn toàn khác (đã xảy ra
    thật với GRVT: lấy nhầm contract 0xce152b73... trong khi contract
    đúng là 0x46F2564E...). Chỉ nên cho phép fallback DEX sau khi event
    đã "live" — lúc đó Binance official list gần như chắc chắn đã có
    token thật, giảm mạnh rủi ro trùng ticker.
    """
    if not symbol:
        return {}

    print(f"[enricher] Enriching {symbol}...")

    # 1. Binance Alpha API (chính xác nhất — token đã chính thức lên Alpha)
    result = _from_binance_alpha(symbol)
    if result.get("contract_address") and result.get("price_snapshot"):
        print(f"[enricher] {symbol} ✓ from Binance Alpha: ${result['price_snapshot']:.6f}")
        return result

    # 2. [MỚI] Binance Web3 token search — vẫn là data GỐC Binance (không
    # phải suy đoán qua DEX bên thứ 3), nên vẫn cho phép dùng ngay cả khi
    # event còn "upcoming" (khác với GeckoTerminal/DexScreener ở bước 3/4
    # bị chặn lúc upcoming để tránh ticker collision).
    result_web3 = _from_binance_web3_search(symbol, project_name)
    if result_web3.get("contract_address"):
        if result.get("price_snapshot") and not result_web3.get("price_snapshot"):
            result_web3["price_snapshot"] = result["price_snapshot"]
        print(f"[enricher] {symbol} ✓ from Binance Web3 search: ${result_web3.get('price_snapshot','?')}")
        return result_web3

    if not allow_dex_fallback:
        if result:
            print(f"[enricher] {symbol} - upcoming, chưa có trong Binance Alpha list lẫn Web3 search → BỎ QUA DEX fallback (tránh trùng ticker), giữ contract=None")
        return result  # trả về những gì Binance Alpha có (có thể rỗng), KHÔNG đoán qua DEX bên thứ 3

    # 3. GeckoTerminal
    result2 = _from_geckoterminal(symbol, project_name)
    if result2.get("contract_address"):
        # Nếu Binance Alpha có giá nhưng không có contract → merge
        if result.get("price_snapshot") and not result2.get("price_snapshot"):
            result2["price_snapshot"] = result["price_snapshot"]
        print(f"[enricher] {symbol} ✓ from GeckoTerminal: ${result2.get('price_snapshot','?')}")
        return result2

    # 4. DexScreener
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