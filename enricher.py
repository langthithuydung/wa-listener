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

# [MỚI] Map slug chain (GeckoTerminal/DexScreener dùng string, không phải
# numeric id) → (chain_id, chain_name) chuẩn của hệ thống mình. Cần cái
# này vì trước đây _from_geckoterminal/_from_dexscreener hardcode chỉ
# tìm trên "bsc" — token ở Base/ETH/ARB/SOL... không bao giờ được tìm
# thấy qua 2 nguồn này dù có pool thật (bug âm thầm, không throw lỗi gì
# nên rất khó nhận ra khi debug).
CHAIN_SLUG_MAP = {
    "eth": ("1", "ETH"), "ethereum": ("1", "ETH"),
    "bsc": ("56", "BSC"), "bnb": ("56", "BSC"), "bnb_smart_chain": ("56", "BSC"),
    "base": ("8453", "Base"),
    "solana": ("501", "SOL"), "sol": ("501", "SOL"),
    "arbitrum": ("42161", "ARB"), "arbitrum_one": ("42161", "ARB"),
    "sonic": ("146", "SONIC"),
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
    """
    Tìm qua GeckoTerminal — free, không cần key.

    [SỬA — BUG] Trước đây gọi API với `network: "bsc"` cứng → CHỈ BAO
    GIỜ tìm trên BSC, token ở Base/ETH/ARB/SOL... luôn trả rỗng dù có
    pool thật (không lỗi gì cả nên rất dễ bị coi nhầm là "GeckoTerminal
    không có data" trong khi thực ra do tự giới hạn chain sai). Giờ bỏ
    filter network để search TOÀN BỘ chain GeckoTerminal hỗ trợ, tự suy
    ra đúng chain_id/chain_name từ chính pool khớp thay vì gán cứng.

    [SỬA — AN TOÀN] Bỏ luôn fallback "lấy pools[0] nếu không có pool nào
    khớp symbol" của bản cũ — đó chính là kiểu code dễ dính ticker
    collision nhất (lấy đại kết quả đầu tiên dù sai token). Giờ CHỈ chấp
    nhận pool khớp CHÍNH XÁC symbol; nếu không có → trả rỗng, để hàm
    enrich_token() rơi xuống nguồn tiếp theo thay vì đoán bừa.
    """
    query = project_name or symbol
    try:
        r = SESSION.get(
            "https://api.geckoterminal.com/api/v2/search/pools",
            params={"query": query},
            headers={"Accept": "application/json;version=20230302"},
            timeout=10
        )
        r.raise_for_status()
        pools = r.json().get("data", [])
        if not pools:
            return {}

        # Trong các pool khớp ĐÚNG symbol, chọn pool thanh khoản cao nhất
        # (tránh chọn nhầm pool rác/pool giả mạo thanh khoản thấp).
        best = None
        best_liq = -1.0
        for p in pools:
            attr = p.get("attributes", {})
            base_sym = attr.get("name", "").split("/")[0].strip().upper()
            if base_sym != symbol.upper():
                continue
            liq = float(attr.get("reserve_in_usd") or 0)
            if liq > best_liq:
                best, best_liq = p, liq
        if not best:
            return {}

        attr = best.get("attributes", {})
        base_id = ""
        try:
            base_id = best["relationships"]["base_token"]["data"]["id"]
        except Exception:
            pass

        network_slug, base_addr = "", None
        if base_id and "_" in base_id:
            network_slug, base_addr = base_id.split("_", 1)
        chain_id, chain_name = CHAIN_SLUG_MAP.get(network_slug, ("56", "BSC"))

        price = attr.get("base_token_price_usd")
        mc    = attr.get("market_cap_usd")
        fdv   = attr.get("fdv_usd")

        return {
            "contract_address": base_addr,
            "price_snapshot":   float(price) if price else None,
            "market_cap":       float(mc) if mc else None,
            "fdv":              float(fdv) if fdv else None,
            "chain_id":         chain_id,
            "chain_name":       chain_name,
            "source":           "geckoterminal",
        }
    except Exception as e:
        print(f"[enricher] GeckoTerminal error: {e}")
        return {}


def _from_dexscreener(symbol: str, project_name: str = None) -> dict:
    """
    Fallback: DexScreener search.

    [SỬA — BUG] Trước đây lọc cứng `chainId == "bsc"` → cùng loại bug với
    GeckoTerminal ở trên, bỏ sót toàn bộ token ở chain khác BSC. Giờ xét
    TẤT CẢ chain DexScreener trả về, vẫn giữ nguyên tắc CHỈ nhận pair
    khớp CHÍNH XÁC symbol (bỏ fallback "pair BSC đầu tiên" cũ) để không
    bắt nhầm ticker trùng tên ở chain khác nhau.
    """
    query = project_name or symbol
    try:
        r = SESSION.get(
            f"https://api.dexscreener.com/latest/dex/search?q={query}",
            timeout=10
        )
        r.raise_for_status()
        pairs = r.json().get("pairs", [])

        exact = [
            p for p in pairs
            if p.get("baseToken", {}).get("symbol", "").upper() == symbol.upper()
        ]
        if not exact:
            return {}

        best = sorted(exact, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0), reverse=True)[0]
        bt = best.get("baseToken", {})
        chain_id, chain_name = CHAIN_SLUG_MAP.get((best.get("chainId") or "").lower(), ("56", "BSC"))

        return {
            "contract_address": bt.get("address"),
            "price_snapshot":   float(best.get("priceUsd") or 0) or None,
            "market_cap":       float(best.get("marketCap") or 0) or None,
            "fdv":              float(best.get("fdv") or 0) or None,
            "chain_id":         chain_id,
            "chain_name":       chain_name,
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


def _enrich_token_auto(symbol: str, project_name: str = None, allow_dex_fallback: bool = True) -> dict:
    """
    Pipeline tự động (Binance Alpha → Web3 search → GeckoTerminal →
    DexScreener). Gọi qua enrich_token() bên dưới — hàm đó áp thêm lớp
    override từ Supabase (bảng contract_overrides) cho case pre-TGE
    chưa có tín hiệu on-chain nào để tự nhận diện được.
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


# ── Override thủ công cho case KHÔNG THỂ tự động được ─────────────────
# Token pre-TGE chưa có pool thanh khoản nào ở đâu hết thì không có tín
# hiệu on-chain nào để phân biệt với token trùng ticker khác — không
# GeckoTerminal/DexScreener/Web3 search nào đoán đúng được, dù đã sửa
# hết bug hardcode BSC ở trên. Trường hợp này CHỈ giải quyết được bằng
# tay (tra qua nguồn ngoài rồi tự nhập).
#
# Thay vì hardcode vào code (phải sửa file + deploy lại mỗi token), lưu
# vào 1 bảng Supabase — set/sửa chỉ cần 1 câu INSERT/UPDATE SQL, không
# cần đụng tới enricher.py hay deploy lại lần nào nữa, dùng được cho
# hàng trăm token sau này y hệt nhau:
#
#   create table if not exists contract_overrides (
#     symbol text primary key,
#     contract_address text not null,
#     chain_id text,
#     chain_name text,
#     note text,
#     created_at timestamptz default now()
#   );
#
#   insert into contract_overrides (symbol, contract_address, chain_id, chain_name, note)
#   values ('QUID', '0x1a44233FAe8D50F1AeB3a5d58dd426ff4814Cb53', '8453', 'Base', 'pre-TGE, verified BaseScan 2026-08-04')
#   on conflict (symbol) do update set
#     contract_address = excluded.contract_address,
#     chain_id = excluded.chain_id,
#     chain_name = excluded.chain_name,
#     note = excluded.note;
#
# Xoá dòng khỏi bảng khi token đã "live" ổn định trên Binance Alpha API
# thật — lúc đó bước 1 tự lấy đúng, override không còn cần thiết nữa.
_override_cache: dict = {}
_override_cache_ts: float = 0
OVERRIDE_CACHE_TTL = 60  # cache ngắn (1 phút) vì admin có thể sửa bảng này bất cứ lúc nào


def _get_manual_overrides() -> dict:
    global _override_cache, _override_cache_ts
    now = time.time()
    if now - _override_cache_ts < OVERRIDE_CACHE_TTL and _override_cache:
        return _override_cache
    try:
        from supabase import create_client
        supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        rows = supabase.table("contract_overrides").select("*").execute().data
        _override_cache = {r["symbol"].upper(): r for r in rows}
        _override_cache_ts = now
    except Exception as e:
        # Bảng chưa tồn tại hoặc lỗi kết nối — không làm hỏng flow enrich
        # bình thường, chỉ đơn giản là không có override nào áp dụng được.
        print(f"[enricher] contract_overrides lookup skipped: {e}")
    return _override_cache


def enrich_token(symbol: str, project_name: str = None, allow_dex_fallback: bool = True) -> dict:
    """
    Main function: tìm contract + giá cho token.
    Chạy pipeline tự động trước (_enrich_token_auto — đã sửa để search
    đa chain, không còn giới hạn BSC), rồi áp override từ bảng
    contract_overrides trên Supabase (nếu có entry cho symbol này) đè
    lên contract_address/chain — dành cho case pre-TGE không có tín hiệu
    on-chain nào để tự nhận diện. Giá/market cap/fdv vẫn giữ nguyên từ
    nguồn tự động nếu tìm được.
    """
    result = _enrich_token_auto(symbol, project_name, allow_dex_fallback)

    # [QUAN TRỌNG] Chỉ áp override khi Binance Alpha CHƯA niêm yết chính
    # thức (source khác "binance_alpha", hoặc thiếu contract). Một khi
    # token đã lên chính thức trên Binance Alpha token list (đủ cả
    # contract + giá — đây là nguồn tin cậy tuyệt đối), TỰ ĐỘNG bỏ qua
    # override và dùng thẳng data thật — không cần vào Supabase xoá tay
    # dòng override. Nhờ vậy quy trình là: auto chạy bình thường → sai
    # thì tự tra rồi SQL đè tạm → lúc Binance niêm yết chính thức, job
    # 5 phút tự nhận ra và tự chuyển sang tin data official, khỏi cần
    # dọn dẹp gì thêm.
    is_official = result.get("source") == "binance_alpha" and result.get("contract_address")

    if not is_official:
        override = _get_manual_overrides().get((symbol or "").upper())
        if override:
            result = dict(result) if result else {}
            result["contract_address"] = override["contract_address"]
            result["chain_id"]   = override.get("chain_id") or result.get("chain_id")
            result["chain_name"] = override.get("chain_name") or result.get("chain_name")
            result["source"] = (result.get("source") or "") + "+manual_override"
            print(f"[enricher] {symbol} ✓ contract từ contract_overrides (Supabase, tạm — chưa niêm yết chính thức): {override['contract_address']}")
    else:
        print(f"[enricher] {symbol} ✓ đã niêm yết chính thức trên Binance Alpha — bỏ qua override, dùng data official")

    return result


def compute_value_usd(amount_per_user, price_snapshot) -> float | None:
    """Tính tổng giá trị airdrop per user."""
    if amount_per_user and price_snapshot:
        try:
            return round(float(amount_per_user) * float(price_snapshot), 4)
        except Exception:
            pass
    return None