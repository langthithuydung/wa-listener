import json
import os
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, date, timedelta

MODEL_NAME = "gemini-3.1-flash-lite"  # 2.0-flash-lite đã bị khai tử 1/6/2026

# Hỗ trợ nhiều API key, phân cách bằng dấu phẩy: "key1,key2,key3"
_raw_keys = os.getenv("GEMINI_API_KEYS") or os.getenv("GEMINI_API_KEY", "")
GEMINI_API_KEYS = [k.strip() for k in _raw_keys.split(",") if k.strip()]

_last_call_ts = 0.0
MIN_INTERVAL = 2.0  # giây giữa 2 lần gọi Gemini

# Circuit breaker: khi tất cả key đều bị rate limit, tạm ngưng gọi Gemini
_cooldown_until = 0.0
COOLDOWN_SECONDS = 300  # 5 phút

# "binance wallet" đơn lẻ quá rộng → match nhầm với campaign quảng cáo không liên quan Alpha
# Bắt buộc phải có "alpha" HOẶC ("airdrop"/"tge" + ngữ cảnh cụ thể)
STRONG_KEYWORDS = ["alpha points", "binance alpha", "alpha box", "alpha events"]
MEDIUM_KEYWORDS = ["airdrop", "tge", "token generation"]

def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    if any(kw in text_lower for kw in STRONG_KEYWORDS):
        return True
    # airdrop/tge chỉ tính khi ĐI KÈM "alpha" (loại các airdrop campaign chung chung)
    if any(kw in text_lower for kw in MEDIUM_KEYWORDS) and "alpha" in text_lower:
        return True
    return False


# Tin "follow-up campaign" — VD "GRVT Deposit Campaign": hướng dẫn user
# ĐÃ NHẬN airdrop rồi đi deposit/transfer token vào ví/Alpha Account để
# nhận thêm thưởng phụ. Đây KHÔNG PHẢI sự kiện claim airdrop mới — không
# có ngưỡng Alpha Points thật, symbol dễ bị nhận nhầm (VD "(MPC)" — viết
# tắt loại ví bị hiểu lầm thành symbol). Case thật: msg 1608 (GRVT gốc)
# tự báo trước "We also have a GRVT Deposit Campaign coming up!", rồi
# msg 1609 chính là tin follow-up đó — bị parse thành 1 event RÁC riêng
# với symbol="MPC" sai hoàn toàn, points_threshold=300 lấy nhầm từ số
# lượng token cần deposit (không phải Alpha Points).
FOLLOWUP_CAMPAIGN_SIGNALS = [
    "deposit campaign",
    "who received their airdrop",
    "received your airdrop",
    "received their airdrop",
]

def is_followup_campaign(text: str) -> bool:
    text_lower = text.lower()
    return any(sig in text_lower for sig in FOLLOWUP_CAMPAIGN_SIGNALS)


SYMBOL_BLACKLIST = {
    "UTC", "TGE", "AM", "PM", "GMT", "USD", "USDT", "USDC", "BNB",
    "CEO", "API", "URL", "FAQ", "TBA", "TBD", "ID", "VIP", "KYC",
    "AML", "DEX", "CEX", "NFT", "DAO", "P2P", "OTC", "BSC", "ETH",
    "SOL", "ARB", "BASE", "EVM",
    "MPC",  # "Binance Wallet (MPC)" — viết tắt loại ví (Multi-Party
            # Computation), KHÔNG phải symbol token. Case thật đã gặp:
            # tin "GRVT Deposit Campaign" nhắc "Binance Wallet(MPC)" bị
            # nhận nhầm MPC thành symbol, ghi đè lên GRVT.
}


def _parse_relative_event_time(text: str, msg_date=None) -> str | None:
    """
    Binance hay ghi giờ kiểu TƯƠNG ĐỐI: "today at 9:00 (UTC)",
    "trade today at 10:00 (UTC)" — không có ngày cụ thể trong text.
    Phải dùng NGÀY THẬT của tin nhắn Telegram (msg_date) làm mốc "today",
    rồi ghép với giờ trong text để ra ISO datetime tuyệt đối.

    Nếu không có msg_date (ví dụ gọi từ /test không có ngày thật) thì bỏ
    qua, không đoán bừa ngày hiện tại của server (có thể sai múi giờ/lúc
    chạy catch-up cho tin cũ).
    """
    if not msg_date:
        return None

    # Khớp: "today at 9:00 (UTC)" / "today at 09:00 UTC" / "trade today at 10:00(UTC)"
    m = re.search(
        r'today\s+at\s+(\d{1,2}):(\d{2})\s*\(?\s*UTC\s*\)?',
        text, re.IGNORECASE
    )
    if not m:
        return None

    hour, minute = int(m.group(1)), int(m.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    # msg_date từ Telethon luôn là datetime UTC-aware — lấy đúng NGÀY của
    # tin nhắn đó làm "today", không dùng ngày giờ server hiện tại.
    event_date: date = msg_date.date() if hasattr(msg_date, "date") else msg_date
    event_dt = datetime(
        event_date.year, event_date.month, event_date.day,
        hour, minute, tzinfo=timezone.utc
    )
    return event_dt.isoformat()


_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

def _parse_absolute_event_time(text: str, msg_date=None) -> str | None:
    """
    Binance cũng hay ghi giờ kiểu TUYỆT ĐỐI ngay trong text (khác hẳn
    dạng "today at..." ở hàm trên): "trading starting on July 27, 2026,
    at 10:00 (UTC)", "debut and trading starting on January 30, 2026, at
    12:00 (UTC)". Trước đây KHÔNG có hàm nào bắt được dạng này bằng regex
    — chỉ trông chờ Gemini fallback, nên khi Gemini không trả (rate limit/
    cooldown/lỗi) hoặc tự bỏ sót, event_time bị bỏ trống hoàn toàn dù text
    ghi rõ ràng ngày giờ.

    SAFETY CHECK quan trọng: nếu có msg_date (ngày đăng tin thật) và ngày
    bắt được lại nằm QUÁ KHỨ xa so với lúc đăng (>2 ngày trước) — nhiều
    khả năng Binance dùng nhầm template cũ/copy-paste sai tháng (case thật
    đã gặp: tin đăng 29/07/2026 nhưng ghi "January 30, 2026" — rất có thể
    lẽ ra phải là "July 30"). Trường hợp này KHÔNG dùng ngày bắt được, để
    trống còn hơn để event bị hệ thống tự động đóng "ended" oan chỉ vì
    tưởng nó đã quá hạn từ tháng 1.
    """
    m = re.search(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
        r'(\d{1,2}),?\s+(\d{4}),?\s+at\s+(\d{1,2}):(\d{2})\s*\(?\s*UTC\s*\)?',
        text, re.IGNORECASE
    )
    if not m:
        return None

    month_name, day, year, hour, minute = m.groups()
    month = _MONTH_NAMES.get(month_name.lower())
    day, year, hour, minute = int(day), int(year), int(hour), int(minute)
    if not month or not (1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    try:
        event_dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None  # VD "February 30" — ngày không tồn tại

    if msg_date:
        posted = msg_date if hasattr(msg_date, "tzinfo") else None
        if posted and event_dt < posted - timedelta(days=2):
            print(f"[Parser] Sanity: ngày tuyệt đối {event_dt.isoformat()} nằm quá khứ xa so với "
                  f"lúc đăng tin ({posted.isoformat()}) — nghi Binance dùng nhầm template/copy-paste "
                  f"sai tháng, BỎ QUA để tránh event bị tự động đóng oan")
            return None

    return event_dt.isoformat()


def _parse_multi_token_tiers(text: str) -> list[dict]:
    """
    Alpha Box nhiều token: mỗi token có 3 mức thưởng (Common/Rare/Super Rare).
    Bắt pattern: "X, Y, or Z SYMBOL tokens" lặp lại cho từng token.
    VD: "69, 86, or 244 EDGE tokens; 584, 729, or 2083 BEE tokens"
    → [{"symbol":"EDGE","tier_common":69,"tier_rare":86,"tier_super_rare":244}, ...]

    Thứ tự số tăng dần ứng với Common(80% pool, giá trị thấp nhất) →
    Rare(15%) → Super Rare(5%, giá trị cao nhất) — đúng theo mô tả tier
    trong thông báo Alpha Box gốc của Binance.
    """
    pattern = re.compile(
        r'(\d[\d,]*)\s*,\s*(\d[\d,]*)\s*,?\s*or\s*(\d[\d,]*)\s+([A-Z]{2,10})\s+tokens?',
        re.IGNORECASE
    )
    tokens = []
    seen = set()
    for m in pattern.finditer(text):
        low, mid, high, symbol = m.groups()
        sym = symbol.upper()
        if sym in seen or sym in SYMBOL_BLACKLIST:
            continue
        seen.add(sym)
        tokens.append({
            "symbol": sym,
            "tier_common": float(low.replace(",", "")),
            "tier_rare": float(mid.replace(",", "")),
            "tier_super_rare": float(high.replace(",", "")),
        })
    return tokens


def parse_with_regex(text: str, msg_date=None) -> dict:
    result = {}

    # ── Alpha Box nhiều token: ưu tiên bắt theo tier reward trước (chính
    # xác hơn nhiều so với chỉ quét symbol trong ngoặc đơn, vì nó đảm bảo
    # symbol thực sự gắn với số lượng thưởng của chính nó, không lẫn lộn). ──
    tokens_detail = _parse_multi_token_tiers(text)

    if tokens_detail:
        result["tokens_detail"] = tokens_detail
        all_symbols = [t["symbol"] for t in tokens_detail]
        result["symbol"] = all_symbols[0]
        if len(all_symbols) > 1:
            result["symbols_all"] = ",".join(all_symbols)
        # amount_per_user cho symbol CHÍNH — lấy tier phổ biến nhất (Common)
        # làm giá trị đại diện, khớp với cách hệ thống hiển thị amount hiện có.
        result["amount_per_user"] = tokens_detail[0]["tier_common"]
    else:
        # Fallback: không có pattern tier (airdrop 1 token thường, không phải
        # Alpha Box nhiều loại) → quét symbol trong ngoặc đơn như cũ.
        all_symbols = []
        for m in re.finditer(r'\(([A-Z]{2,10})\)|\$([A-Z]{2,10})', text):
            candidate = m.group(1) or m.group(2)
            if candidate not in SYMBOL_BLACKLIST and candidate not in all_symbols:
                all_symbols.append(candidate)

        if all_symbols:
            result["symbol"] = all_symbols[0]
            if len(all_symbols) > 1:
                result["symbols_all"] = ",".join(all_symbols)

    points = re.search(
        r'(?:at\s+least\s+)?(\d+)\s*(?:binance\s*)?alpha\s*points?',
        text, re.IGNORECASE
    )
    if points:
        result["points_threshold"] = int(points.group(1))

    # CHỈ chạy fallback này khi CHƯA có tokens_detail (Alpha Box nhiều token
    # đã tự set amount_per_user chính xác ở trên rồi — không được ghi đè).
    if "tokens_detail" not in result:
        amount = re.search(
            r'(?:airdrop\s+of\s+|claim\s+(?:an?\s+)?(?:airdrop\s+of\s+)?)?'
            r'(\d[\d,]*)\s+[A-Z]{2,10}\s+tokens?',
            text, re.IGNORECASE
        )
        if not amount:
            amount = re.search(
                r'(\d[\d,]*\.?\d*)\s*(?:tokens?|coins?)\s*per\s*user',
                text, re.IGNORECASE
            )
        if amount:
            result["amount_per_user"] = float(amount.group(1).replace(",", ""))

    decay = re.search(
        r'(?:score\s+)?threshold\s+will\s+(?:automatically\s+)?decrease\s+by\s+'
        r'(\d+)\s*points?\s+every\s+(\d+)\s*minutes?',
        text, re.IGNORECASE
    )
    if decay:
        result["decay_rule"] = f"-{decay.group(1)}pts/{decay.group(2)}min"

    cost = re.search(
        r'(?:consume|cost|use)\s+(\d+)\s*(?:binance\s*)?alpha\s*points?',
        text, re.IGNORECASE
    )
    if cost:
        result["points_cost"] = int(cost.group(1))

    lower = text.lower()
    if "tge" in lower or "token generation" in lower:
        result["event_type"] = "tge"
    elif "airdrop" in lower:
        result["event_type"] = "airdrop"

    # Giờ tương đối "today at HH:MM (UTC)" → ISO datetime tuyệt đối
    event_time = _parse_relative_event_time(text, msg_date)
    if not event_time:
        # [MỚI] Thử dạng ngày TUYỆT ĐỐI ghi thẳng trong text (VD "July 27,
        # 2026, at 10:00 (UTC)") — trước đây chỉ Gemini xử lý được dạng
        # này, giờ có thêm regex xử lý trực tiếp, không phụ thuộc Gemini
        # (nhanh hơn, không tốn quota, và không bị rate-limit/cooldown).
        event_time = _parse_absolute_event_time(text, msg_date)
    if event_time:
        result["event_time_utc"] = event_time

    return result

def _clean_json_text(raw: str) -> str:
    raw = raw.strip()
    raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^```\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return raw.strip()


def _call_gemini_with_key(prompt: str, api_key: str) -> tuple[bool, dict, bool]:
    """
    Gọi Gemini với 1 key cụ thể.
    Return: (success, result_dict, is_rate_limited)
    """
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0, "responseMimeType": "application/json"}
    }
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text_out = data["candidates"][0]["content"]["parts"][0]["text"]
        raw = _clean_json_text(text_out)
        parsed = json.loads(raw)
        return (True, parsed if isinstance(parsed, dict) else {}, False)
    except urllib.error.HTTPError as e:
        is_rate_limited = (e.code == 429)
        print(f"[Gemini error] key=...{api_key[-6:]} HTTP {e.code}: {'Too Many Requests' if is_rate_limited else e.reason}")
        return (False, {}, is_rate_limited)
    except Exception as e:
        print(f"[Gemini error] key=...{api_key[-6:] if api_key else '??????'} {e}")
        return (False, {}, False)


def parse_with_gemini(text: str) -> dict:
    global _last_call_ts, _cooldown_until

    if not GEMINI_API_KEYS:
        return {}

    # Circuit breaker: đang trong thời gian cooldown → bỏ qua Gemini hoàn toàn
    now = time.time()
    if now < _cooldown_until:
        remaining = int(_cooldown_until - now)
        print(f"[Gemini] In cooldown ({remaining}s left), skip call")
        return {}

    # Rate limit: đảm bảo khoảng cách tối thiểu giữa các lần gọi
    elapsed = now - _last_call_ts
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_call_ts = time.time()

    prompt = f"""
Extract info from this Binance Alpha announcement.

Return ONLY valid JSON, no explanation, no markdown.

Fields:
- project_name (string or null)
- symbol (string or null — null if not announced yet)
- event_type (string: "airdrop" or "tge" or null)
- amount_per_user (number or null)
- points_threshold (number or null)
- points_cost (number or null)
- decay_rule (string or null)
- event_time_utc (string ISO8601 or null — CHỈ điền nếu text có NGÀY CỤ THỂ,
  KHÔNG được tự suy đoán ngày cho các cụm "today"/"tomorrow" vì bạn không
  biết chính xác tin được đăng ngày nào)

Announcement:
{text}
""".strip()

    # Thử lần lượt từng key, xoay vòng khi bị 429
    for i, key in enumerate(GEMINI_API_KEYS):
        success, result, is_rate_limited = _call_gemini_with_key(prompt, key)
        if success:
            return result
        if not is_rate_limited:
            # Lỗi khác (không phải rate limit) → không thử key khác, dừng luôn
            return {}
        # Rate limited → thử key tiếp theo
        if i < len(GEMINI_API_KEYS) - 1:
            print(f"[Gemini] Rotating to next key ({i+2}/{len(GEMINI_API_KEYS)})...")

    # Tất cả key đều bị rate limit → kích hoạt cooldown, ngưng gọi Gemini một thời gian
    _cooldown_until = time.time() + COOLDOWN_SECONDS
    print(f"[Gemini error] All {len(GEMINI_API_KEYS)} key(s) rate limited → cooldown {COOLDOWN_SECONDS}s")
    return {}


# Symbol/project_name không hợp lệ (Gemini hay hallucinate mấy cái này)
INVALID_SYMBOLS = {"USDT", "USDC", "BNB", "BUSD", "BTC", "ETH", "N/A", "NONE", "NULL"}
INVALID_PROJECT_NAMES = {"BINANCE", "BINANCE ALPHA", "BINANCE WALLET", "N/A", "NONE"}

def _sanity_check(result: dict, original_text: str = "") -> dict:
    """Loại bỏ field bị Gemini hallucinate rõ ràng sai logic."""
    # Symbol không hợp lệ
    sym = (result.get("symbol") or "").upper()
    if sym in INVALID_SYMBOLS:
        result["symbol"] = None

    # Project name không hợp lệ
    pname = (result.get("project_name") or "").upper()
    if pname in INVALID_PROJECT_NAMES:
        result["project_name"] = None

    # event_time_utc năm không hợp lý (phải là năm nay hoặc năm sau)
    et = result.get("event_time_utc")
    if et:
        try:
            year = int(str(et)[:4])
            current_year = datetime.now().year
            if year < current_year or year > current_year + 1:
                result["event_time_utc"] = None
        except Exception:
            result["event_time_utc"] = None

    # amount_per_user quá nhỏ bất thường (Gemini hallucinate "1")
    amt = result.get("amount_per_user")
    if amt is not None and amt < 1:
        result["amount_per_user"] = None

    # amount_per_user PHẢI thực sự xuất hiện trong text gốc, nếu không → hallucination
    amt = result.get("amount_per_user")
    if amt is not None and original_text:
        amt_int = int(amt) if amt == int(amt) else amt
        if str(amt_int) not in original_text and str(amt) not in original_text:
            print(f"[Parser] Sanity: amount_per_user={amt} không xuất hiện trong text gốc → loại bỏ")
            result["amount_per_user"] = None

    # points_threshold cũng phải xuất hiện trong text
    pts = result.get("points_threshold")
    if pts is not None and original_text:
        if str(int(pts)) not in original_text:
            print(f"[Parser] Sanity: points_threshold={pts} không xuất hiện trong text gốc → loại bỏ")
            result["points_threshold"] = None

    return result


def parse_message(text: str, msg_date=None) -> dict | None:
    """
    msg_date: thời gian THẬT tin được đăng trên Telegram (từ Telethon
    message.date) — dùng để quy đổi các mốc giờ tương đối kiểu
    "today at 9:00 (UTC)" thành ngày giờ tuyệt đối chính xác. Không truyền
    (None) vẫn hoạt động bình thường, chỉ là event_time_utc sẽ trống cho
    các tin dạng "today at..." (an toàn hơn là đoán sai).
    """
    if not is_relevant(text):
        return None

    if is_followup_campaign(text):
        print("[Parser] Bỏ qua: tin follow-up (Deposit/Transfer Campaign) của token ĐÃ airdrop rồi, không phải sự kiện claim mới")
        return None

    result = parse_with_regex(text, msg_date=msg_date)

    missing = (
        not result.get("project_name")
        or not result.get("event_time_utc")
        or not result.get("symbol")
        or not result.get("event_type")
    )

    if missing:
        print("[Parser] Missing fields → use Gemini")
        gemini_result = parse_with_gemini(text)
        gemini_result = _sanity_check(gemini_result, original_text=text)
        result = {**gemini_result, **result}

    if not result.get("event_type"):
        print("[Parser] Bỏ qua: không xác định được event_type")
        return None

    return result