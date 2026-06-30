import json
import os
import re
import urllib.request
import urllib.error

MODEL_NAME = "gemini-2.0-flash-lite"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

KEYWORDS = [
    "alpha", "airdrop", "tge", "token generation",
    "claim", "alpha points", "binance wallet", "collect"
]

def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in KEYWORDS)


# Các từ viết hoa trong ngoặc KHÔNG phải token symbol
SYMBOL_BLACKLIST = {
    "UTC", "TGE", "AM", "PM", "GMT", "USD", "USDT", "USDC", "BNB",
    "CEO", "API", "URL", "FAQ", "TBA", "TBD", "ID", "VIP", "KYC",
    "AML", "DEX", "CEX", "NFT", "DAO", "P2P", "OTC", "BSC", "ETH",
    "SOL", "ARB", "BASE", "EVM",
}

def parse_with_regex(text: str) -> dict:
    result = {}

    # ── Symbol: (COLLECT) hoặc $COLLECT ──────────────────────────────
    for m in re.finditer(r'\(([A-Z]{2,10})\)|\$([A-Z]{2,10})', text):
        candidate = m.group(1) or m.group(2)
        if candidate not in SYMBOL_BLACKLIST:
            result["symbol"] = candidate
            break

    # ── Points threshold ──────────────────────────────────────────────
    # "224 Binance Alpha Points" / "at least 224 Alpha Points"
    points = re.search(
        r'(?:at\s+least\s+)?(\d+)\s*(?:binance\s*)?alpha\s*points?',
        text, re.IGNORECASE
    )
    if points:
        result["points_threshold"] = int(points.group(1))

    # ── Amount per user ───────────────────────────────────────────────
    # "800 COLLECT tokens" / "100 tokens per user" / "airdrop of 800 TOKEN"
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

    # ── Decay rule ────────────────────────────────────────────────────
    # "decrease by 5 points every 5 minutes"
    decay = re.search(
        r'(?:score\s+)?threshold\s+will\s+(?:automatically\s+)?decrease\s+by\s+'
        r'(\d+)\s*points?\s+every\s+(\d+)\s*minutes?',
        text, re.IGNORECASE
    )
    if decay:
        result["decay_rule"] = f"-{decay.group(1)}pts/{decay.group(2)}min"

    # ── Points cost to claim ──────────────────────────────────────────
    # "claiming the airdrop will consume 15 Binance Alpha Points"
    cost = re.search(
        r'(?:consume|cost|use)\s+(\d+)\s*(?:binance\s*)?alpha\s*points?',
        text, re.IGNORECASE
    )
    if cost:
        result["points_cost"] = int(cost.group(1))

    # ── Event type ────────────────────────────────────────────────────
    lower = text.lower()
    if "tge" in lower or "token generation" in lower:
        result["event_type"] = "tge"
    elif "airdrop" in lower:
        result["event_type"] = "airdrop"

    return result

def _clean_json_text(raw: str) -> str:
    raw = raw.strip()
    raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^```\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return raw.strip()

def parse_with_gemini(text: str) -> dict:
    if not GEMINI_API_KEY:
        print("[Gemini error] GEMINI_API_KEY not set")
        return {}

    prompt = f"""
Extract info from this Binance Alpha announcement.

Return ONLY valid JSON, no explanation, no markdown.

Fields:
- project_name (string or null)
- symbol (string or null — null if not announced yet)
- event_type (string: "airdrop" or "tge" or null)
- amount_per_user (number or null)
- points_threshold (number or null — minimum points required to claim)
- points_cost (number or null — points consumed when claiming)
- decay_rule (string or null — e.g. "-5pts/5min")
- event_time_utc (string ISO8601 or null)

Announcement:
{text}
""".strip()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={GEMINI_API_KEY}"
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
        return parsed if isinstance(parsed, dict) else {}
    except Exception as e:
        print(f"[Gemini error] {e}")
        return {}

def parse_message(text: str) -> dict | None:
    if not is_relevant(text):
        return None

    result = parse_with_regex(text)

    missing = (
        not result.get("project_name")
        or not result.get("event_time_utc")
        or not result.get("symbol")
        or not result.get("event_type")
    )

    if missing:
        print("[Parser] Missing fields → use Gemini")
        gemini_result = parse_with_gemini(text)
        # Regex ưu tiên hơn Gemini
        result = {**gemini_result, **result}

    if not result.get("event_type"):
        print("[Parser] Bỏ qua: không xác định được event_type")
        return None

    return result