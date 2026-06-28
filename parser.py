import json
import os
import re
from typing import Any

from google import genai

MODEL_NAME = "gemini-3.1-flash-lite"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

KEYWORDS = [
    "alpha",
    "airdrop",
    "tge",
    "token generation",
    "claim",
    "alpha points",
    "binance wallet",
]


def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in KEYWORDS)


def parse_with_regex(text: str) -> dict:
    """Parse nhanh bằng regex trước, tiết kiệm Gemini quota."""
    result = {}

    # Symbol: (XXX) hoặc $XXX
    symbol = re.search(r"\(([A-Z]{2,10})\)|\$([A-Z]{2,10})", text)
    if symbol:
        result["symbol"] = symbol.group(1) or symbol.group(2)

    # Points threshold
    points = re.search(r"(\d+)\s*alpha\s*points?", text, re.IGNORECASE)
    if points:
        result["points_threshold"] = int(points.group(1))

    # Amount per user
    amount = re.search(
        r"(\d+[\d,]*\.?\d*)\s*(tokens?|coins?)\s*per\s*user",
        text,
        re.IGNORECASE,
    )
    if amount:
        result["amount_per_user"] = float(amount.group(1).replace(",", ""))

    # Event type
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
    """Dùng Gemini khi regex không đủ."""
    if not client:
        print("[Gemini error] GEMINI_API_KEY not set")
        return {}

    prompt = f"""
Extract info from this Binance Alpha announcement.

Return ONLY valid JSON, no explanation, no markdown.

Fields to extract:
- project_name (string or null)
- symbol (string, uppercase, or null)
- event_type (string: "airdrop" or "tge" or null)
- amount_per_user (number or null)
- points_threshold (number or null)
- decay_rule (string or null)
- event_time_utc (string ISO8601 or null)

Announcement:
{text}

JSON:
""".strip()

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
            },
        )

        raw = _clean_json_text(response.text or "")
        data = json.loads(raw)

        if not isinstance(data, dict):
            return {}

        return data

    except Exception as e:
        print(f"[Gemini error] {e}")
        return {}


def parse_message(text: str) -> dict | None:
    """Main parser: regex trước, Gemini fallback."""
    if not is_relevant(text):
        return None

    result = parse_with_regex(text)

    # Nếu thiếu field quan trọng thì dùng Gemini
    missing = (
        not result.get("project_name")
        or not result.get("event_time_utc")
        or not result.get("symbol")
        or not result.get("event_type")
    )

    if missing:
        print("[Parser] Missing fields -> use Gemini")
        gemini_result = parse_with_gemini(text)
        result = {**gemini_result, **result}  # regex override Gemini nếu có

    if not result.get("symbol"):
        return None

    return result