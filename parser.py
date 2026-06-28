import re
import json
import google.generativeai as genai
import os

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-1.5-flash")  # free tier

KEYWORDS = ["alpha", "airdrop", "tge", "token generation", 
            "claim", "alpha points", "binance wallet"]

def is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in KEYWORDS)

def parse_with_regex(text: str) -> dict:
    """Parse nhanh bằng regex trước, tiết kiệm Gemini quota"""
    result = {}
    
    # Symbol: (XXX) hoặc $XXX
    symbol = re.search(r'\(([A-Z]{2,10})\)|\$([A-Z]{2,10})', text)
    if symbol:
        result["symbol"] = symbol.group(1) or symbol.group(2)
    
    # Points threshold
    points = re.search(r'(\d+)\s*alpha\s*points?', text, re.IGNORECASE)
    if points:
        result["points_threshold"] = int(points.group(1))
    
    # Amount per user
    amount = re.search(r'(\d+[\d,]*\.?\d*)\s*(tokens?|coins?)\s*per\s*user', text, re.IGNORECASE)
    if amount:
        result["amount_per_user"] = float(amount.group(1).replace(",", ""))
    
    # Event type
    if "tge" in text.lower() or "token generation" in text.lower():
        result["event_type"] = "tge"
    elif "airdrop" in text.lower():
        result["event_type"] = "airdrop"
    
    return result

def parse_with_gemini(text: str) -> dict:
    """Dùng Gemini khi regex không đủ"""
    prompt = f"""Extract info from this Binance Alpha announcement. Return ONLY valid JSON, no explanation.

Fields to extract:
- project_name (string)
- symbol (string, uppercase)  
- event_type (string: "airdrop" or "tge")
- amount_per_user (number or null)
- points_threshold (number or null)
- decay_rule (string or null, e.g. "-5 pts per 5 min")
- event_time_utc (string ISO8601 or null)

Announcement:
{text}

JSON:"""
    
    try:
        response = model.generate_content(prompt)
        raw = response.text.strip()
        # Clean markdown nếu có
        raw = re.sub(r'```json|```', '', raw).strip()
        return json.loads(raw)
    except Exception as e:
        print(f"[Gemini error] {e}")
        return {}

def parse_message(text: str) -> dict | None:
    """Main parser: regex trước, Gemini fallback"""
    if not is_relevant(text):
        return None
    
    result = parse_with_regex(text)
    
    # Nếu thiếu field quan trọng thì dùng Gemini
    missing = not result.get("symbol") or not result.get("event_type")
    if missing:
        gemini_result = parse_with_gemini(text)
        result = {**gemini_result, **result}  # regex override Gemini nếu có
    
    if not result.get("symbol"):
        return None  # Không parse được gì hết
        
    return result
