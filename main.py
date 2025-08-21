import os
import logging
import time
import json
import asyncio
from typing import Optional, Dict, Any, Set
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
import gspread
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hubspot_telegram_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_TOKEN", "")

# Allow overriding internal property names if they differ in your HubSpot portal
DEAL_OWNER_PROP = os.getenv("HUBSPOT_DEAL_OWNER_PROP", "hubspot_owner_id")
DEAL_LOCATION_PROP = os.getenv("HUBSPOT_DEAL_LOCATION_PROP", "location")
DISTRIBUTION_FLAG_PROP = os.getenv("HUBSPOT_DISTRIBUTION_FLAG_PROP", "distribution_flag")
MAIN_PRACTICE_PROP = os.getenv("HUBSPOT_MAIN_PRACTICE_PROP", "main_practice")

HS_BASE = "https://api.hubapi.com"
HS_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

HUBSPOT_OWNERS_CACHE_TTL = int(os.getenv("HUBSPOT_OWNERS_CACHE_TTL", "900"))
COMPANY_NAME_PROP = os.getenv("HUBSPOT_COMPANY_NAME_PROP", "name")
TELEGRAM_MENTIONS_JSON = os.getenv("TELEGRAM_MENTIONS_JSON", "")
TELEGRAM_OWNER_MENTIONS_JSON = os.getenv("TELEGRAM_OWNER_MENTIONS_JSON", "")
HUBSPOT_PORTAL_ID = os.getenv("HUBSPOT_PORTAL_ID", "")
REMINDER_TEST_MINUTES = int(os.getenv("REMINDER_TEST_MINUTES", "0"))
# Optional: mapping from internal dealstage values -> pretty labels
DEALSTAGE_MAP_JSON = os.getenv("HUBSPOT_DEALSTAGE_MAP_JSON", "")
# Google Sheets settings
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")
GOOGLE_SHEETS_SHEET_NAME = os.getenv("GOOGLE_SHEETS_SHEET_NAME", "Deals")

# Cache for HubSpot owners: owner_id -> "First Last" (fallbacks to email/id)
_OWNERS_MAP_CACHE: Dict[str, str] = {}
_OWNERS_MAP_TS: float = 0.0

# Mentions mapping cache (surname -> @username or telegram user id)
_MENTIONS_MAP: Dict[str, Any] = {}
try:
    if TELEGRAM_MENTIONS_JSON.strip():
        parsed = json.loads(TELEGRAM_MENTIONS_JSON)
        if isinstance(parsed, dict):
            _MENTIONS_MAP = {str(k).strip().lower(): v for k, v in parsed.items()}
except Exception:
    logger.exception("Failed to parse TELEGRAM_MENTIONS_JSON; ignoring")

_OWNER_MENTIONS_MAP: Dict[str, Any] = {}
try:
    if TELEGRAM_OWNER_MENTIONS_JSON.strip():
        parsed = json.loads(TELEGRAM_OWNER_MENTIONS_JSON)
        if isinstance(parsed, dict):
            _OWNER_MENTIONS_MAP = {str(k).strip(): v for k, v in parsed.items()}
except Exception:
    logger.exception("Failed to parse TELEGRAM_OWNER_MENTIONS_JSON; ignoring")

_INTEREST_USERS: Dict[str, Set[int]] = {}

# Dealstage mapping cache (internal value -> pretty label)
_DEALSTAGE_MAP: Dict[str, str] = {}
try:
    if DEALSTAGE_MAP_JSON.strip():
        parsed = json.loads(DEALSTAGE_MAP_JSON)
        if isinstance(parsed, dict):
            _DEALSTAGE_MAP = {str(k).strip(): str(v) for k, v in parsed.items()}
except Exception:
    logger.exception("Failed to parse HUBSPOT_DEALSTAGE_MAP_JSON; ignoring")

# In-memory deduplication of initial posts per deal_id
_POSTED_DEALS: Set[str] = set()
_POST_LOCK = asyncio.Lock()

def build_interest_keyboard(deal_id: str, count: int) -> InlineKeyboardMarkup:
    label = f"Интересуюсь ({count})" if count > 0 else "Интересуюсь (0)"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(text=label, callback_data=f"interest:{deal_id}")],
        [InlineKeyboardButton(text="Список", callback_data=f"list:{deal_id}")]
    ])

_gs_client = None

def get_gs_client():
    global _gs_client
    if _gs_client:
        return _gs_client
    try:
        if GOOGLE_SERVICE_ACCOUNT_JSON.strip():
            info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = service_account.Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
            _gs_client = gspread.authorize(creds)
            return _gs_client
        if GOOGLE_APPLICATION_CREDENTIALS.strip():
            creds = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/spreadsheets"])
            _gs_client = gspread.authorize(creds)
            return _gs_client
    except Exception:
        logger.exception("Failed to init Google Sheets client")
    return None

def append_deal_row_to_sheet(row: list[Any]):
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return
    client = get_gs_client()
    if not client:
        return
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        try:
            ws = sh.worksheet(GOOGLE_SHEETS_SHEET_NAME)
        except Exception:
            ws = sh.add_worksheet(title=GOOGLE_SHEETS_SHEET_NAME, rows=1000, cols=50)
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to append row to Google Sheet")

def append_interest_row_to_sheet(row: list[Any]):
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return
    client = get_gs_client()
    if not client:
        return
    try:
        sh = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        sheet_name = os.getenv("GOOGLE_SHEETS_INTEREST_SHEET_NAME", "Interest")
        try:
            ws = sh.worksheet(sheet_name)
        except Exception:
            ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=10)
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("Failed to append interest row to Google Sheet")

async def interest_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        data = query.data or ""
        if data.startswith("interest:"):
            deal_id = data.split(":", 1)[1]
            user = query.from_user
            alias = f"@{user.username}" if user and user.username else str(user.id)
            ts = datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M")
            # Update in-memory set and compute count
            users = _INTEREST_USERS.setdefault(deal_id, set())
            first_time = user.id not in users
            users.add(user.id)
            count = len(users)
            # Update button counter, keep visible
            try:
                await query.edit_message_reply_markup(reply_markup=build_interest_keyboard(deal_id, count))
            except Exception:
                logger.exception("Failed to update interest keyboard for deal %s", deal_id)
            # Log only first click to sheet
            if first_time:
                append_interest_row_to_sheet([deal_id, ts, alias])
            # Only toast, no extra message
            await query.answer(text=("Интерес зафиксирован" if first_time else "Вы уже отмечались"), show_alert=False)
        elif data.startswith("list:"):
            deal_id = data.split(":", 1)[1]
            users = _INTEREST_USERS.get(deal_id, set())
            if not users:
                await query.answer(text="Пока никто не отметил интерес", show_alert=True)
                return
            # Format user list for popup
            user_list = []
            for user_id in sorted(users):
                try:
                    # Try to get user info from Telegram
                    chat_member = await context.bot.get_chat_member(TELEGRAM_CHAT_ID, user_id)
                    user_name = chat_member.user.first_name or ""
                    user_last_name = chat_member.user.last_name or ""
                    full_name = f"{user_name} {user_last_name}".strip()
                    if full_name:
                        user_list.append(f"• {full_name}")
                    else:
                        user_list.append(f"• ID: {user_id}")
                except Exception:
                    # Fallback to user ID if can't get name
                    user_list.append(f"• ID: {user_id}")
            text = f"Интересуются по сделке {deal_id}:\n" + "\n".join(user_list)
            # Limit popup text length (Telegram has limits)
            if len(text) > 200:
                text = text[:197] + "..."
            await query.answer(text=text, show_alert=True)
        else:
            await query.answer()
    except Exception:
        logger.exception("Failed to handle interest callback")

def hs_get_owners_map() -> Dict[str, str]:
    global _OWNERS_MAP_CACHE, _OWNERS_MAP_TS
    now = time.time()
    if _OWNERS_MAP_CACHE and (now - _OWNERS_MAP_TS) < HUBSPOT_OWNERS_CACHE_TTL:
        return _OWNERS_MAP_CACHE

    url = f"{HS_BASE}/crm/v3/owners"
    params: Dict[str, Any] = {"archived": "false", "limit": 100}
    owners_map: Dict[str, str] = {}
    try:
        while True:
            r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
            if not r.ok:
                logger.warning("Owners API request failed: %s %s", r.status_code, r.text)
                break
            data = r.json() or {}
            for owner in data.get("results", []) or []:
                owner_id = str(owner.get("id", "")).strip()
                if not owner_id:
                    continue
                first_name = (owner.get("firstName") or "").strip()
                last_name = (owner.get("lastName") or "").strip()
                full_name = (f"{first_name} {last_name}").strip()
                if not full_name:
                    full_name = (owner.get("email") or "").strip() or owner_id
                owners_map[owner_id] = full_name
            paging = (data.get("paging") or {}).get("next") or {}
            after = paging.get("after")
            if after:
                params["after"] = after
            else:
                break
    except Exception:
        logger.exception("Failed to fetch owners from HubSpot; using cached/empty map")

    if owners_map:
        _OWNERS_MAP_CACHE = owners_map
        _OWNERS_MAP_TS = now
    return _OWNERS_MAP_CACHE

def render_owner_name(raw_owner_id: Any) -> str:
    if raw_owner_id is None:
        return ""
    owner_id_str = str(raw_owner_id).strip()
    if not owner_id_str:
        return ""
    owners_map = hs_get_owners_map()
    return owners_map.get(owner_id_str, owner_id_str)

def render_mentions_from_surnames(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""
    # Split by semicolon or comma
    tokens = [t.strip() for t in text.replace(',', ';').split(';') if t.strip()]
    mentions: list[str] = []
    for token in tokens:
        # Try mapping by: full name, then last word (surname), then first word
        lc_full = token.lower()
        words = [w for w in token.split() if w]
        candidates = [lc_full]
        if len(words) >= 1:
            candidates.append(words[-1].lower())  # surname as last word
        if len(words) >= 2:
            candidates.append(words[0].lower())   # first name as a fallback
        mapped = None
        for cand in candidates:
            mapped = _MENTIONS_MAP.get(cand)
            if mapped is not None:
                break
        if mapped is None:
            # No mapping -> keep as plain text token
            mentions.append(token)
            continue
        if isinstance(mapped, int) or (isinstance(mapped, str) and mapped.isdigit()):
            user_id = int(mapped)
            mentions.append(f"<a href=\"tg://user?id={user_id}\">{token}</a>")
        elif isinstance(mapped, str) and mapped.startswith('@'):
            mentions.append(mapped)
        elif isinstance(mapped, str) and mapped:
            # treat non-@ string as display text
            mentions.append(mapped)
        else:
            mentions.append(token)
    return ", ".join(mentions)

def render_owner_mention(owner_id: Any, fallback_name: str) -> str:
    if owner_id is None:
        return fallback_name or ""
    key = str(owner_id).strip()
    mapped = _OWNER_MENTIONS_MAP.get(key)
    if mapped is None:
        return fallback_name or key
    if isinstance(mapped, int) or (isinstance(mapped, str) and mapped.isdigit()):
        user_id = int(mapped)
        display = fallback_name or key
        return f"<a href=\"tg://user?id={user_id}\">{display}</a>"
    if isinstance(mapped, str) and mapped.startswith('@'):
        return mapped
    if isinstance(mapped, str) and mapped:
        return mapped
    return fallback_name or key

def render_dealstage(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    key = str(raw_value).strip()
    if not key:
        return ""
    # exact match first
    val = _DEALSTAGE_MAP.get(key)
    if val is not None:
        return val
    # try lowercase key as a fallback
    val = _DEALSTAGE_MAP.get(key.lower())
    if val is not None:
        return val
    return key

MSK_TZ = ZoneInfo("Europe/Moscow")

def format_date_yyyy_mm_dd(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        # numeric timestamp (ms or s)
        if s.isdigit():
            ts = int(s)
            if ts > 10**12:
                dt = datetime.utcfromtimestamp(ts / 1000)
            else:
                dt = datetime.utcfromtimestamp(ts)
            return dt.date().isoformat()
        # ISO-like string
        if len(s) >= 10 and s[4] == '-' and s[7] == '-':
            return s[:10]
        if 'T' in s and len(s.split('T')[0]) == 10:
            return s.split('T')[0]
    except Exception:
        pass
    return s

def add_business_hours_msk(start_dt_utc: datetime, hours: float) -> datetime:
    """Return UTC datetime when given number of business-hours elapse.
    Business hours: 09:00-19:00 MSK, Mon-Fri.
    """
    remaining = timedelta(hours=hours)
    current_msk = start_dt_utc.astimezone(MSK_TZ)
    while remaining.total_seconds() > 0:
        if current_msk.weekday() >= 5:  # Sat/Sun
            days_ahead = 7 - current_msk.weekday()
            next_morning = (current_msk + timedelta(days=days_ahead)).replace(hour=9, minute=0, second=0, microsecond=0)
            current_msk = next_morning
            continue
        day_start = current_msk.replace(hour=9, minute=0, second=0, microsecond=0)
        day_end = current_msk.replace(hour=19, minute=0, second=0, microsecond=0)
        if current_msk < day_start:
            current_msk = day_start
            continue
        if current_msk >= day_end:
            next_day = current_msk + timedelta(days=1)
            current_msk = next_day.replace(hour=9, minute=0, second=0, microsecond=0)
            continue
        available = day_end - current_msk
        if available >= remaining:
            current_msk = current_msk + remaining
            remaining = timedelta(0)
            break
        else:
            current_msk = day_end
            remaining -= available
    return current_msk.astimezone(ZoneInfo("UTC"))

async def schedule_owner_reminder(deal_id: str, owner_id: Any, portal_id: Optional[str]):
    try:
        now_utc = datetime.now(tz=ZoneInfo("UTC"))
        if REMINDER_TEST_MINUTES > 0:
            delay = REMINDER_TEST_MINUTES * 60
            logger.info("Scheduling test reminder in %s seconds for deal %s", delay, deal_id)
        else:
            trigger_utc = add_business_hours_msk(now_utc, 8.0)
            delay = max(0, (trigger_utc - now_utc).total_seconds())
            logger.info("Scheduling business-hours reminder in %s seconds (trigger %s UTC) for deal %s", delay, trigger_utc.isoformat(), deal_id)
        await asyncio.sleep(delay)
        # Re-check deal state at reminder time; skip if main practice is already set
        try:
            deal_at_reminder = hs_get_deal(deal_id)
            props_at_reminder = deal_at_reminder.get("properties", {})
            mp_value = props_at_reminder.get(MAIN_PRACTICE_PROP)
            is_set = False
            if mp_value is None:
                is_set = False
            elif isinstance(mp_value, str):
                is_set = bool(mp_value.strip())
            else:
                # Non-string values treated as set if truthy
                is_set = bool(mp_value)
            if is_set:
                logger.info("Skipping reminder for deal %s because %s is already set", deal_id, MAIN_PRACTICE_PROP)
                return
        except Exception:
            logger.exception("Failed to re-fetch deal %s at reminder time; proceeding with best effort", deal_id)
        owner_name = render_owner_name(owner_id)
        mention = render_owner_mention(owner_id, owner_name)
        pid = "24115553"
        if pid:
            deal_link = f"https://app.hubspot.com/contacts/{pid}/record/0-3/{deal_id}"
        else:
            deal_link = f"deal id: {deal_id}"
        text = f"{mention} напоминаю, что необходимо определить основной пул по сделке\n{deal_link}"
        await application.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        logger.exception("Failed to send owner reminder for deal %s", deal_id)

def hs_get_deal(deal_id: str) -> Dict[str, Any]:
    url = f"{HS_BASE}/crm/v3/objects/deals/{deal_id}"
    params = {
        "properties": [
            "dealname",
            "dealstage",
            "amount",
            DEAL_OWNER_PROP,
            DEAL_LOCATION_PROP,
            DISTRIBUTION_FLAG_PROP,
            MAIN_PRACTICE_PROP,
            "source_of_deal",
            "description",
            "closedate",
            "duration",
            "onsight_remote",
            "financial_terms",
            "hs_next_step",
            "to_notify",
            "documents_for_deal",
            "description_of_deal",
        ],
        "archived": "false",
        "associations": "companies",
    }
    r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
    if not r.ok:
        raise HTTPException(status_code=502, detail="HubSpot get deal failed")
    return r.json()

def hs_get_company(company_id: str) -> Dict[str, Any]:
    url = f"{HS_BASE}/crm/v3/objects/companies/{company_id}"
    params = {
        "properties": [COMPANY_NAME_PROP],
        "archived": "false",
    }
    r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
    if not r.ok:
        # Try with archived true as a fallback
        params["archived"] = "true"
        r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
        if not r.ok:
            logger.error("Company fetch failed for %s: %s %s", company_id, r.status_code, r.text)
            raise HTTPException(status_code=502, detail="HubSpot get company failed")
    return r.json()

def extract_primary_company_id_from_deal(deal: Dict[str, Any]) -> Optional[str]:
    associations = deal.get("associations") or {}
    companies = (associations.get("companies") or {}).get("results") or []
    if not companies:
        return None
    # HubSpot marks primary in associationTypeId or via 'primary' flag in some webhook shapes; be defensive
    for assoc in companies:
        if assoc.get("primary") is True:
            cid = str(assoc.get("id") or "").strip()
            if cid:
                return cid
        assoc_type = str(assoc.get("type") or "").lower()
        if "primary" in assoc_type:
            cid = str(assoc.get("id") or "").strip()
            if cid:
                return cid
    # Fallback to first
    first = companies[0]
    cid = str(first.get("id") or "").strip()
    return cid or None

def hs_get_primary_company_id_via_api(deal_id: str) -> Optional[str]:
    # Use associations API v4 to fetch primary association
    url = f"{HS_BASE}/crm/v4/objects/deals/{deal_id}/associations/companies"
    params: Dict[str, Any] = {"limit": 100}
    r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
    if not r.ok:
        logger.warning("Associations v4 fetch failed: %s %s", r.status_code, r.text)
        return None
    data = r.json() or {}
    results = data.get("results") or []
    # v4 returns an array with associationTypeId and toObjectId
    primary_id = None
    for item in results:
        if item.get("associationSpec", {}).get("primary", False):
            primary_id = str(item.get("toObjectId") or "").strip()
            if primary_id:
                return primary_id
    # Fallback to first
    if results:
        primary_id = str(results[0].get("toObjectId") or "").strip()
    return primary_id or None

def hs_update_deal(deal_id: str, properties: Dict[str, Any]):
    url = f"{HS_BASE}/crm/v3/objects/deals/{deal_id}"
    payload = {"properties": properties}
    r = requests.patch(url, headers=HS_HEADERS, json=payload, timeout=15)
    if not r.ok:
        raise HTTPException(status_code=502, detail="HubSpot update failed")
    return r.json()

application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

async def assign_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /assign <deal_id> key=value ...")
        return
    deal_id = context.args[0]
    props = {}
    for arg in context.args[1:]:
        if "=" in arg:
            k, v = arg.split("=", 1)
            props[k] = v
    try:
        hs_update_deal(deal_id, props)
        await update.message.reply_text(f"✅ Updated deal {deal_id} with {props}")
    except HTTPException:
        await update.message.reply_text("Failed to update HubSpot")

application.add_handler(CommandHandler("assign", assign_cmd))

app = FastAPI()
async def posttest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await application.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="🔧 Test post to TELEGRAM_CHAT_ID"
        )
        await update.message.reply_text("✅ Sent to channel")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")

application.add_handler(CommandHandler("posttest", posttest_cmd))

application.add_handler(CallbackQueryHandler(interest_callback))

class HubSpotEvent(BaseModel):
    objectId: str
    objectType: Optional[str] = None

@app.on_event("startup")
async def on_startup():
    await application.bot.set_webhook(url=f"{TELEGRAM_WEBHOOK_URL}")
    await application.initialize()
    await application.start()

@app.on_event("shutdown")
async def on_shutdown():
    await application.stop()
    await application.shutdown()

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return JSONResponse({"ok": True})

@app.post("/hubspot/webhook")
async def hubspot_webhook(request: Request):
    body = await request.json()
    logger.info("Webhook body: %s", body)  # временно — чтобы видеть реальный формат

    # Приводим тело к единому списку событий
    events = []
    if isinstance(body, list):
        events = body  # App Webhooks
    elif isinstance(body, dict):
        # Несколько распространённых вариантов
        if "objectId" in body:
            events = [body]
        elif "event" in body and isinstance(body["event"], dict) and "objectId" in body["event"]:
            events = [body["event"]]
        elif "id" in body and str(body.get("objectType", "")).lower() in ("deal", "deals"):
            events = [{"objectId": body["id"], "objectType": "deal"}]
        else:
            logger.warning("Unknown webhook payload shape; skipping")
            return JSONResponse({"ok": True})
    else:
        logger.warning("Unexpected payload type; skipping")
        return JSONResponse({"ok": True})

    # Обрабатываем события
    for ev in events:
        deal_id = str(ev.get("objectId") or ev.get("id") or "").strip()
        if not deal_id:
            logger.warning("No deal_id in event: %s", ev)
            continue
        try:
            deal = hs_get_deal(deal_id)
            properties = deal.get("properties", {})

            # Gate posting by distribution flag (must be true)
            flag_value = properties.get(DISTRIBUTION_FLAG_PROP)
            should_post = False
            if isinstance(flag_value, bool):
                should_post = flag_value is True
            elif isinstance(flag_value, str):
                should_post = flag_value.strip().lower() == "true"
            if not should_post:
                logger.info("Distribution flag is false for deal %s (value=%r); skipping post", deal_id, flag_value)
                continue

            # Post only if main practice is NOT set at this moment
            mp_value = properties.get(MAIN_PRACTICE_PROP)
            mp_is_set = False
            if mp_value is None:
                mp_is_set = False
            elif isinstance(mp_value, str):
                mp_is_set = bool(mp_value.strip())
            else:
                mp_is_set = bool(mp_value)
            if mp_is_set:
                logger.info("Main practice is already set for deal %s; skipping initial post", deal_id)
                continue

            # Deduplicate: ensure we post only once per deal
            async with _POST_LOCK:
                if deal_id in _POSTED_DEALS:
                    logger.info("Already posted initial message for deal %s; skipping duplicate", deal_id)
                    continue
                _POSTED_DEALS.add(deal_id)
            title = properties.get("dealname", "(no title)")

            # Try to include primary company name
            company_name = None
            try:
                primary_company_id = extract_primary_company_id_from_deal(deal)
                if not primary_company_id:
                    primary_company_id = hs_get_primary_company_id_via_api(deal_id)
                if primary_company_id:
                    company = hs_get_company(primary_company_id)
                    company_name = (company.get("properties") or {}).get(COMPANY_NAME_PROP)
                    if isinstance(company_name, str) and company_name.strip() == "":
                        company_name = None
            except Exception:
                logger.exception("Failed to fetch primary company for deal %s", deal_id)

            lines = [
                f"📌 Название сделки: {title}",
                f"ID: {deal_id}",
            ]
            if company_name:
                lines.append(f"Компания: {company_name}")

            # label -> internal property name (russian labels)
            fields_to_render = [
                ("Стадия сделки", "dealstage"),
                ("Сумма сделки", "amount"),
                ("Владелец сделки", DEAL_OWNER_PROP),
                ("Локация", DEAL_LOCATION_PROP),
                ("Источник сделки", "source_of_deal"),
                ("Описание сделки", "description"),
                ("Дата старта", "closedate"),
                ("Продолжительность проекта", "duration"),
                ("Формат проекта", "onsight_remote"),
                ("Финансовые условия", "financial_terms"),
                ("Следующие шаги", "hs_next_step"),
                ("Оповестить", "to_notify"),
                ("Комментарии", "description_of_deal"),
            ]

            for label, prop_key in fields_to_render:
                value = properties.get(prop_key)
                if value is None:
                    continue
                if isinstance(value, str) and value.strip() == "":
                    continue
                if prop_key == DEAL_OWNER_PROP:
                    display_value = render_owner_name(value)
                elif prop_key == "to_notify":
                    display_value = render_mentions_from_surnames(value)
                elif prop_key == "closedate":
                    display_value = format_date_yyyy_mm_dd(value)
                elif prop_key == "dealstage":
                    display_value = render_dealstage(value)
                else:
                    display_value = value
                lines.append(f"{label}: {display_value}")

            text = "\n".join(lines)

            keyboard = build_interest_keyboard(deal_id, len(_INTEREST_USERS.get(deal_id, set())))

            message = await application.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )

            # Append to Google Sheet in the same order as the posted fields
            try:
                row_values: list[Any] = []
                # Название сделки, ID, Компания
                row_values.append(title)
                row_values.append(deal_id)
                row_values.append(company_name or "")
                # Стадия сделки -> ... -> Комментарии
                order_props = [
                    "dealstage",
                    "amount",
                    DEAL_OWNER_PROP,
                    DEAL_LOCATION_PROP,
                    "source_of_deal",
                    "description",
                    "closedate",
                    "duration",
                    "onsight_remote",
                    "financial_terms",
                    "hs_next_step",
                    "to_notify",
                    "description_of_deal",
                ]
                for prop_key in order_props:
                    val = properties.get(prop_key)
                    if prop_key == DEAL_OWNER_PROP:
                        val = render_owner_name(val) if val is not None else ""
                    elif prop_key == "to_notify":
                        val = render_mentions_from_surnames(val) if val is not None else ""
                    elif prop_key == "closedate":
                        val = format_date_yyyy_mm_dd(val) if val is not None else ""
                    elif prop_key == "dealstage":
                        val = render_dealstage(val) if val is not None else ""
                    else:
                        val = "" if (val is None or (isinstance(val, str) and not val.strip())) else val
                    row_values.append(val)
                post_dt = datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M")
                row_values.append(post_dt)
                append_deal_row_to_sheet(row_values)
            except Exception:
                logger.exception("Failed to append deal to Google Sheet")

            # Schedule reminder for the owner in 8 business hours (MSK)
            try:
                owner_id_value = properties.get(DEAL_OWNER_PROP)
                portal_id = str(deal.get("portalId") or "").strip() or None
                if owner_id_value:
                    asyncio.create_task(schedule_owner_reminder(deal_id, owner_id_value, portal_id))
            except Exception:
                logger.exception("Failed to schedule owner reminder for deal %s", deal_id)

            # Documents are not required anymore; skipping any file forwarding
        except Exception:
            logger.exception("Failed to fetch/post deal %s", deal_id)

    return JSONResponse({"ok": True})



@app.get("/")
async def root():
    return {"status": "ok"}
@app.head("/")
async def root_head():
    return JSONResponse({"status": "ok"})

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
