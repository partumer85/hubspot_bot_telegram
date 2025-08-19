import os
import logging
import time
import json
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

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

MSK_TZ = ZoneInfo("Europe/Moscow")

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

def hs_get_deal_file_association_ids_v3(deal_id: str) -> list[str]:
    url = f"{HS_BASE}/crm/v3/objects/deals/{deal_id}/associations/files"
    params: Dict[str, Any] = {"limit": 100}
    file_ids: list[str] = []
    try:
        while True:
            r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
            if not r.ok:
                logger.warning("Deal->files association fetch failed: %s %s", r.status_code, r.text)
                break
            data = r.json() or {}
            for assoc in data.get("results", []) or []:
                fid = str(assoc.get("toObjectId") or assoc.get("id") or "").strip()
                if fid:
                    file_ids.append(fid)
            paging = (data.get("paging") or {}).get("next") or {}
            after = paging.get("after")
            if after:
                params["after"] = after
            else:
                break
    except Exception:
        logger.exception("Failed to fetch file associations for deal %s", deal_id)
    return file_ids

def hs_get_file(file_id: str) -> Dict[str, Any]:
    # Files API lives under /files/v3
    url = f"{HS_BASE}/files/v3/files/{file_id}"
    r = requests.get(url, headers=HS_HEADERS, timeout=15)
    if not r.ok:
        logger.warning("File fetch failed for %s: %s %s", file_id, r.status_code, r.text)
        raise HTTPException(status_code=502, detail="HubSpot get file failed")
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
                # documents_for_deal - не показываем в основном сообщении
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
                else:
                    display_value = value
                lines.append(f"{label}: {display_value}")

            text = "\n".join(lines)

            message = await application.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

            # Schedule reminder for the owner in 8 business hours (MSK)
            try:
                owner_id_value = properties.get(DEAL_OWNER_PROP)
                portal_id = str(deal.get("portalId") or "").strip() or None
                if owner_id_value:
                    asyncio.create_task(schedule_owner_reminder(deal_id, owner_id_value, portal_id))
            except Exception:
                logger.exception("Failed to schedule owner reminder for deal %s", deal_id)

            # If there are any files associated with the deal, forward them afterwards
            try:
                file_ids = hs_get_deal_file_association_ids_v3(deal_id)
                for fid in file_ids:
                    try:
                        file_meta = hs_get_file(fid)
                        file_url = file_meta.get("url") or file_meta.get("urlFull")
                        if not file_url:
                            continue
                        # Send as a simple link (uploading requires public URL/stream)
                        await application.bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=f"Документ: {file_url}",
                            reply_to_message_id=message.message_id,
                        )
                    except Exception:
                        logger.exception("Failed to post file %s for deal %s", fid, deal_id)
            except Exception:
                logger.exception("Failed to fetch files for deal %s", deal_id)
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
