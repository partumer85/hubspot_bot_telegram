import os
import logging
import time
from typing import Optional, Dict, Any

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

# Cache for HubSpot owners: owner_id -> "First Last" (fallbacks to email/id)
_OWNERS_MAP_CACHE: Dict[str, str] = {}
_OWNERS_MAP_TS: float = 0.0

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
                f"📌 New deal: {title}",
                f"ID: {deal_id}",
            ]
            if company_name:
                lines.append(f"company: {company_name}")

            # label -> internal property name
            fields_to_render = [
                ("dealstage", "dealstage"),
                ("amount", "amount"),
                ("hubspot_owner_id", DEAL_OWNER_PROP),
                ("location", DEAL_LOCATION_PROP),
                ("source_of_deal", "source_of_deal"),
                ("description", "description"),
                ("closedate", "closedate"),
                ("duration", "duration"),
                ("onsight_remote", "onsight_remote"),
                ("financial_terms", "financial_terms"),
                ("hs_next_step", "hs_next_step"),
                ("to_notify", "to_notify"),
                ("documents_for_deal", "documents_for_deal"),
                ("description_of_deal", "description_of_deal"),
            ]

            for label, prop_key in fields_to_render:
                value = properties.get(prop_key)
                if value is None:
                    continue
                if isinstance(value, str) and value.strip() == "":
                    continue
                if prop_key == DEAL_OWNER_PROP:
                    display_value = render_owner_name(value)
                else:
                    display_value = value
                lines.append(f"{label}: {display_value}")

            text = "\n".join(lines)

            await application.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
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
