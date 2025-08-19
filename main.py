import os
import logging
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

HS_BASE = "https://api.hubapi.com"
HS_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

def hs_get_deal(deal_id: str) -> Dict[str, Any]:
    url = f"{HS_BASE}/crm/v3/objects/deals/{deal_id}"
    params = {
        "properties": [
            "dealname",
            "dealstage",
            "amount",
            DEAL_OWNER_PROP,
            DEAL_LOCATION_PROP,
        ],
        "archived": "false",
    }
    r = requests.get(url, headers=HS_HEADERS, params=params, timeout=15)
    if not r.ok:
        raise HTTPException(status_code=502, detail="HubSpot get deal failed")
    return r.json()

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
            title = properties.get("dealname", "(no title)")

            lines = [
                f"📌 New deal: {title}",
                f"ID: {deal_id}",
            ]

            # label -> internal property name
            fields_to_render = [
                ("dealstage", "dealstage"),
                ("amount", "amount"),
                ("hubspot_owner_id", DEAL_OWNER_PROP),
                ("location", DEAL_LOCATION_PROP),
            ]

            for label, prop_key in fields_to_render:
                value = properties.get(prop_key)
                if value is None:
                    continue
                if isinstance(value, str) and value.strip() == "":
                    continue
                lines.append(f"{label}: {value}")

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
