import asyncio, os, time
from dotenv import load_dotenv
load_dotenv("/app/.env")
from pyrogram import Client
import motor.motor_asyncio
from biisal.vars import Var
from biisal.utils import telegram_delivery as td

API_ID=int(os.getenv("API_ID")); API_HASH=os.getenv("API_HASH")
SESSION=os.getenv("USER_SESSION_STRING"); BIN=int(os.getenv("BIN_CHANNEL"))
TEST_BOT_TOKEN=os.getenv("MULTI_TOKEN2")   # streaammmmvps_bot, has a DM with session acct
OWNER=6787924476

async def seed_peer():
    """One-time: fetch the bot's real access_hash WITHOUT ResolveUsername
    (via get_dialogs) and persist it, exactly as the cold path would."""
    c=Client("seed",api_id=API_ID,api_hash=API_HASH,session_string=SESSION,no_updates=True,in_memory=True)
    await c.start()
    bot_id,uname = await td.get_bot_identity(TEST_BOT_TOKEN)
    ah=None
    async for d in c.get_dialogs(limit=300):
        if d.chat.id==bot_id:
            p=await c.resolve_peer(bot_id); ah=p.access_hash; break
    await c.stop()
    print("seed: bot",bot_id,uname,"access_hash",ah)
    if ah is None:
        print("!! no dialog with test bot; cannot seed"); return None
    mc=motor.motor_asyncio.AsyncIOMotorClient(Var.DATABASE_URL)
    await mc[Var.name].bot_peer_cache.update_one(
        {"_id":bot_id},{"$set":{"access_hash":str(ah),"username":uname,"updated_at":time.time()}},upsert=True)
    print("seeded mongo peer cache");  return bot_id

async def main():
    bot_id=await seed_peer()
    if not bot_id: return
    # pick recent BIN media
    c=await td._get_user_client()
    msg_id=None
    async for m in c.get_chat_history(BIN,limit=5):
        if m.video or m.document: msg_id=m.id; break
    print("relay BIN msg",msg_id)
    for i in (1,2):
        t0=time.monotonic()
        ok,res=await td.deliver_via_dm_relay(bot_token=TEST_BOT_TOKEN,chat_id=OWNER,
            from_chat_id=BIN,message_id=msg_id,caption=f"warm-path test {i}")
        print(f"DELIVERY {i}: ok={ok} res={res!r} elapsed={time.monotonic()-t0:.1f}s")

asyncio.run(main())
