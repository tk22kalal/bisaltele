import asyncio, os, time
from dotenv import load_dotenv
load_dotenv("/app/.env")
from biisal.utils import telegram_delivery as td

BIN=int(os.getenv("BIN_CHANNEL")); TOKEN=os.getenv("MULTI_TOKEN2"); OWNER=6787924476

async def main():
    print("session strings found:", len(td._collect_session_strings()))
    sessions = td._get_sessions()
    print("pool size:", len(sessions), "indexes:", [s.index for s in sessions])

    # need a started session to read BIN history for a msg id
    s = await td._acquire_session(set())
    print("acquired session #", s.index, "account", s.account_id)
    td._release_session(s)
    msg_id=None
    async for m in s.client.get_chat_history(BIN, limit=5):
        if m.video or m.document: msg_id=m.id; break
    print("relay BIN msg", msg_id)

    for i in (1,2):
        t0=time.monotonic()
        ok,res=await td.deliver_via_dm_relay(bot_token=TOKEN, chat_id=OWNER,
            from_chat_id=BIN, message_id=msg_id, caption=f"pool test {i}")
        print(f"DELIVERY {i}: ok={ok} res={res!r} elapsed={time.monotonic()-t0:.1f}s")

asyncio.run(main())
