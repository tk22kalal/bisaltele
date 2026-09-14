import asyncio
from dotenv import load_dotenv
load_dotenv("/app/.env")

from biisal.vars import Var
from pyrogram import Client
from pyrogram.types import ChatPrivileges
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserNotParticipant, RPCError
import aiohttp

USER_BOT_TOKEN = "8993867611:AAGoukWMXUUH5t8LvpqcZHxWmftIHjbwxdY"
BIN_MSG_ID = 1620415  # from the given final URL path /1620415/...

FULL = ChatPrivileges(
    can_manage_chat=True, can_delete_messages=True, can_manage_video_chats=True,
    can_restrict_members=True, can_promote_members=True, can_change_info=True,
    can_post_messages=True, can_edit_messages=True, can_invite_users=True,
    can_pin_messages=True, is_anonymous=False)


async def bot_api(token, method, params):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{token}/{method}", json=params) as r:
            return await r.json()


async def main():
    # 1) does BIN message 1620415 still exist?
    bot = Client("t_bot", api_id=Var.API_ID, api_hash=Var.API_HASH,
                 bot_token=Var.BOT_TOKEN, no_updates=True, in_memory=True)
    await bot.start()
    msg = await bot.get_messages(Var.BIN_CHANNEL, BIN_MSG_ID)
    print("STEP1 BIN msg", BIN_MSG_ID, "exists=", bool(msg and not getattr(msg, 'empty', False)),
          "media=", (msg.media if msg else None))
    await bot.stop()

    # 2) bot identity + owner chat
    me = await bot_api(USER_BOT_TOKEN, "getMe", {})
    username = me.get("result", {}).get("username")
    print("STEP2 user bot =", username, me.get("ok"))
    ups = await bot_api(USER_BOT_TOKEN, "getUpdates", {"limit": 100, "timeout": 0})
    owner = None
    for u in reversed(ups.get("result", [])):
        chat = (u.get("message") or u.get("my_chat_member") or {}).get("chat") or {}
        if chat.get("type") == "private" and chat.get("id"):
            owner = chat["id"]; break
    print("STEP2 owner chat =", owner)

    # 3) promote via user session (retry)
    us = Client("t_user", api_id=Var.API_ID, api_hash=Var.API_HASH,
                session_string=Var.USER_SESSION_STRING, no_updates=True, in_memory=True)
    await us.start()
    peer = f"@{username}" if username else int(USER_BOT_TOKEN.split(":")[0])
    status = None
    try:
        m = await us.get_chat_member(Var.BIN_CHANNEL, peer)
        status = m.status
        print("STEP3 current status in BIN:", status)
    except UserNotParticipant:
        print("STEP3 not a participant yet")
    if status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        try:
            await us.promote_chat_member(Var.BIN_CHANNEL, peer, privileges=FULL)
            print("STEP3 PROMOTE OK (bot added as full admin)")
        except UserNotParticipant:
            try:
                await us.add_chat_members(Var.BIN_CHANNEL, peer)
                await us.promote_chat_member(Var.BIN_CHANNEL, peer, privileges=FULL)
                print("STEP3 ADD+PROMOTE OK")
            except RPCError as e:
                print("STEP3 ADD+PROMOTE FAILED:", type(e).__name__, str(e)[:120])
        except RPCError as e:
            print("STEP3 PROMOTE FAILED:", type(e).__name__, str(e)[:120])
    else:
        print("STEP3 already admin -> no promotion needed")
    await us.stop()

    # 4) deliver with protect content
    if owner:
        res = await bot_api(USER_BOT_TOKEN, "copyMessage", {
            "chat_id": owner,
            "from_chat_id": Var.BIN_CHANNEL,
            "message_id": BIN_MSG_ID,
            "protect_content": True,
        })
        print("STEP4 DELIVER ok=", res.get("ok"), "desc=", res.get("description"),
              "msg_id=", (res.get("result") or {}).get("message_id"))
    else:
        print("STEP4 skipped: no owner chat (press /start on the user bot)")

asyncio.run(main())
