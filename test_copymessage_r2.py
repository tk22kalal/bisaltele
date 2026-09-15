"""Round 2: WHY did supergroup member copy fail, and what fixes it?

  T5  privacy-mode hypothesis: bot copies a message that MENTIONS it (visible
      to privacy-restricted bots) -> should PASS if visibility is the gate
  T6  bot-promotes-bot: main bot (SG admin w/ can_promote) promotes user bot
      via pure Bot API (no user session) -> copyMessage should PASS
  T7  self-join: user bot joins SG via invite link using its own MTProto bot
      client (no user session) -> membership check
"""
import asyncio
import json
import aiohttp
from pyrogram import Client
from pyrogram.errors import RPCError, FloodWait

API_ID = 24316517
API_HASH = "ab33479d43c662f11cf9ae4b26350709"
SESSION = "BQFzCmUAtVFj0sTpMdbRm95HKU90B1pynTfw9YJyfIl1tjt4q-opjhxHDgxB3u2vuuRGLMmng6AcaAMmrxnLC2Z1e-Fm8Eh3462vznKReDtcRqk3l5EeIizWZrD3IDrtXd3DEtoJz-ljOvCkij7Cqz6f367MFlhUtcdE31-TRwPBNbObPb0YnpRygK_7GtAAi9JC1xEl8s4qGVccZL17D-FnfQPwaT4rk7nAC3gEwXBbc7OZx8w53-oVqNXz2N1IPevan3MYDdKHPB2kV3SmlQ3G4qAJdiIVSnfwCG8BIlvAIwY5U2YsiBA6hTfkoG2dN_Vyr3yeldUAKhUp8F2x8MJC_YuHAAAAAAGUl4H8AA"
MAIN_BOT = "8446242353:AAG0EuaKSYudx1ODqYUUZnaPDoPt2-lnnfc"
TEST_BOT = "8932279745:AAGjQN7X2CQXOYtqE6vIYpAkUKMFarwXlnw"

API = "https://api.telegram.org"
RESULTS = {}


async def fw(factory, what="call"):
    while True:
        try:
            return await factory()
        except FloodWait as e:
            print(f"  floodwait on {what}: sleeping {e.value + 5}s", flush=True)
            await asyncio.sleep(e.value + 5)


async def bot_api(token, method, **params):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{API}/bot{token}/{method}", json=params) as r:
            return await r.json()


def record(key, ok, detail):
    RESULTS[key] = {"ok": ok, "detail": str(detail)[:400]}
    print(f"[{'PASS' if ok else 'FAIL'}] {key}: {str(detail)[:300]}", flush=True)


async def main():
    user = Client("copytester2", api_id=API_ID, api_hash=API_HASH,
                  session_string=SESSION, in_memory=True, no_updates=True)
    await user.start()
    tb = (await bot_api(TEST_BOT, "getMe"))["result"]
    mb = (await bot_api(MAIN_BOT, "getMe"))["result"]
    tb_peer = await fw(lambda: user.resolve_peer(tb["username"]), "resolve test bot")
    mb_peer = await fw(lambda: user.resolve_peer(mb["username"]), "resolve main bot")
    print(f"test bot {tb['id']} @{tb['username']} | main bot {mb['id']} @{mb['username']}", flush=True)

    # ---- T5: mention-visibility hypothesis ----------------------------------
    sg5 = await fw(lambda: user.create_supergroup("T5 mention test", "tmp"), "create sg5")
    await fw(lambda: user.add_chat_members(sg5.id, tb_peer.user_id), "add tb sg5")
    plain = await user.send_message(sg5.id, "plain message, no mention")
    ment = await user.send_message(sg5.id, f"hey @{tb['username']} look at this")
    cp_plain = await bot_api(TEST_BOT, "copyMessage", chat_id=sg5.id,
                             from_chat_id=sg5.id, message_id=plain.id)
    record("T5a copy PLAIN msg (privacy bot, member)", cp_plain.get("ok"),
           cp_plain.get("description") or "copied")
    cp_ment = await bot_api(TEST_BOT, "copyMessage", chat_id=sg5.id,
                            from_chat_id=sg5.id, message_id=ment.id)
    record("T5b copy MENTIONED msg (privacy bot, member)", cp_ment.get("ok"),
           cp_ment.get("description") or "copied -> visibility gates copyMessage")

    # ---- T6: bot promotes bot via pure Bot API -------------------------------
    sg6 = await fw(lambda: user.create_supergroup("T6 bot-promote test", "tmp"), "create sg6")
    await fw(lambda: user.add_chat_members(sg6.id, mb_peer.user_id), "add mb sg6")
    await fw(lambda: user.add_chat_members(sg6.id, tb_peer.user_id), "add tb sg6")
    from pyrogram.types import ChatPrivileges
    await fw(lambda: user.promote_chat_member(
        sg6.id, mb_peer.user_id,
        privileges=ChatPrivileges(can_promote_members=True, can_invite_users=True)), "promote main bot")
    msg6 = await user.send_message(sg6.id, "T6 message to copy after promotion")
    pr = await bot_api(MAIN_BOT, "promoteChatMember", chat_id=sg6.id, user_id=tb["id"],
                       can_manage_chat=False, can_delete_messages=False,
                       can_manage_video_chats=False, can_restrict_members=False,
                       can_promote_members=False, can_change_info=False,
                       can_post_stories=False, can_edit_stories=False,
                       can_delete_stories=False)
    record("T6a main bot promotes user bot via Bot API", pr.get("ok"),
           pr.get("description") or "promoted (no user session involved)")
    if pr.get("ok"):
        await asyncio.sleep(2)
        cp6 = await bot_api(TEST_BOT, "copyMessage", chat_id=sg6.id,
                            from_chat_id=sg6.id, message_id=msg6.id,
                            protect_content=True)
        record("T6b user bot copyMessage after Bot API promotion",
               cp6.get("ok"), cp6.get("description") or "copied with protect_content")
        dm = await bot_api(MAIN_BOT, "promoteChatMember", chat_id=sg6.id, user_id=tb["id"],
                           can_manage_chat=False, is_anonymous=False)
        record("T6c main bot DEMOTES user bot via Bot API", dm.get("ok"),
               dm.get("description") or "demoted")

    # ---- T7: bot self-joins a group via invite link --------------------------
    sg7 = await fw(lambda: user.create_supergroup("T7 self-join test", "tmp"), "create sg7")
    invite = await fw(lambda: user.create_invite_link(sg7.id), "invite link")
    bot_client = Client("tb_selfjoin", api_id=API_ID, api_hash=API_HASH,
                        bot_token=TEST_BOT, in_memory=True, no_updates=True)
    await bot_client.start()
    try:
        joined = await fw(lambda: bot_client.join_chat(invite.invite_link), "bot join_chat")
        record("T7a bot self-joins supergroup via invite link", True,
               f"joined chat id={joined.id}")
        gm = await bot_api(TEST_BOT, "getChat", chat_id=sg7.id)
        record("T7b bot sees supergroup after self-join", gm.get("ok"),
               gm.get("description") or "ok")
    except RPCError as e:
        record("T7a bot self-joins supergroup via invite link", False, str(e))
    await bot_client.stop()

    # cleanup
    for g in (sg5.id, sg6.id, sg7.id):
        try:
            await user.delete_supergroup(g)
        except RPCError:
            pass
    await user.stop()
    with open("/app/test_reports/copymessage_findings_r2.json", "w") as f:
        json.dump(RESULTS, f, indent=2)
    print("done -> /app/test_reports/copymessage_findings_r2.json", flush=True)


asyncio.run(main())
