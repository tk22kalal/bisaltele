"""Round 3: pin down the copyMessage access rule + test the DM-relay idea.

  T8  supergroup: does RECEIVING a message via getUpdates unlock copyMessage?
      (plain msg, mention msg, self-sent msg, msg while admin)
  T9  DM-relay: user session copies BIN_CHANNEL video into the user bot's DM;
      can the user bot then copyMessage it to the destination? (NO channel
      admin needed at all)
  T7  retry: bot self-joins a supergroup via invite link (pyrofork bot client)
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
BIN_CHANNEL = -1002133410746
USER_ACCOUNT_ID = 6787924476

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


async def get_updates_seen(token):
    d = await bot_api(token, "getUpdates", limit=100, timeout=0)
    seen = []
    for u in d.get("result", []):
        m = u.get("message") or u.get("channel_post") or {}
        if m.get("message_id"):
            seen.append((m.get("message_id"), (m.get("chat") or {}).get("id"),
                         (m.get("text") or m.get("caption") or "<media>")[:40]))
    return seen


async def main():
    user = Client("copytester3", api_id=API_ID, api_hash=API_HASH,
                  session_string=SESSION, in_memory=True, no_updates=True)
    await user.start()
    tb = (await bot_api(TEST_BOT, "getMe"))["result"]
    tb_peer = await fw(lambda: user.resolve_peer(tb["username"]), "resolve tb")

    # drain old updates
    d = await bot_api(TEST_BOT, "getUpdates", limit=100, timeout=0)
    if d.get("result"):
        await bot_api(TEST_BOT, "getUpdates", offset=d["result"][-1]["update_id"] + 1, timeout=0)

    # ---- T8: updates unlock copyMessage? ------------------------------------
    sg8 = await fw(lambda: user.create_supergroup("T8 updates test", "tmp"), "create sg8")
    await fw(lambda: user.add_chat_members(sg8.id, tb_peer.user_id), "add tb sg8")
    m_plain = await user.send_message(sg8.id, "plain msg no mention")
    m_ment = await user.send_message(sg8.id, f"hi @{tb['username']} ping")
    await asyncio.sleep(3)
    seen1 = await get_updates_seen(TEST_BOT)
    record("T8a updates received by privacy bot", True, f"seen={seen1}")
    got_plain = any(mid == m_plain.id for mid, _, _ in seen1)
    got_ment = any(mid == m_ment.id for mid, _, _ in seen1)
    record("T8a1 plain msg in updates", got_plain, f"plain_id={m_plain.id}")
    record("T8a2 mention msg in updates", got_ment, f"mention_id={m_ment.id}")

    cp_plain = await bot_api(TEST_BOT, "copyMessage", chat_id=sg8.id,
                             from_chat_id=sg8.id, message_id=m_plain.id)
    record("T8b copy PLAIN msg after getUpdates", cp_plain.get("ok"),
           cp_plain.get("description") or "copied")
    cp_ment = await bot_api(TEST_BOT, "copyMessage", chat_id=sg8.id,
                            from_chat_id=sg8.id, message_id=m_ment.id)
    record("T8c copy MENTIONED msg after getUpdates", cp_ment.get("ok"),
           cp_ment.get("description") or "copied -> updates unlock copyMessage")

    m_self = await bot_api(TEST_BOT, "sendMessage", chat_id=sg8.id, text="bot's own msg")
    self_id = (m_self.get("result") or {}).get("message_id")
    if self_id:
        cp_self = await bot_api(TEST_BOT, "copyMessage", chat_id=sg8.id,
                                from_chat_id=sg8.id, message_id=self_id)
        record("T8d copy bot's OWN msg", cp_self.get("ok"),
               cp_self.get("description") or "copied")

    from pyrogram.types import ChatPrivileges
    await fw(lambda: user.promote_chat_member(
        sg8.id, tb_peer.user_id,
        privileges=ChatPrivileges(can_manage_chat=True, can_delete_messages=True,
                                  can_invite_users=True)), "promote tb sg8")
    m_admin = await user.send_message(sg8.id, "msg sent while bot is admin")
    await asyncio.sleep(3)
    seen2 = await get_updates_seen(TEST_BOT)
    got_admin = any(mid == m_admin.id for mid, _, _ in seen2)
    record("T8e msg received while admin (privacy still ON)", got_admin,
           f"admin_msg_id={m_admin.id}")
    cp_admin = await bot_api(TEST_BOT, "copyMessage", chat_id=sg8.id,
                             from_chat_id=sg8.id, message_id=m_admin.id,
                             protect_content=True)
    record("T8f copy msg as group ADMIN", cp_admin.get("ok"),
           cp_admin.get("description") or "copied")

    # ---- T9: DM-relay --------------------------------------------------------
    print("\n=== T9: user session copies BIN_CHANNEL video into bot DM ===", flush=True)
    bin_msg = None
    async for msg in user.get_chat_history(BIN_CHANNEL, limit=1):
        bin_msg = msg.id
    record("T9a latest BIN_CHANNEL msg", bin_msg is not None, f"msg_id={bin_msg}")
    if bin_msg:
        try:
            sent = await fw(lambda: user.copy_message(
                chat_id=tb_peer.user_id, from_chat_id=BIN_CHANNEL,
                message_id=bin_msg), "session copy to bot DM")
            record("T9b session copy BIN->bot DM", True, f"dm_msg_id={sent.id}")
            await asyncio.sleep(3)
            seen3 = await get_updates_seen(TEST_BOT)
            dm_msg = next((mid for mid, cid, _ in seen3 if cid == USER_ACCOUNT_ID), None)
            record("T9c bot received DM in updates", dm_msg is not None,
                   f"dm_msg_id={dm_msg}")
            if dm_msg:
                cp_dm = await bot_api(TEST_BOT, "copyMessage", chat_id=sg8.id,
                                      from_chat_id=USER_ACCOUNT_ID,
                                      message_id=dm_msg, protect_content=True)
                record("T9d bot copyMessage from its DM (protect_content)",
                       cp_dm.get("ok"), cp_dm.get("description") or "copied")
        except RPCError as e:
            record("T9b session copy BIN->bot DM", False, str(e))

    # ---- T7 retry: bot self-join via invite link -----------------------------
    print("\n=== T7: bot self-joins supergroup via invite link ===", flush=True)
    sg7 = await fw(lambda: user.create_supergroup("T7 selfjoin test", "tmp"), "create sg7")
    link = await fw(lambda: user.create_chat_invite_link(sg7.id), "invite link")
    bot_client = Client("tb_selfjoin2", api_id=API_ID, api_hash=API_HASH,
                        bot_token=TEST_BOT, in_memory=True, no_updates=True)
    await bot_client.start()
    try:
        joined = await fw(lambda: bot_client.join_chat(link.invite_link), "bot join_chat")
        record("T7 bot self-join via invite link", True, f"joined {joined.id}")
    except RPCError as e:
        record("T7 bot self-join via invite link", False, str(e))
    await bot_client.stop()

    for g in (sg8.id, sg7.id):
        try:
            await user.delete_supergroup(g)
        except RPCError:
            pass
    await user.stop()
    with open("/app/test_reports/copymessage_findings_r3.json", "w") as f:
        json.dump(RESULTS, f, indent=2)
    print("done -> /app/test_reports/copymessage_findings_r3.json", flush=True)


asyncio.run(main())
