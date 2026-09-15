"""Live experiment: can copyMessage work WITHOUT admin rights?

Tests:
  T1  user bot as PLAIN MEMBER of BIN_CHANNEL (channel) -> copyMessage?
  T2  user bot as plain member of a SUPERGROUP -> copyMessage?
  T3  supergroup with protected content ON -> copyMessage?
  T4  main bot leg: DB_CHANNEL -> supergroup (main bot admin in DB only)
Results printed + saved to /app/test_reports/copymessage_findings.json
"""
import asyncio
import json
import aiohttp
from pyrogram import Client
from pyrogram.errors import RPCError, FloodWait


async def fw(coro_factory, what="call"):
    """Retry wrapper that sleeps through Telegram FLOOD_WAIT."""
    while True:
        try:
            return await coro_factory()
        except FloodWait as e:
            wait = e.value + 5
            print(f"  floodwait on {what}: sleeping {wait}s", flush=True)
            await asyncio.sleep(wait)

API_ID = 24316517
API_HASH = "ab33479d43c662f11cf9ae4b26350709"
SESSION = "BQFzCmUAtVFj0sTpMdbRm95HKU90B1pynTfw9YJyfIl1tjt4q-opjhxHDgxB3u2vuuRGLMmng6AcaAMmrxnLC2Z1e-Fm8Eh3462vznKReDtcRqk3l5EeIizWZrD3IDrtXd3DEtoJz-ljOvCkij7Cqz6f367MFlhUtcdE31-TRwPBNbObPb0YnpRygK_7GtAAi9JC1xEl8s4qGVccZL17D-FnfQPwaT4rk7nAC3gEwXBbc7OZx8w53-oVqNXz2N1IPevan3MYDdKHPB2kV3SmlQ3G4qAJdiIVSnfwCG8BIlvAIwY5U2YsiBA6hTfkoG2dN_Vyr3yeldUAKhUp8F2x8MJC_YuHAAAAAAGUl4H8AA"
MAIN_BOT = "8446242353:AAG0EuaKSYudx1ODqYUUZnaPDoPt2-lnnfc"
TEST_BOT = "8932279745:AAGjQN7X2CQXOYtqE6vIYpAkUKMFarwXlnw"
BIN_CHANNEL = -1002133410746
DB_CHANNEL = -1002024354927

API = "https://api.telegram.org"
RESULTS = {}


async def bot_api(token, method, **params):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{API}/bot{token}/{method}", json=params) as r:
            return await r.json()


def record(key, ok, detail):
    RESULTS[key] = {"ok": ok, "detail": str(detail)[:400]}
    print(f"[{'PASS' if ok else 'FAIL'}] {key}: {str(detail)[:300]}")


async def main():
    user = Client("copytester", api_id=API_ID, api_hash=API_HASH,
                  session_string=SESSION, in_memory=True, no_updates=True)
    await user.start()
    me = await user.get_me()
    print(f"user session: id={me.id} @{me.username}")

    tb = (await bot_api(TEST_BOT, "getMe"))["result"]
    mb = (await bot_api(MAIN_BOT, "getMe"))["result"]
    print(f"test user-bot: {tb['id']} @{tb['username']} | main bot: {mb['id']} @{mb['username']}")

    # sanity: who is admin where
    for cid, name in ((BIN_CHANNEL, "BIN_CHANNEL"), (DB_CHANNEL, "DB_CHANNEL")):
        m = await bot_api(MAIN_BOT, "getChatMember", chat_id=cid, user_id=mb["id"])
        record(f"sanity mainbot in {name}", m.get("ok"), m.get("result", m).get("status") if m.get("ok") else m.get("description"))

    # ---- STEP 0: create test supergroup ------------------------------------
    # resolve usernames ONCE (flood-safe) and reuse numeric ids everywhere
    tb_peer = await fw(lambda: user.resolve_peer(tb["username"]), "resolve test bot")
    mb_peer = await fw(lambda: user.resolve_peer(mb["username"]), "resolve main bot")
    sg = await fw(lambda: user.create_supergroup("BIN-SG copyMessage test", "temporary test group"), "create supergroup")
    sg_id = sg.id
    print(f"\ntest supergroup created: {sg_id}", flush=True)
    await fw(lambda: user.add_chat_members(sg_id, tb_peer.user_id), "add test bot")   # plain member
    await fw(lambda: user.add_chat_members(sg_id, mb_peer.user_id), "add main bot")   # plain member
    tm = await user.send_message(sg_id, "copy test message #1")
    record("T0 supergroup setup", True, f"sg_id={sg_id} msg_id={tm.id}")

    # ---- T1: bot as PLAIN MEMBER of a channel ------------------------------
    print("\n=== T1: user bot as plain member of BIN_CHANNEL ===")
    t1_added = False
    try:
        await fw(lambda: user.add_chat_members(BIN_CHANNEL, tb_peer.user_id), "add bot to channel")
        t1_added = True
        member = await user.get_chat_member(BIN_CHANNEL, tb_peer.user_id)
        record("T1a add bot to channel as member", True, f"status={member.status}")
    except RPCError as e:
        record("T1a add bot to channel as member", False, str(e))
    if t1_added:
        g = await bot_api(TEST_BOT, "getChat", chat_id=BIN_CHANNEL)
        record("T1b bot getChat on channel", g.get("ok"), g.get("description") or "bot can see chat object")
        latest = None
        try:
            async for msg in user.get_chat_history(BIN_CHANNEL, limit=1):
                latest = msg.id
        except RPCError as e:
            print("  (cannot read BIN history via user:", e, ")")
        if latest:
            cp = await bot_api(TEST_BOT, "copyMessage", chat_id=sg_id,
                               from_chat_id=BIN_CHANNEL, message_id=latest)
            record("T1c copyMessage from channel (bot=member)", cp.get("ok"),
                   cp.get("description") or f"copied -> msg {(cp.get('result') or {}).get('message_id')}")
        # cleanup: remove test bot from BIN_CHANNEL
        try:
            await user.ban_chat_member(BIN_CHANNEL, tb_peer.user_id)
            await user.unban_chat_member(BIN_CHANNEL, tb_peer.user_id)
            print("  cleanup: test bot removed from BIN_CHANNEL")
        except RPCError as e:
            print("  cleanup failed:", e)

    # ---- T2: bot as plain member of a SUPERGROUP ---------------------------
    print("\n=== T2: user bot as plain member of supergroup ===")
    g2 = await bot_api(TEST_BOT, "getChat", chat_id=sg_id)
    record("T2a bot getChat on supergroup", g2.get("ok"), g2.get("description") or "ok")
    cp2 = await bot_api(TEST_BOT, "copyMessage", chat_id=sg_id, from_chat_id=sg_id,
                        message_id=tm.id, protect_content=True)
    record("T2b copyMessage from supergroup (bot=member, protect_content)",
           cp2.get("ok"), cp2.get("description") or f"copied -> msg {(cp2.get('result') or {}).get('message_id')}")

    # privacy mode: try reading message via getUpdates-independent call (forward check)
    cp2c = await bot_api(TEST_BOT, "copyMessage", chat_id=sg_id, from_chat_id=sg_id,
                         message_id=tm.id)
    record("T2c copyMessage without protect (privacy-mode check)",
           cp2c.get("ok"), cp2c.get("description") or "ok")

    # ---- T4: main bot leg DB_CHANNEL -> supergroup -------------------------
    print("\n=== T4: main bot copies DB_CHANNEL -> supergroup ===")
    db_msg = None
    try:
        async for msg in user.get_chat_history(DB_CHANNEL, limit=1):
            db_msg = msg.id
    except RPCError as e:
        print("  user session cannot read DB_CHANNEL:", e)
    if db_msg is None:
        # ask main bot for a known id via user session is impossible; skip
        record("T4 DB->SG via main bot", False, "no DB message id available; leg identical to existing proven DB->BIN flow")
    else:
        cp3 = await bot_api(MAIN_BOT, "copyMessage", chat_id=sg_id,
                            from_chat_id=DB_CHANNEL, message_id=db_msg)
        ok3 = cp3.get("ok")
        record("T4a main bot copyMessage DB->SG", ok3,
               cp3.get("description") or f"copied msg {db_msg}")
        if ok3:
            sg_msg_id = (cp3.get("result") or {}).get("message_id")
            # now the user bot copies THAT message out of the supergroup
            upd = await bot_api(TEST_BOT, "getUpdates", limit=100, timeout=0)
            dest = None
            for u in reversed(upd.get("result", [])):
                chat = (u.get("message") or {}).get("chat") or {}
                if chat.get("type") == "private":
                    dest = chat["id"]
                    break
            dest = dest or sg_id
            cp4 = await bot_api(TEST_BOT, "copyMessage", chat_id=dest,
                                from_chat_id=sg_id, message_id=sg_msg_id,
                                protect_content=True)
            record("T4b user bot copyMessage SG->user (protect_content)",
                   cp4.get("ok"), cp4.get("description") or f"delivered to {dest}")

    # ---- T3: protected-content supergroup -----------------------------------
    print("\n=== T3: supergroup with protected content ON ===")
    try:
        await user.set_chat_protected_content(sg_id, True)
        cp5 = await bot_api(TEST_BOT, "copyMessage", chat_id=sg_id,
                            from_chat_id=sg_id, message_id=tm.id)
        record("T3 copyMessage from PROTECTED supergroup", cp5.get("ok"),
               cp5.get("description") or "copied")
        await user.set_chat_protected_content(sg_id, False)
    except (RPCError, AttributeError) as e:
        record("T3 protected content test", None, f"could not toggle: {e}")

    # ---- cleanup -------------------------------------------------------------
    try:
        await user.delete_supergroup(sg_id)
        print("\ntest supergroup deleted")
    except (RPCError, AttributeError) as e:
        print("\ncould not delete supergroup:", e)

    await user.stop()
    with open("/app/test_reports/copymessage_findings.json", "w") as f:
        json.dump(RESULTS, f, indent=2)
    print("\nresults saved to /app/test_reports/copymessage_findings.json")


asyncio.run(main())
