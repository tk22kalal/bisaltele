"""Personal-bot delivery: hand a BIN_CHANNEL message to the user's own bot.

Flow (see /api/telegram/{token} in stream_routes.py):
  1. Resolve the access_code -> user's bot token (+ optional saved chat id) from
     Supabase through a SECURITY DEFINER RPC (anon key cannot read tables).
  2. Make the user's bot a full admin of BIN_CHANNEL (once) using a Telegram
     USER session, because a bot cannot add/promote another bot via Bot API.
  3. The main bot copies the video from DB_CHANNEL into BIN_CHANNEL (done by the
     route, exactly like the Web/WebX path).
  4. The user's own bot copies that BIN_CHANNEL message to the user with
     protect_content=ON, via the Telegram Bot HTTP API.
"""

import os
import time
import asyncio
import logging

import aiohttp
from pyrogram import Client
from pyrogram.types import ChatPrivileges
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserNotParticipant, RPCError

from biisal.vars import Var

logger = logging.getLogger("stream.telegram_delivery")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

_TG_API = "https://api.telegram.org"

# One shared user-account session used only to promote/demote user bots in BIN_CHANNEL.
_user_client: Client | None = None
_user_client_lock = asyncio.Lock()

# Telegram channels allow at most 50 admins total (bots included). We rotate:
# promote the user's bot -> deliver -> demote it. MAX_BIN_ADMIN_BOTS bounds how
# many user bots may hold admin at once, leaving headroom for real admins.
MAX_BIN_ADMIN_BOTS = 45
_STALE_ADMIN_SECONDS = 15 * 60          # safety-net demote for stragglers
_DELETE_AFTER_SECONDS = 24 * 60 * 60    # best-effort auto-delete of delivered msg

_admin_semaphore = asyncio.Semaphore(MAX_BIN_ADMIN_BOTS)
# bot_id -> {"peer": str|int, "ts": float}  (bots WE promoted and must demote)
_promoted_registry: dict = {}
_registry_lock = asyncio.Lock()
_sweeper_started = False


# ── Supabase helpers (RPC only, matches supabase_quota pattern) ───────────────

async def _rpc(function_name: str, payload: dict):
    if not (SUPABASE_URL and SUPABASE_KEY):
        return None, "Supabase is not configured"
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/{function_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(endpoint, json=payload, headers=headers) as resp:
                body = await resp.text()
                if resp.status < 200 or resp.status >= 300:
                    logger.error("Supabase RPC %s failed %s: %s", function_name, resp.status, body[:300])
                    return None, "Supabase delivery lookup failed"
                if not body.strip():
                    return None, None
                import json
                return json.loads(body), None
    except (aiohttp.ClientError, asyncio.TimeoutError) as error:
        logger.error("Supabase RPC %s error: %s", function_name, error)
        return None, "Supabase is unavailable"


def _first(value):
    if isinstance(value, list):
        return value[0] if value and isinstance(value[0], dict) else None
    return value if isinstance(value, dict) else None


async def fetch_delivery_config(access_code: str):
    """Return {'bot_token', 'chat_id'} for the user tied to this access code."""
    result, error = await _rpc("get_media_delivery_config", {"p_code": access_code})
    if error:
        return None, error
    row = _first(result)
    if not row or not (row.get("bot_token") or "").strip():
        return None, "No Telegram bot is configured for this account."
    return {
        "bot_token": row["bot_token"].strip(),
        "chat_id": (str(row.get("chat_id")).strip() if row.get("chat_id") else ""),
    }, None


async def save_delivery_chat(access_code: str, chat_id):
    await _rpc("set_media_delivery_chat", {"p_code": access_code, "p_chat_id": str(chat_id)})


# ── Telegram Bot HTTP API helpers (for the USER's own bot) ────────────────────

async def _bot_api(bot_token: str, method: str, params: dict):
    url = f"{_TG_API}/bot{bot_token}/{method}"
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=params) as resp:
            return await resp.json()


async def get_bot_identity(bot_token: str):
    """Return (bot_id, username) for the user's bot, or (None, None)."""
    data = await _bot_api(bot_token, "getMe", {})
    if not data.get("ok"):
        return None, None
    me = data["result"]
    return me.get("id"), me.get("username")


async def resolve_owner_chat_id(bot_token: str):
    """Discover who /start'd the user's bot via getUpdates (last messager)."""
    data = await _bot_api(bot_token, "getUpdates", {"limit": 100, "timeout": 0})
    if not data.get("ok"):
        return None
    for update in reversed(data.get("result", [])):
        msg = update.get("message") or update.get("my_chat_member") or {}
        chat = msg.get("chat") or {}
        if chat.get("type") == "private" and chat.get("id"):
            return chat["id"]
    return None


async def deliver_via_user_bot(bot_token: str, chat_id, bin_message_id: int, caption: str | None):
    """Copy the BIN_CHANNEL message into the user's chat with protect_content ON.

    Returns (True, delivered_message_id) or (False, error_text)."""
    params = {
        "chat_id": chat_id,
        "from_chat_id": Var.BIN_CHANNEL,
        "message_id": bin_message_id,
        "protect_content": True,
    }
    if caption:
        params["caption"] = caption[:1024]
    data = await _bot_api(bot_token, "copyMessage", params)
    if not data.get("ok"):
        return False, data.get("description", "Telegram delivery failed")
    return True, (data.get("result") or {}).get("message_id")


async def delete_user_message(bot_token: str, chat_id, message_id: int):
    """Best-effort delete of a message the user's bot previously sent."""
    try:
        await _bot_api(bot_token, "deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception as error:  # noqa: BLE001
        logger.warning("Auto-delete failed for chat %s msg %s: %s", chat_id, message_id, error)


def schedule_message_deletion(bot_token: str, chat_id, message_id, delay: int = _DELETE_AFTER_SECONDS):
    """Fire-and-forget: remove the delivered video from the user's bot after `delay`.

    Best-effort only — an in-process timer that is lost if the bot restarts."""
    if not message_id:
        return

    async def _task():
        await asyncio.sleep(delay)
        await delete_user_message(bot_token, chat_id, message_id)

    asyncio.create_task(_task())


# ── User session (promotes the user's bot into BIN_CHANNEL) ───────────────────

async def _get_user_client():
    global _user_client
    if _user_client is not None:
        return _user_client
    async with _user_client_lock:
        if _user_client is not None:
            return _user_client
        session_string = Var.USER_SESSION_STRING
        if not session_string:
            return None
        client = Client(
            name="bin_promoter",
            api_id=Var.API_ID,
            api_hash=Var.API_HASH,
            session_string=session_string,
            no_updates=True,
            in_memory=True,
        )
        await client.start()
        _user_client = client
        logger.info("User session for BIN_CHANNEL promotion started")
        return _user_client


_FULL_PRIVILEGES = ChatPrivileges(
    can_manage_chat=True,
    can_delete_messages=True,
    can_manage_video_chats=True,
    can_restrict_members=True,
    can_promote_members=True,
    can_change_info=True,
    can_post_messages=True,
    can_edit_messages=True,
    can_invite_users=True,
    can_pin_messages=True,
    is_anonymous=False,
)
_NO_PRIVILEGES = ChatPrivileges(
    can_manage_chat=False,
    can_delete_messages=False,
    can_manage_video_chats=False,
    can_restrict_members=False,
    can_promote_members=False,
    can_change_info=False,
    can_post_messages=False,
    can_edit_messages=False,
    can_invite_users=False,
    can_pin_messages=False,
    is_anonymous=False,
)


def _ensure_sweeper():
    """Start the safety-net sweeper once, on the running loop."""
    global _sweeper_started
    if _sweeper_started:
        return
    _sweeper_started = True
    asyncio.create_task(_sweep_stale_admins())


async def _sweep_stale_admins():
    """Demote any bot WE promoted that lingered past the stale window."""
    while True:
        await asyncio.sleep(60)
        now = time.time()
        stale = []
        async with _registry_lock:
            for bid, info in list(_promoted_registry.items()):
                if now - info["ts"] > _STALE_ADMIN_SECONDS:
                    stale.append((bid, info["peer"]))
        for bot_id, peer in stale:
            logger.warning("Sweeper demoting stale admin bot %s", bot_id)
            await release_bin_admin(bot_id, peer)


async def acquire_bin_admin(bot_token: str):
    """Make the user's bot a full admin of BIN_CHANNEL for one delivery.

    Returns (status, bot_id, peer, error):
      status == "promoted" -> we promoted it; caller MUST release_bin_admin().
      status == "already"  -> pre-existing admin (manual); do NOT release.
      status == "error"    -> error set, bot_id/peer None.
    """
    bot_id, username = await get_bot_identity(bot_token)
    if not bot_id:
        return "error", None, None, "Invalid bot token."

    client = await _get_user_client()
    if client is None:
        return "error", None, None, "Server is missing USER_SESSION_STRING for bot promotion."

    _ensure_sweeper()
    peer = f"@{username}" if username else bot_id

    # Already an admin (e.g. added manually during the 24h fresh-session window)?
    # Reuse it and do not manage its lifecycle.
    try:
        member = await client.get_chat_member(Var.BIN_CHANNEL, peer)
        if member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            return "already", bot_id, peer, None
    except UserNotParticipant:
        pass
    except RPCError:
        pass

    # Reserve one of the 45 admin slots (wait briefly if the channel is full).
    try:
        await asyncio.wait_for(_admin_semaphore.acquire(), timeout=30)
    except asyncio.TimeoutError:
        return "error", None, None, (
            "The delivery service is busy right now. Please try again shortly."
        )

    try:
        await client.promote_chat_member(Var.BIN_CHANNEL, peer, privileges=_FULL_PRIVILEGES)
    except UserNotParticipant:
        try:
            await client.add_chat_members(Var.BIN_CHANNEL, peer)
            await client.promote_chat_member(Var.BIN_CHANNEL, peer, privileges=_FULL_PRIVILEGES)
        except RPCError as error:
            _admin_semaphore.release()
            logger.error("Failed to add+promote bot %s: %s", bot_id, error)
            return "error", None, None, "Could not add the bot to BIN_CHANNEL."
    except RPCError as error:
        text = str(error)
        if "not modified" in text.lower():
            pass  # already has these rights
        elif "FRESH_CHANGE_ADMINS_FORBIDDEN" in text:
            _admin_semaphore.release()
            logger.error("Promote blocked (fresh session) for bot %s: %s", bot_id, error)
            return "error", None, None, (
                "The promoter account was logged in too recently. Add the bot to "
                "BIN_CHANNEL as admin manually once, or retry ~24h after login."
            )
        else:
            _admin_semaphore.release()
            logger.error("Failed to promote bot %s: %s", bot_id, error)
            return "error", None, None, "Could not grant admin rights to the bot in BIN_CHANNEL."

    async with _registry_lock:
        _promoted_registry[bot_id] = {"peer": peer, "ts": time.time()}
    return "promoted", bot_id, peer, None


async def release_bin_admin(bot_id: int, peer):
    """Demote (and thereby remove) a bot we previously promoted; free its slot."""
    async with _registry_lock:
        present = _promoted_registry.pop(bot_id, None)
    if present is None:
        return  # already released or never ours
    client = await _get_user_client()
    if client is not None:
        try:
            await client.promote_chat_member(Var.BIN_CHANNEL, peer, privileges=_NO_PRIVILEGES)
        except RPCError as error:
            logger.error("Failed to demote bot %s: %s", bot_id, error)
    _admin_semaphore.release()
