"""Personal-bot delivery via DM-relay (no admin rights needed for user bots).

Flow (see /api/telegram/{token} in stream_routes.py):
  1. Resolve the access_code -> user's bot token (+ optional saved chat id).
  2. The main bot copies the DB_CHANNEL message into BIN_CHANNEL (DB_CHANNEL has
     "restrict saving content" ON, so only the bot -- admin of both channels --
     can copy out of it; the fresh BIN copy is unrestricted).
  3. A Telegram USER session copies that BIN message straight into the user's
     bot DM -- one plain copy op. The bot never becomes admin of any channel, so
     there is no 50-admin cap, no promote/demote churn, no session rate-limit
     storm, and both channels stay anonymous to the user's bot.
  4. The bot picks the message up via getUpdates, copies it to its owner with
     protect_content=ON, then deletes the DM copy.
"""

import os
import time
import asyncio
import logging

import aiohttp
import motor.motor_asyncio
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from biisal.vars import Var

logger = logging.getLogger("stream.telegram_delivery")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

_TG_API = "https://api.telegram.org"

# FloodWait beyond these caps means Telegram is throttling this account hard
# (ResolveUsername floods can be many HOURS).  We must NOT sleep through those
# or the delivery request hangs forever; instead fail fast with a clear
# message so the user (and the 10s frontend timer) get an instant answer.
RESOLVE_MAX_FLOOD_WAIT = int(os.getenv("TG_RESOLVE_MAX_FLOOD_WAIT", "45"))
COPY_MAX_FLOOD_WAIT = int(os.getenv("TG_COPY_MAX_FLOOD_WAIT", "60"))
DIALOG_SCAN_LIMIT = int(os.getenv("TG_DIALOG_SCAN_LIMIT", "500"))

_RATE_LIMIT_MSG = (
    "Telegram is rate-limiting delivery right now. "
    "Please wait a few minutes and tap Send again."
)


class DeliveryRateLimited(Exception):
    """Raised when a FloodWait exceeds our cap so we fail fast, never hang."""

    def __init__(self, seconds: int):
        self.seconds = seconds
        super().__init__(f"rate limited for {seconds}s")


# One shared user-account session used only to copy the source message into
# each user's bot DM.
_user_client: Client | None = None
_user_client_lock = asyncio.Lock()
_session_account_id: int | None = None
_peer_cache: dict = {}          # bot_id -> resolved MTProto peer (per process)
_delivery_locks: dict = {}      # bot_id -> asyncio.Lock (serialize per bot)

# Persistent (restart-proof) bot-peer cache.  Resolving a bot by username via
# MTProto (contacts.ResolveUsername) is heavily rate-limited; doing it on every
# delivery / process restart exhausts the quota and yields multi-hour
# FloodWaits (the root cause of "delivery not possible" / stuck sends).  We
# resolve each bot by username at most ONCE, then persist its access_hash and
# rebuild the peer locally forever after — no further ResolveUsername calls.
_peer_db_client = None
_peer_col = None


def _peer_collection():
    global _peer_db_client, _peer_col
    if _peer_col is not None:
        return _peer_col
    uri = Var.DATABASE_URL
    if not uri:
        return None
    _peer_db_client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    _peer_col = _peer_db_client[Var.name].bot_peer_cache
    return _peer_col


async def _load_persisted_peer(bot_id: int):
    col = _peer_collection()
    if col is None:
        return None
    try:
        return await col.find_one({"_id": bot_id})
    except Exception as error:  # noqa: BLE001
        logger.warning("peer cache load failed for %s: %s", bot_id, error)
        return None


async def _save_persisted_peer(bot_id: int, access_hash, username: str | None):
    col = _peer_collection()
    if col is None:
        return
    try:
        await col.update_one(
            {"_id": bot_id},
            {"$set": {
                "access_hash": str(access_hash),
                "username": username,
                "updated_at": time.time(),
            }},
            upsert=True,
        )
    except Exception as error:  # noqa: BLE001
        logger.warning("peer cache save failed for %s: %s", bot_id, error)


async def _invalidate_peer(bot_id: int):
    _peer_cache.pop(bot_id, None)
    col = _peer_collection()
    if col is None:
        return
    try:
        await col.delete_one({"_id": bot_id})
    except Exception as error:  # noqa: BLE001
        logger.warning("peer cache delete failed for %s: %s", bot_id, error)


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


# Per-bot getUpdates cursor. Polling with a moving positive offset keeps each
# bot's update queue drained, so bots that were (manually) added as admins of
# busy channels can never flood the queue and hide the relayed DM message.
_update_offsets: dict = {}


async def _poll_updates(bot_token: str, limit: int = 100):
    """Return pending updates and advance the per-bot cursor past them."""
    offset = _update_offsets.get(bot_token)
    params = {"limit": limit, "timeout": 0,
              "offset": offset if offset else -limit}
    data = await _bot_api(bot_token, "getUpdates", params)
    updates = data.get("result", []) if data.get("ok") else []
    if updates:
        _update_offsets[bot_token] = updates[-1]["update_id"] + 1
    elif not data.get("ok"):
        logger.warning("getUpdates failed: %s", data.get("description"))
    return updates


async def resolve_owner_chat_id(bot_token: str):
    """Discover who /start'd the user's bot via getUpdates (last messager)."""
    for update in reversed(await _poll_updates(bot_token)):
        msg = update.get("message") or update.get("my_chat_member") or {}
        chat = msg.get("chat") or {}
        if chat.get("type") == "private" and chat.get("id"):
            return chat["id"]
    return None


async def delete_user_message(bot_token: str, chat_id, message_id: int):
    """Best-effort delete of a message the user's bot can access."""
    try:
        await _bot_api(bot_token, "deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception as error:  # noqa: BLE001
        logger.warning("Auto-delete failed for chat %s msg %s: %s", chat_id, message_id, error)


# ── User session (copies the source message into each bot's DM) ───────────────

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
            name="dm_relay",
            api_id=Var.API_ID,
            api_hash=Var.API_HASH,
            session_string=session_string,
            no_updates=True,
            in_memory=True,
        )
        await client.start()
        _user_client = client
        logger.info("User session for DM-relay delivery started")
        return client


async def _flood_safe(factory, what="telegram call", max_wait=None):
    """Retry a session call, sleeping through short Telegram FLOOD_WAITs.

    A FloodWait larger than `max_wait` (when given) is NOT slept through —
    it raises DeliveryRateLimited so the caller fails fast instead of blocking
    the request (and the user) for minutes or hours."""
    while True:
        try:
            return await factory()
        except FloodWait as error:
            wait = int(getattr(error, "value", 0) or 0)
            if max_wait is not None and wait > max_wait:
                logger.error(
                    "FloodWait %ss on %s exceeds cap %ss; failing fast",
                    wait, what, max_wait,
                )
                raise DeliveryRateLimited(wait)
            logger.warning("FloodWait %ss on %s", wait, what)
            await asyncio.sleep(wait + 2)


async def _resolve_bot_peer(client: Client, bot_id: int, username: str | None,
                            force_fresh: bool = False):
    if not force_fresh and bot_id in _peer_cache:
        return _peer_cache[bot_id]

    # 1. Rebuild the peer from a previously persisted access_hash (survives
    #    restarts) so a known bot never triggers ResolveUsername again.
    if not force_fresh:
        persisted = await _load_persisted_peer(bot_id)
        if persisted and persisted.get("access_hash"):
            try:
                await client.storage.update_peers([(
                    bot_id, int(persisted["access_hash"]), "bot",
                    persisted.get("username") or username, None,
                )])
                peer = await client.resolve_peer(bot_id)
                _peer_cache[bot_id] = peer
                return peer
            except (RPCError, ValueError, TypeError) as error:
                logger.warning(
                    "Persisted peer for %s unusable (%s); re-resolving",
                    bot_id, error,
                )

    # 2. Cold path: resolve by username exactly once, then persist the peer.
    if username:
        try:
            peer = await _flood_safe(
                lambda: client.resolve_peer(username),
                f"resolve bot peer {bot_id}",
                max_wait=RESOLVE_MAX_FLOOD_WAIT,
            )
            _peer_cache[bot_id] = peer
            access_hash = getattr(peer, "access_hash", None)
            if access_hash is not None:
                await _save_persisted_peer(bot_id, access_hash, username)
            return peer
        except DeliveryRateLimited:
            # ResolveUsername is flood-limited.  Fall back to a dialog scan,
            # which recovers any bot the session already has a DM with — no
            # ResolveUsername needed — so previously-connected users keep
            # working even while the account is throttled.
            peer = await _resolve_via_dialogs(client, bot_id, username)
            if peer is not None:
                return peer
            raise

    peer = await _resolve_via_dialogs(client, bot_id, None)
    if peer is not None:
        return peer
    raise DeliveryRateLimited(0)


async def _resolve_via_dialogs(client: Client, bot_id: int, username: str | None):
    """Recover a bot's peer from the session's existing dialogs (getDialogs is
    not ResolveUsername, so it survives a ResolveUsername flood)."""
    try:
        async for dialog in client.get_dialogs(limit=DIALOG_SCAN_LIMIT):
            if dialog.chat and dialog.chat.id == bot_id:
                peer = await client.resolve_peer(bot_id)
                _peer_cache[bot_id] = peer
                access_hash = getattr(peer, "access_hash", None)
                if access_hash is not None:
                    await _save_persisted_peer(
                        bot_id, access_hash,
                        username or getattr(dialog.chat, "username", None),
                    )
                logger.info("Recovered bot %s peer via dialog scan", bot_id)
                return peer
    except (RPCError, FloodWait) as error:
        logger.warning("Dialog-scan resolve failed for %s: %s", bot_id, error)
    return None


async def _get_session_account_id(client: Client) -> int:
    global _session_account_id
    if _session_account_id is None:
        me = await client.get_me()
        _session_account_id = me.id
    return _session_account_id


def _delivery_lock(bot_id: int) -> asyncio.Lock:
    lock = _delivery_locks.get(bot_id)
    if lock is None:
        lock = asyncio.Lock()
        _delivery_locks[bot_id] = lock
    return lock


async def _find_dm_message(bot_token: str, account_id: int, attempts: int = 6):
    """Poll the bot's updates for the DM message the session just relayed.

    The cursor was advanced past the backlog just before the relay copy, so
    only fresh updates arrive here."""
    for _ in range(attempts):
        found = None
        for upd in await _poll_updates(bot_token):
            msg = upd.get("message") or {}
            chat = msg.get("chat") or {}
            if chat.get("type") == "private" and chat.get("id") == account_id:
                found = msg.get("message_id") or found
        if found:
            return found
        await asyncio.sleep(1.5)
    return None


async def deliver_via_dm_relay(bot_token: str, chat_id, from_chat_id,
                               message_id: int, caption: str | None):
    """Relay source message -> bot DM (user session) -> owner (bot, protected).

    Returns (True, delivered_message_id) or (False, error_text)."""
    bot_id, username = await get_bot_identity(bot_token)
    if not bot_id:
        return False, "Invalid bot token."

    client = await _get_user_client()
    if client is None:
        return False, "Server is missing USER_SESSION_STRING for delivery."

    async with _delivery_lock(bot_id):
        account_id = await _get_session_account_id(client)

        # Drain the bot's backlog so only the fresh DM message matches after.
        await _poll_updates(bot_token)

        # Resolve + copy into the bot's DM.  One retry with a fresh username
        # resolve if a stale persisted access_hash makes the copy fail.
        copied = False
        for attempt in range(2):
            try:
                await _resolve_bot_peer(
                    client, bot_id, username, force_fresh=(attempt == 1)
                )
            except DeliveryRateLimited as error:
                logger.error("Resolve rate-limited for bot %s: %ss",
                             bot_id, error.seconds)
                return False, _RATE_LIMIT_MSG
            except RPCError as error:
                logger.error("Cannot resolve bot %s: %s", bot_id, error)
                return False, "Could not reach your bot from the delivery service."

            try:
                await _flood_safe(
                    lambda: client.copy_message(
                        chat_id=bot_id,
                        from_chat_id=from_chat_id,
                        message_id=message_id,
                        caption=(caption[:1024] if caption else None),
                    ),
                    f"copy to bot DM {bot_id}",
                    max_wait=COPY_MAX_FLOOD_WAIT,
                )
                copied = True
                break
            except DeliveryRateLimited as error:
                logger.error("Copy rate-limited for bot %s: %ss",
                             bot_id, error.seconds)
                return False, _RATE_LIMIT_MSG
            except RPCError as error:
                if attempt == 0 and _is_peer_error(error):
                    logger.warning(
                        "Stale peer for bot %s (%s); retrying with fresh resolve",
                        bot_id, error,
                    )
                    await _invalidate_peer(bot_id)
                    continue
                logger.error("DM-relay copy failed for bot %s: %s", bot_id, error)
                return False, "Could not relay the file to your bot. Please try again."

        if not copied:
            return False, "Could not relay the file to your bot. Please try again."

        dm_message_id = await _find_dm_message(bot_token, account_id)
        if not dm_message_id:
            return False, "Your bot did not receive the file. Please try again."

        data = await _bot_api(bot_token, "copyMessage", {
            "chat_id": chat_id,
            "from_chat_id": account_id,
            "message_id": dm_message_id,
            "protect_content": True,
        })
        await delete_user_message(bot_token, account_id, dm_message_id)
        if not data.get("ok"):
            return False, data.get("description", "Telegram delivery failed")

        return True, (data.get("result") or {}).get("message_id")


def _is_peer_error(error: RPCError) -> bool:
    text = f"{getattr(error, 'ID', '')} {error}".upper()
    return any(tok in text for tok in ("PEER_ID_INVALID", "PEER_ID", "ACCESS_HASH"))
