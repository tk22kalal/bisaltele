"""Restart-proof auto-delete for lectures delivered into users' bots.

State lives in a dedicated MongoDB collection (separate cluster), so it
survives the hourly VPS restarts that killed the old in-process timer.

Two independent mechanisms:
  - Sliding window (primary): at most AUTODEL_MAX_LECTURES messages per bot.
    When lecture #11 is delivered, lecture #1 is deleted from the user's bot.
    Triggered on delivery, so it always runs regardless of restarts.
  - TTL sweeper (optional, AUTODEL_TTL_HOURS > 0): periodic task that deletes
    any recorded message older than the TTL. Resumes automatically on startup.
"""

import time
import asyncio
import logging

import motor.motor_asyncio

from biisal.vars import Var
from biisal.utils.telegram_delivery import _bot_api

logger = logging.getLogger("stream.autodelete")

_DB_NAME = "bisal_autodel"
_SWEEP_INTERVAL = 15 * 60

_client = None
_col = None
if Var.AUTODEL_DB_URI:
    try:
        _client = motor.motor_asyncio.AsyncIOMotorClient(Var.AUTODEL_DB_URI)
        _col = _client[_DB_NAME].delivered_lectures
    except Exception as error:  # noqa: BLE001
        logger.error("Autodelete Mongo init failed: %s", error)

_locks: dict = {}
_index_ensured = False
_sweeper_started = False


def _lock(key: str) -> asyncio.Lock:
    lock = _locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _locks[key] = lock
    return lock


async def _ensure_index():
    global _index_ensured
    if _index_ensured or _col is None:
        return
    await _col.create_index([("bot_token", 1), ("delivered_at", -1)])
    _index_ensured = True


async def _delete_recorded(doc: dict):
    """Delete the Telegram message, then its Mongo record."""
    try:
        data = await _bot_api(doc["bot_token"], "deleteMessage", {
            "chat_id": doc["chat_id"],
            "message_id": doc["message_id"],
        })
        if not data.get("ok"):
            logger.warning("Autodelete deleteMessage failed for bot %s msg %s: %s",
                           doc.get("bot_id"), doc["message_id"],
                           data.get("description"))
    except Exception as error:  # noqa: BLE001
        logger.warning("Autodelete deleteMessage error: %s", error)
    await _col.delete_one({"_id": doc["_id"]})


async def record_delivery(bot_token: str, bot_id, chat_id, message_id: int,
                          file_name: str | None = None):
    """Record a delivered lecture and enforce the per-bot sliding window."""
    if _col is None or not message_id:
        return
    await _ensure_index()
    async with _lock(bot_token):
        await _col.insert_one({
            "bot_token": bot_token,
            "bot_id": bot_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "file_name": file_name,
            "delivered_at": time.time(),
        })
        max_keep = Var.AUTODEL_MAX_LECTURES
        if max_keep <= 0:
            return
        stale = await _col.find(
            {"bot_token": bot_token}
        ).sort("delivered_at", -1).skip(max_keep).to_list(length=100)
        for doc in stale:
            logger.info("Sliding window: deleting oldest lecture %s (msg %s) for bot %s",
                        doc.get("file_name"), doc["message_id"], doc.get("bot_id"))
            await _delete_recorded(doc)


async def count_lectures(bot_token: str) -> int:
    if _col is None:
        return 0
    return await _col.count_documents({"bot_token": bot_token})


async def _sweeper():
    while True:
        await asyncio.sleep(_SWEEP_INTERVAL)
        ttl_hours = Var.AUTODEL_TTL_HOURS
        if _col is None or ttl_hours <= 0:
            continue
        cutoff = time.time() - ttl_hours * 3600
        try:
            stale = await _col.find({"delivered_at": {"$lt": cutoff}}).to_list(length=200)
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete sweeper query failed: %s", error)
            continue
        for doc in stale:
            logger.info("TTL: deleting lecture %s (msg %s) for bot %s",
                        doc.get("file_name"), doc["message_id"], doc.get("bot_id"))
            await _delete_recorded(doc)


def start_sweeper():
    """Start the TTL sweeper once, on the running loop."""
    global _sweeper_started
    if _sweeper_started or _col is None:
        return
    _sweeper_started = True
    asyncio.create_task(_sweeper())
    logger.info("Autodelete sweeper started (TTL=%sh, max=%s per bot)",
                Var.AUTODEL_TTL_HOURS, Var.AUTODEL_MAX_LECTURES)
