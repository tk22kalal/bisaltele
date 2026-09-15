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


async def _delete_recorded(doc: dict) -> bool:
    """Delete the Telegram message, then its Mongo record.

    Returns True if the record was cleared (message gone or permanently
    ungettable). On a transient failure the record is KEPT so a later delivery
    or the sweeper can retry — this makes eviction self-healing across the
    hourly VPS restarts."""
    description = ""
    ok = False
    try:
        data = await _bot_api(doc["bot_token"], "deleteMessage", {
            "chat_id": doc["chat_id"],
            "message_id": doc["message_id"],
        })
        ok = data.get("ok", False)
        description = (data.get("description") or "").lower()
    except Exception as error:  # noqa: BLE001
        description = str(error).lower()
        logger.warning("Autodelete deleteMessage error: %s", error)

    # "message to delete not found" / "message can't be deleted" (older than
    # 48h) are permanent — the message is effectively gone or unrecoverable, so
    # stop tracking it. Anything else is transient: keep the record and retry.
    permanent = ok or "not found" in description or "can't be deleted" in description \
        or "message identifier is not specified" in description
    if not ok and not permanent:
        logger.warning("Autodelete keeping record for retry (bot %s msg %s): %s",
                       doc.get("bot_id"), doc["message_id"], description)
        return False
    await _col.delete_one({"_id": doc["_id"]})
    return True


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


async def _sweep_once():
    """One reconciliation pass: enforce both the sliding window and the TTL.

    Runs the same eviction the delivery path does, so any deletes interrupted
    by an hourly VPS restart are retried here. Self-healing and idempotent."""
    if _col is None:
        return
    max_keep = Var.AUTODEL_MAX_LECTURES
    ttl_hours = Var.AUTODEL_TTL_HOURS

    # 1) Sliding-window catch-up: any bot holding more than max_keep lectures.
    if max_keep > 0:
        try:
            bot_ids = await _col.distinct("bot_token")
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete sweeper distinct failed: %s", error)
            bot_ids = []
        for bot_token in bot_ids:
            async with _lock(bot_token):
                stale = await _col.find(
                    {"bot_token": bot_token}
                ).sort("delivered_at", -1).skip(max_keep).to_list(length=200)
                for doc in stale:
                    logger.info("Sweeper window: deleting lecture %s (msg %s) for bot %s",
                                doc.get("file_name"), doc["message_id"], doc.get("bot_id"))
                    await _delete_recorded(doc)

    # 2) TTL: delete anything older than the TTL regardless of count.
    if ttl_hours > 0:
        cutoff = time.time() - ttl_hours * 3600
        try:
            stale = await _col.find({"delivered_at": {"$lt": cutoff}}).to_list(length=500)
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete sweeper TTL query failed: %s", error)
            stale = []
        for doc in stale:
            logger.info("TTL: deleting lecture %s (msg %s) for bot %s",
                        doc.get("file_name"), doc["message_id"], doc.get("bot_id"))
            await _delete_recorded(doc)


async def _sweeper():
    # First pass shortly after startup — the VPS restarts hourly, so we cannot
    # wait a full interval before reconciling.
    await asyncio.sleep(30)
    while True:
        try:
            await _sweep_once()
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete sweep pass failed: %s", error)
        await asyncio.sleep(_SWEEP_INTERVAL)


def start_sweeper():
    """Start the TTL sweeper once, on the running loop."""
    global _sweeper_started
    if _sweeper_started or _col is None:
        return
    _sweeper_started = True
    asyncio.create_task(_sweeper())
    logger.info("Autodelete sweeper started (TTL=%sh, max=%s per bot)",
                Var.AUTODEL_TTL_HOURS, Var.AUTODEL_MAX_LECTURES)
