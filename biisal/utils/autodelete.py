"""Restart-proof auto-delete for lectures delivered into users' bots.

Every delivery is stored in MongoDB with a status:
  delivered    -> waiting for its TTL (AUTODEL_TTL_HOURS)
  deleted      -> removed from the user's bot (deleted_at set)
  undeletable  -> Telegram refused (bots can't delete after 48h) / already gone

Deletion runs:
  - 30s after every startup (catches up after VPS downtime)
  - every 10 minutes
  - daily full scans at AUTODEL_SCAN_HOURS (IST), default 3 AM and 5 AM

FloodWait (HTTP 429 retry_after) is honoured; transient errors are retried.
"""

import time
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import motor.motor_asyncio

from biisal.vars import Var
from biisal.utils.telegram_delivery import _bot_api

logger = logging.getLogger("stream.autodelete")

_SWEEP_INTERVAL = 10 * 60
_IST = timezone(timedelta(hours=5, minutes=30))
_DONE = ["deleted", "undeletable"]

_client = None
_col = None
_uri = Var.AUTODEL_DB_URI or Var.DATABASE_URL

if _uri:
    try:
        _client = motor.motor_asyncio.AsyncIOMotorClient(_uri)
        _db = "bisal_autodel" if Var.AUTODEL_DB_URI else Var.name
        _col = _client[_db].delivered_lectures
    except Exception as error:  # noqa: BLE001
        logger.error("Autodelete Mongo init failed: %s", error)
else:
    logger.error("Autodelete DISABLED: set AUTODEL_DB_URI or DATABASE_URL")

_locks: dict = {}
_index_ensured = False
_sweeper_started = False
_sweep_lock = asyncio.Lock()


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
    await _col.create_index([("status", 1), ("delivered_at", 1)])
    _index_ensured = True


async def _delete_recorded(doc: dict) -> bool:
    """Delete one message (FloodWait-aware) and update its Mongo status."""
    description = ""
    ok = False

    for _ in range(5):
        try:
            data = await _bot_api(
                doc["bot_token"],
                "deleteMessage",
                {
                    "chat_id": doc["chat_id"],
                    "message_id": doc["message_id"],
                },
            )
        except Exception as error:  # noqa: BLE001
            description = str(error).lower()
            await asyncio.sleep(3)
            continue

        ok = data.get("ok", False)
        description = (data.get("description") or "").lower()
        retry_after = (data.get("parameters") or {}).get("retry_after")

        if not ok and (data.get("error_code") == 429 or retry_after):
            wait = int(retry_after or 5) + 1
            logger.warning("FloodWait %ss on bot %s, waiting", wait, doc.get("bot_id"))
            await asyncio.sleep(wait)
            continue

        break

    now = time.time()

    if ok:
        status = "deleted"
    elif (
        "not found" in description
        or "can't be deleted" in description
        or "message identifier is not specified" in description
        or "unauthorized" in description
        or "bot was blocked" in description
        or "chat not found" in description
    ):
        status = "undeletable"
    else:
        await _col.update_one(
            {"_id": doc["_id"]},
            {
                "$inc": {"attempts": 1},
                "$set": {"last_error": description, "last_attempt_at": now},
            },
        )
        logger.warning(
            "Autodelete retry later (bot %s msg %s): %s",
            doc.get("bot_id"),
            doc["message_id"],
            description,
        )
        return False

    await _col.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "status": status,
                "deleted_at": now,
                "last_error": description,
            }
        },
    )
    logger.info(
        "Autodelete %s: %s (msg %s) bot %s",
        status,
        doc.get("file_name"),
        doc["message_id"],
        doc.get("bot_id"),
    )
    return True


async def record_delivery(
    bot_token: str,
    bot_id,
    chat_id,
    message_id: int,
    file_name: str | None = None,
):
    """Record a delivered lecture (retried) and enforce the optional window."""
    if _col is None or not message_id:
        return

    now = time.time()

    for attempt in range(3):
        try:
            await _ensure_index()
            await _col.insert_one(
                {
                    "bot_token": bot_token,
                    "bot_id": bot_id,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "file_name": file_name,
                    "status": "delivered",
                    "delivered_at": now,
                    "delete_due_at": now + Var.AUTODEL_TTL_HOURS * 3600,
                    "attempts": 0,
                }
            )
            break
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete record failed (try %s): %s", attempt + 1, error)
            await asyncio.sleep(2)

    max_keep = Var.AUTODEL_MAX_LECTURES
    if max_keep <= 0:
        return

    async with _lock(bot_token):
        stale = (
            await _col.find(
                {"bot_token": bot_token, "status": {"$nin": _DONE}}
            )
            .sort("delivered_at", -1)
            .skip(max_keep)
            .to_list(length=100)
        )
        for doc in stale:
            await _delete_recorded(doc)


async def count_lectures(bot_token: str) -> int:
    if _col is None:
        return 0
    return await _col.count_documents(
        {"bot_token": bot_token, "status": {"$nin": _DONE}}
    )


async def _sweep_once(reason: str = "periodic"):
    """Delete every pending lecture older than the TTL, oldest first."""
    if _col is None:
        return

    async with _sweep_lock:
        ttl_hours = Var.AUTODEL_TTL_HOURS
        cutoff = time.time() - ttl_hours * 3600
        done = failed = 0
        seen = set()

        while True:
            batch = (
                await _col.find(
                    {
                        "status": {"$nin": _DONE},
                        "delivered_at": {"$lt": cutoff},
                        "_id": {"$nin": list(seen)},
                    }
                )
                .sort("delivered_at", 1)
                .to_list(length=200)
            )
            if not batch:
                break

            for doc in batch:
                seen.add(doc["_id"])
                if await _delete_recorded(doc):
                    done += 1
                else:
                    failed += 1
                await asyncio.sleep(0.1)

        if Var.AUTODEL_MAX_LECTURES > 0:
            for bot_token in await _col.distinct(
                "bot_token", {"status": {"$nin": _DONE}}
            ):
                async with _lock(bot_token):
                    stale = (
                        await _col.find(
                            {"bot_token": bot_token, "status": {"$nin": _DONE}}
                        )
                        .sort("delivered_at", -1)
                        .skip(Var.AUTODEL_MAX_LECTURES)
                        .to_list(length=200)
                    )
                    for doc in stale:
                        await _delete_recorded(doc)
                        await asyncio.sleep(0.1)

        if done or failed or reason != "periodic":
            logger.info(
                "Autodelete sweep (%s, TTL=%sh): deleted/closed=%s retry_later=%s",
                reason,
                ttl_hours,
                done,
                failed,
            )


async def _periodic():
    await asyncio.sleep(30)
    reason = "startup"

    while True:
        try:
            await _sweep_once(reason)
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete sweep failed: %s", error)

        reason = "periodic"
        await asyncio.sleep(_SWEEP_INTERVAL)


def _seconds_to_next_scan() -> float:
    now = datetime.now(_IST)
    hours = Var.AUTODEL_SCAN_HOURS or [3]
    candidates = []

    for h in hours:
        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if t <= now:
            t += timedelta(days=1)
        candidates.append(t)

    return (min(candidates) - now).total_seconds()


async def _daily():
    while True:
        await asyncio.sleep(_seconds_to_next_scan() + 1)
        try:
            await _sweep_once("daily-scan")
        except Exception as error:  # noqa: BLE001
            logger.error("Autodelete daily scan failed: %s", error)


def start_sweeper():
    global _sweeper_started
    if _sweeper_started or _col is None:
        return

    _sweeper_started = True
    asyncio.create_task(_periodic())
    asyncio.create_task(_daily())
    logger.info(
        "Autodelete started (TTL=%sh, daily scans IST %s, max=%s per bot)",
        Var.AUTODEL_TTL_HOURS,
        Var.AUTODEL_SCAN_HOURS,
        Var.AUTODEL_MAX_LECTURES,
    )
