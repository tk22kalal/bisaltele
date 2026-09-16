"""Restart-proof per-user daily quota, keyed on the real lecture identity.

Counts DISTINCT lectures per user per rolling 24h window, per action
(stream / download / telegram). Re-watching or re-downloading the SAME lecture
within the window is free (idempotent) — only new distinct lectures consume a
slot. The lecture identity is derived server-side from the Telegram file's
unique id, so tampering with the URL's lecture_key / id cannot bypass the cap.

State lives in MongoDB (main app cluster), so limits survive process restarts.
"""

import time
import asyncio
import logging

import motor.motor_asyncio

from biisal.vars import Var

logger = logging.getLogger("stream.daily_quota")

_WINDOW_SECONDS = 24 * 60 * 60

_client = None
_col = None
_index_ready = False
_locks: dict = {}


def _collection():
    global _client, _col
    if _col is not None:
        return _col
    uri = Var.DATABASE_URL
    if not uri:
        return None
    _client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    _col = _client[Var.name].media_daily_quota
    return _col


def _lock(user_id: str, action: str) -> asyncio.Lock:
    key = f"{user_id}:{action}"
    lock = _locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _locks[key] = lock
    return lock


async def _ensure_index(col):
    global _index_ready
    if _index_ready:
        return
    await col.create_index(
        [("user_id", 1), ("action", 1), ("lecture_id", 1)], unique=True
    )
    await col.create_index([("user_id", 1), ("action", 1), ("last_at", -1)])
    _index_ready = True


async def claim(user_id: str, action: str, lecture_id: str, limit: int):
    """Reserve one distinct-lecture slot for the current 24h window.

    Returns (allowed: bool, reason: str|None, created_new: bool):
      allowed=True  -> request may proceed.
      reason='daily_limit' when the distinct-lecture cap is hit.
      created_new=True only when this call first counted the lecture this
        window (so a failed delivery can refund it via `unclaim`).
    """
    col = _collection()
    if col is None or not user_id or not lecture_id:
        return True, None, False  # fail open if storage is unavailable
    await _ensure_index(col)

    now = time.time()
    window_start = now - _WINDOW_SECONDS
    async with _lock(user_id, action):
        existing = await col.find_one(
            {"user_id": user_id, "action": action, "lecture_id": lecture_id}
        )
        if existing and existing.get("last_at", 0) >= window_start:
            await col.update_one({"_id": existing["_id"]}, {"$set": {"last_at": now}})
            return True, None, False  # already counted this window — free

        distinct = await col.count_documents({
            "user_id": user_id,
            "action": action,
            "last_at": {"$gte": window_start},
        })
        if distinct >= limit:
            return False, "daily_limit", False

        await col.update_one(
            {"user_id": user_id, "action": action, "lecture_id": lecture_id},
            {"$set": {"last_at": now}, "$setOnInsert": {"first_at": now}},
            upsert=True,
        )
        return True, None, True


async def unclaim(user_id: str, action: str, lecture_id: str):
    """Refund a slot claimed for a request that ultimately failed."""
    col = _collection()
    if col is None:
        return
    try:
        await col.delete_one(
            {"user_id": user_id, "action": action, "lecture_id": lecture_id}
        )
    except Exception as error:  # noqa: BLE001
        logger.warning("daily_quota unclaim failed: %s", error)
