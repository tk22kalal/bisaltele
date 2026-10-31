"""Watch progress stored in MongoDB, keyed by access code + lecture (no Supabase dependency)."""

import hashlib
import logging
import time

import motor.motor_asyncio

from biisal.vars import Var

logger = logging.getLogger("stream.watch_progress")

_col = None


def _collection():
    global _col
    if _col is None and Var.DATABASE_URL:
        client = motor.motor_asyncio.AsyncIOMotorClient(Var.DATABASE_URL)
        _col = client[Var.name].watch_progress
    return _col


def _key(code, lecture_key):
    return hashlib.sha256(f"{code}:{lecture_key}".encode()).hexdigest()


async def get(code, lecture_key):
    col = _collection()
    if col is None or not code or not lecture_key:
        return None
    try:
        doc = await col.find_one({"_id": _key(code, lecture_key)})
    except Exception as error:
        logger.warning("watch_progress get failed: %s", error)
        return None
    if not doc:
        return None
    return {
        "position_seconds": float(doc.get("position_seconds") or 0),
        "duration_seconds": float(doc.get("duration_seconds") or 0),
    }


async def save(code, lecture_key, position, duration):
    col = _collection()
    if col is None or not code or not lecture_key:
        return None
    try:
        await col.update_one(
            {"_id": _key(code, lecture_key)},
            {
                "$set": {
                    "position_seconds": max(0.0, float(position)),
                    "duration_seconds": max(0.0, float(duration)),
                    "updated_at": time.time(),
                }
            },
            upsert=True,
        )
    except Exception as error:
        logger.warning("watch_progress save failed: %s", error)
        return None
    return {"position_seconds": float(position), "duration_seconds": float(duration)}
