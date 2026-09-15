"""One-time cleanup for the SESSION account's chat list.

Archives every private chat with a bot (username ending in 'bot' / is_bot),
so the hundreds of user-bot DMs created before auto-archiving was added get
moved out of the main list. Safe to re-run; already-archived chats are skipped.

Run:  cd /app && python3 archive_existing_bots.py
"""

import os
import asyncio

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["USER_SESSION_STRING"]


async def main():
    app = Client("bulk_archiver", api_id=API_ID, api_hash=API_HASH,
                 session_string=SESSION, in_memory=True, no_updates=True)
    await app.start()

    bot_ids = []
    async for dialog in app.get_dialogs():
        chat = dialog.chat
        if chat.type.value == "bot" or (getattr(chat, "is_bot", False)):
            bot_ids.append(chat.id)

    print(f"Found {len(bot_ids)} bot chats to archive.")
    batch = []
    archived = 0
    for bid in bot_ids:
        batch.append(bid)
        if len(batch) >= 100:
            archived += await _archive(app, batch)
            batch = []
    if batch:
        archived += await _archive(app, batch)

    print(f"Archived {archived} bot chats.")
    await app.stop()


async def _archive(app, ids):
    while True:
        try:
            await app.archive_chats(ids)
            return len(ids)
        except FloodWait as e:
            print(f"FloodWait {e.value}s, sleeping…")
            await asyncio.sleep(e.value + 2)
        except RPCError as e:
            print("archive batch failed:", e)
            return 0


if __name__ == "__main__":
    asyncio.run(main())
