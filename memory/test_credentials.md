# Test credentials / data — bisalprosupa2 Telegram delivery

This is a raw pyrogram + aiohttp Telegram bot (NOT the preview React/FastAPI stack).

## Run the app (from repo ROOT /app)
```
cd /app && ./.venv/bin/python -m biisal
```
- Serves HTTP on PORT from /app/.env (currently 8899). Health: `curl http://127.0.0.1:8899/`
- /app/.env holds all secrets (git-ignored): API_ID/HASH, BOT_TOKEN, BIN_CHANNEL,
  DB_CHANNEL, SUPABASE_URL/KEY, USER_SESSION_STRING, PORT=8899.

## Telegram-delivery test data (live)
- Endpoint: `GET http://127.0.0.1:8899/api/telegram/{token}?access_code={code}`
- Valid temp token (in Mongo temp_files): `VhkJDs-2a_XjirHVo1-fkg`
  (msg 309278 Bates video) and `wLhCaAHWKNDRkAQjQwtHFQ` (BIN msg 1628310,
  "Approach to differentiating lesions brainstem", domain webx)
- Valid access codes (Supabase, active): `VLT5FHQARL` (user 498b007b, bot
  8993867611) and `3HMGK6GMRA` (user d014beb9, bot @Sudhrbnzodus_bot
  8815264788:AAFqBR0PVFrfYLce2YOGpCs-2GHeO9I1-Ik, owner chat 6147509071)
- Recipient user bot: @Sndnndnnd_bot (token 8993867611:...) — already a manual admin
  of BIN_CHANNEL, owner @MichaelAnderson266 (id 8495837104) has /start-ed it.
- Expected success JSON: {"success": true, "message": "Video sent to your Telegram bot.", ...}
- Invalid/missing access_code -> HTTP 403.

## DM-relay (2026-09) — admin rotation REMOVED
- Delivery no longer promotes/demotes user bots. User session copies the source
  message into the bot's DM; the bot copies it to its owner (protect_content ON)
  and deletes the DM copy. No admin cap, no fresh-session constraint.
- Verified live with code 3HMGK6GMRA + video 1628310: delivered twice, zero
  FloodWait, DM left clean.
