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
- Valid access code (Supabase, active): `VLT5FHQARL` (belongs to user 498b007b who has bot 8993867611 configured; older code AKAVLZ6SK3 has expired via 24h TTL)
- Recipient user bot: @Sndnndnnd_bot (token 8993867611:...) — already a manual admin
  of BIN_CHANNEL, owner @MichaelAnderson266 (id 8495837104) has /start-ed it.
- Expected success JSON: {"success": true, "message": "Video sent to your Telegram bot.", ...}
- Invalid/missing access_code -> HTTP 403.

## Known constraint
- Auto promote/demote (admin rotation) needs the USER_SESSION_STRING account to be
  >24h old (Telegram FRESH_CHANGE_ADMINS_FORBIDDEN). Until then only bots already
  admin of BIN_CHANNEL (like @Sndnndnnd_bot) can be tested end-to-end.
