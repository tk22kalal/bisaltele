# PRD — Personal Telegram Bot Delivery (bisalprosupa2)

## Problem statement
Add a 3rd delivery option (alongside Web / WebX streaming links) to the Telegram
file-stream bot: a **Telegram** option that delivers the requested video directly
into the user's OWN Telegram bot with **Protect Content ON**, so the main
DB_CHANNEL and main bot stay hidden.

Repo: https://github.com/tk22kalal/bisalprosupa2 (cloned at /app/repo)
Frontend PWA (separate repo, user-owned): sunday2212/WEBREPLITX5 stream-player.html

## Architecture / flow
1. Frontend calls `GET /api/telegram/{token}?access_code=CODE` (same token/code
   contract as the existing `/api/generate` and `/api/download`).
2. Backend resolves access_code -> user via Supabase RPC, then fetches the user's
   bot token via `get_media_delivery_config` (SECURITY DEFINER RPC; anon key +
   RLS cannot read tables directly).
3. Destination chat id: if not saved, discovered from the user's bot `getUpdates`
   (owner must have pressed /start once) and cached via `set_media_delivery_chat`.
4. A Telegram USER session (USER_SESSION_STRING) promotes the user's bot to full
   admin of BIN_CHANNEL (once). If the bot is already admin, promotion is skipped.
5. Main bot copies the source message DB_CHANNEL -> BIN_CHANNEL (as before).
6. The user's own bot copies that BIN_CHANNEL message to the user with
   `protect_content=True` via the Telegram Bot HTTP API (copyMessage).

## Implemented (2026-06)
- `biisal/utils/telegram_delivery.py` (new): Supabase RPC helpers, user-bot HTTP
  helpers (getMe/getUpdates/copyMessage), and BIN_CHANNEL auto-promotion via user
  session (idempotent, skips if already admin).
- `biisal/server/stream_routes.py`: new route `GET /api/telegram/{token}`; moved
  the catch-all `/{path:.+}` route to the end so literal routes register first.
- `biisal/server/__init__.py`: `/api/telegram/` added to access-code-protected
  prefixes.
- `biisal/vars.py`: new `USER_SESSION_STRING` env var.
- `SUPABASE_SETUP.sql` (new): adds `profiles.telegram_bot_token` +
  `profiles.telegram_chat_id` and the two RPCs.

## Live test result (verified)
- Delivered real video (msg 309278 in DB_CHANNEL) to user bot @Sndnndnnd_bot ->
  account @MichaelAnderson266, protect_content ON. `success: true`.
- Repeat delivery OK; invalid/missing access_code -> 403.

## Deployment notes / requirements
- New env var: `USER_SESSION_STRING` = pyrofork session string of an account that
  is admin/owner of BIN_CHANNEL.
- Telegram limits: a freshly logged-in session cannot promote admins for ~24h
  (FRESH_CHANGE_ADMINS_FORBIDDEN). Until then, add each user bot to BIN_CHANNEL
  as admin manually once; after 24h auto-promote works for new bots.
- Frontend must save the user's bot token into `profiles.telegram_bot_token`.
- Owner of each personal bot must press /start on it once (so the bot can DM them).

## Backlog / next
- Frontend: add "Send to my Telegram bot" button in stream-player.html calling
  `/api/telegram/{token}?access_code=...` and showing the returned message.
- Optional: store telegram_chat_id explicitly at bot-setup time instead of
  getUpdates discovery.

## Frontend two-tab UI + session env (2026-06)
- `frontend/stream-player.html` (copy for the user's PWA repo) now has TWO tabs:
  - **Stream Online** (default active): unchanged — embedded video, WATCH-S2,
    DOWNLOAD-S1/S2, Next Lectures; Stream(10)/Download(5) limits untouched.
  - **Get File In Telegram**: inline panel. New user -> bot-token input + a
    "How to get bot token?" modal (4 image steps, placeholder images to be
    replaced). Token saved to `profiles.telegram_bot_token` via the PWA's
    Google-auth Supabase session; returning users are auto-detected and get a
    "Send to my Telegram" button. Telegram delivery has NO daily limit.
- Verified (iteration_3): backend 4/4 + frontend 5/5, no issues. Admin rotation
  (promote->deliver->demote) is now live (fresh-session 24h window cleared).
- `USER_SESSION_STRING` persisted in `/app/.env`; also handed to the user to add
  to their VPS env so no OTP/login is ever needed again.
- Telegram channels allow **max 50 admins total** (bots included) — cannot keep
  thousands of user bots as permanent admins.
- Implemented rotation in `telegram_delivery.py`: `acquire_bin_admin` (promote to
  full admin, bounded by `MAX_BIN_ADMIN_BOTS=45` via an async semaphore) →
  `deliver_via_user_bot` (copyMessage, protect_content ON) → `release_bin_admin`
  (demote = remove from channel) in the route's `finally`. Bots already admin
  (manually added during the fresh-session window) take an "already" path and are
  not auto-removed.
- Safety-net sweeper demotes any WE-promoted bot lingering > 15 min.
- Best-effort `schedule_message_deletion`: deletes the delivered video from the
  user's bot after 24h (in-process timer; lost on restart).
- Constraint: promote/demote need the USER_SESSION_STRING account to be logged in
  > ~24h (FRESH_CHANGE_ADMINS_FORBIDDEN). Until then, add each user bot to
  BIN_CHANNEL as admin manually. Verified: token 8952076926 (@Ejehfbdbsnsb_bot)
  is valid but auto-promote is blocked while the session is fresh.

## copyMessage-without-admin experiments (2026-09) — scripts test_copymessage*.py
Live tests with USER_SESSION account @D1234abcesb, main bot @streamtedt_bot,
test user-bot @streaammmmvps_bot (MULTI_TOKEN1). Reports:
/app/test_reports/copymessage_findings{,_r2,_r3}.json

- FAIL T1: bots can NEVER be plain channel members ("Bots in channels can only
  be administrators, not members", 400 USER_BOT). Channel-member idea is dead.
- FAIL: copyMessage from a supergroup as plain member (privacy ON) -> "message
  to copy not found". Bots can only copy messages they have RECEIVED in their
  own update stream (or sent themselves, T8d PASS).
- FAIL T7: bots cannot self-join groups via invite link (BOT_METHOD_INVALID,
  messages.ImportChatInvite). A user must add them.
- PASS: main bot copies DB_CHANNEL -> supergroup as a plain member.
- PASS T6: main bot can PROMOTE/DEMOTE user bots in a supergroup via pure Bot
  API (no user session needed).
- PASS T8e/f: a bot that is ADMIN in a supergroup receives all subsequent
  messages and can copyMessage them even with privacy mode ON (messages must be
  sent AFTER promotion).
- PASS T9 (WINNER — DM-relay): user session copies BIN_CHANNEL msg into the
  user bot's DM (1 plain MTProto copy op, NO admin anywhere) -> bot finds it
  via getUpdates -> bot copyMessage(DM -> owner, protect_content=True) -> works.
  Bot can deleteMessage the DM copy afterwards (bots may delete any message in
  their own private chats).
- Recommended next implementation: replace acquire/release_bin_admin with
  DM-relay (session copy -> getUpdates -> copyMessage -> deleteMessage).
  Removes 50-admin cap, admin-op churn, sweeper; 1 session op per delivery
  instead of 2 privileged ones. Risks: space session sends (FloodWait/PEER_FLOOD
  if DMing many new bots), getUpdates conflicts with webhooks on user bots,
  DM copy must stay unprotected (protected source can't be re-copied).

## DM-relay IMPLEMENTED (2026-09)
- `telegram_delivery.py` rewritten: rotation/sweeper/semaphore removed. New
  `deliver_via_dm_relay()`: session `copy_message(DB/BIN -> bot DM)` ->
  per-bot update-cursor `_poll_updates` (drains backlog via moving offset, so
  bots that are admins of busy channels can't flood the queue) -> bot
  `copyMessage(DM -> owner, protect_content=True)` -> bot deletes DM copy.
  `_flood_safe` sleeps through FloodWait; per-bot asyncio.Lock serializes
  deliveries; resolved peers and session account id cached in-process.
- `stream_routes.py` `/api/telegram/{token}`: no more main-bot BIN copy, no
  acquire/release — a single `deliver_via_dm_relay` call.
- Live verified (pyrofork 2.3.69 restored after pyromod pulled vanilla
  pyrogram 2.0.106): access code 3HMGK6GMRA, video BIN msg 1628310
  (Approach_to_differentiating_lesions_brainstem, 50MB), bot @Sudhrbnzodus_bot
  -> owner 6147509071. Delivered 2x back-to-back, no FloodWait, no admin ops,
  DM clean afterwards, bad code -> 403.
- Deployment: VPS just needs updated code + USER_SESSION_STRING; bots no longer
  need to be admins anywhere.
