-- ============================================================================
-- Personal-bot delivery: schema + RPCs for the Telegram delivery option.
-- Run this ONCE in Supabase (SQL Editor). Safe to re-run (idempotent).
--
-- The bot backend uses the anon/publishable key, so RLS blocks direct table
-- reads. These SECURITY DEFINER functions expose exactly what the bot needs.
-- ============================================================================

-- 1) Where the user's own bot token + delivery chat id live.
--    The frontend saves the bot token into profiles.telegram_bot_token.
alter table public.profiles
    add column if not exists telegram_bot_token text,
    add column if not exists telegram_chat_id   text;

-- 2) Read the delivery config for the user tied to an access code.
create or replace function public.get_media_delivery_config(p_code text)
returns table(user_id uuid, bot_token text, chat_id text)
language sql
security definer
set search_path = public
as $$
    select p.id, p.telegram_bot_token, p.telegram_chat_id
    from   media_access_codes c
    join   profiles p on p.id = c.user_id
    where  c.code = p_code
      and  coalesce(c.active, true) = true
    limit  1;
$$;

-- 3) Cache the discovered delivery chat id (first time the user's bot is used).
create or replace function public.set_media_delivery_chat(p_code text, p_chat_id text)
returns void
language sql
security definer
set search_path = public
as $$
    update public.profiles
    set    telegram_chat_id = p_chat_id
    where  id = (select user_id from public.media_access_codes where code = p_code limit 1);
$$;

grant execute on function public.get_media_delivery_config(text)        to anon, authenticated;
grant execute on function public.set_media_delivery_chat(text, text)     to anon, authenticated;

-- 4) Allow a signed-in user to save their OWN bot token from the frontend.
--    Run this only if saving the token in the PWA fails with a permission error.
alter table public.profiles enable row level security;
drop policy if exists "profiles_update_own_tg_token" on public.profiles;
create policy "profiles_update_own_tg_token" on public.profiles
    for update to authenticated
    using (auth.uid() = id)
    with check (auth.uid() = id);
