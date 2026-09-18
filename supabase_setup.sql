-- Paste this in Supabase: SQL Editor -> Run

create table if not exists public.pr_events (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  payload jsonb not null
);

alter table public.pr_events enable row level security;

drop policy if exists pr_events_select on public.pr_events;
drop policy if exists pr_events_insert on public.pr_events;

create policy pr_events_select on public.pr_events for select using (true);
create policy pr_events_insert on public.pr_events for insert with check (true);

grant select, insert on public.pr_events to anon, authenticated;
grant usage, select on sequence public.pr_events_id_seq to anon, authenticated;
