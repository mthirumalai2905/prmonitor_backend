import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"), override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()
TABLE_URL = f"{SUPABASE_URL}/rest/v1/pr_events"

SETUP_HINT = """
Create this table in the Supabase SQL editor, then restart the backend:

create table if not exists public.pr_events (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  payload jsonb not null
);
alter table public.pr_events enable row level security;
create policy if not exists pr_events_select on public.pr_events for select using (true);
create policy if not exists pr_events_insert on public.pr_events for insert with check (true);
grant select, insert on public.pr_events to anon, authenticated;
grant usage, select on sequence public.pr_events_id_seq to anon, authenticated;
"""


def _headers(extra: dict | None = None) -> dict:
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in backend/.env")
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


def _is_missing_table(response: httpx.Response) -> bool:
    return response.status_code == 404 and "PGRST205" in response.text


def _raise_if_missing(response: httpx.Response) -> None:
    if _is_missing_table(response):
        raise RuntimeError(SETUP_HINT.strip())
    response.raise_for_status()


def init_db() -> None:
    with httpx.Client(timeout=20.0) as client:
        response = client.get(
            TABLE_URL,
            headers=_headers(),
            params={"select": "id", "limit": "1"},
        )
        _raise_if_missing(response)


def save_event(event: dict) -> None:
    with httpx.Client(timeout=20.0) as client:
        response = client.post(
            TABLE_URL,
            headers=_headers({"Prefer": "return=minimal"}),
            json={"payload": event},
        )
        _raise_if_missing(response)


def load_events(limit: int = 1000) -> list[dict]:
    with httpx.Client(timeout=20.0) as client:
        response = client.get(
            TABLE_URL,
            headers=_headers(),
            params={"select": "payload", "order": "id.desc", "limit": str(limit)},
        )
        if _is_missing_table(response):
            return []
        _raise_if_missing(response)
    rows = response.json()
    return [row["payload"] for row in rows]


def event_count() -> int:
    with httpx.Client(timeout=20.0) as client:
        response = client.get(
            TABLE_URL,
            headers=_headers({"Prefer": "count=exact"}),
            params={"select": "id", "limit": "1"},
        )
        if _is_missing_table(response):
            return 0
        _raise_if_missing(response)
    content_range = response.headers.get("content-range", "")
    if "/" in content_range:
        total = content_range.split("/")[-1]
        if total.isdigit():
            return int(total)
    return 0
