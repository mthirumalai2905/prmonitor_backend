import asyncio
import json
import logging
import os
import sqlite3
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from store import event_count, init_db, load_events, save_event

load_dotenv(Path(__file__).with_name(".env"), override=True)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b").strip()
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

HANDLED_EVENTS = {
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "pull_request_review_thread",
    "issue_comment",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("github-pr-monitor")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

connected_clients: list[WebSocket] = []
live_events: list[dict] = []
pending_supabase: list[dict] = []
GITHUB_REPO = os.getenv("GITHUB_REPO", "mthirumalai2905/weatherapp").strip()
SQLITE_PATH = Path(__file__).with_name("events.db")


@app.get("/")
async def health():
    stored = 0
    try:
        stored = event_count()
    except Exception:
        logger.exception("Health could not read stored events")
    return {"status": "ok", "groq": bool(GROQ_API_KEY), "stored_events": stored}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    logger.info("Frontend connected. clients=%s", len(connected_clients))
    try:
        snapshot = load_events()
        if not snapshot:
            snapshot = list(live_events)
    except Exception:
        logger.exception("Could not load stored events")
        snapshot = list(live_events)
    await websocket.send_json({"type": "snapshot", "events": snapshot})
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=20)
            except TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        logger.info("Frontend disconnected. clients=%s", max(len(connected_clients) - 1, 0))
    except Exception:
        logger.exception("WebSocket error")
    finally:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


async def persist_to_supabase(event: dict) -> bool:
    try:
        save_event(event)
        return True
    except Exception:
        logger.warning("Supabase persist deferred until pr_events exists")
        pending_supabase.append(event)
        return False


async def flush_pending_supabase() -> None:
    if not pending_supabase:
        return
    leftover: list[dict] = []
    for event in pending_supabase:
        try:
            save_event(event)
        except Exception:
            leftover.append(event)
    pending_supabase[:] = leftover
    if not leftover:
        logger.info("Flushed historical events to Supabase")


async def supabase_retry_loop() -> None:
    while True:
        await asyncio.sleep(15)
        await flush_pending_supabase()


async def broadcast_to_ui(event: dict) -> None:
    live_events.insert(0, event)
    stale: list[WebSocket] = []
    for client in connected_clients:
        try:
            await client.send_json(event)
        except Exception:
            stale.append(client)
    for client in stale:
        if client in connected_clients:
            connected_clients.remove(client)


async def send_to_frontends(event: dict) -> None:
    # Real time: webhook hits the UI first, then Supabase.
    await broadcast_to_ui(event)
    asyncio.create_task(persist_to_supabase(event))


def login(user: dict | None) -> str | None:
    return (user or {}).get("login")


def parse_github_event(event_name: str, payload: dict) -> dict | None:
    """Turn a GitHub webhook into one flat object the UI can show."""
    pull_request = payload.get("pull_request") or {}
    repository = payload.get("repository") or {}
    action = payload.get("action")

    event = {
        "event_type": event_name,
        "repository": repository.get("full_name"),
        "number": payload.get("number") or pull_request.get("number"),
        "title": pull_request.get("title"),
        "author": login(pull_request.get("user")),
        "action": action,
        "url": pull_request.get("html_url"),
        "detail": None,
        "review_state": None,
        "file_path": None,
        "actor": None,
    }

    if event_name == "pull_request":
        event["actor"] = login(payload.get("sender")) or event["author"]
        event["detail"] = (pull_request.get("body") or "")[:500] or None
        return event

    if event_name == "pull_request_review":
        review = payload.get("review") or {}
        event["actor"] = login(review.get("user")) or login(payload.get("sender"))
        event["review_state"] = review.get("state")
        event["url"] = review.get("html_url") or event["url"]
        event["detail"] = (review.get("body") or "")[:500] or None
        return event

    if event_name == "pull_request_review_comment":
        comment = payload.get("comment") or {}
        event["actor"] = login(comment.get("user")) or login(payload.get("sender"))
        event["url"] = comment.get("html_url") or event["url"]
        event["file_path"] = comment.get("path")
        event["detail"] = (comment.get("body") or "")[:500] or None
        return event

    if event_name == "pull_request_review_thread":
        thread = payload.get("thread") or {}
        comments = thread.get("comments") or []
        last_comment = comments[-1] if comments else {}
        event["actor"] = login(payload.get("sender"))
        event["file_path"] = last_comment.get("path")
        event["url"] = last_comment.get("html_url") or event["url"]
        event["detail"] = (last_comment.get("body") or "")[:500] or f"Thread {action}"
        return event

    if event_name == "issue_comment":
        issue = payload.get("issue") or {}
        if not issue.get("pull_request"):
            return None
        comment = payload.get("comment") or {}
        event["number"] = issue.get("number")
        event["title"] = issue.get("title")
        event["author"] = login(issue.get("user"))
        event["actor"] = login(comment.get("user")) or login(payload.get("sender"))
        event["url"] = comment.get("html_url") or issue.get("html_url")
        event["detail"] = (comment.get("body") or "")[:500] or None
        event["event_type"] = "issue_comment"
        return event

    return None


async def classify_event(event: dict) -> dict:
    empty = {
        "kind": "unknown",
        "summary": "Could not classify this event.",
        "suggestion": "Open it on GitHub.",
    }

    if not GROQ_API_KEY:
        empty["summary"] = "GROQ_API_KEY is not set in backend/.env"
        return empty

    system_prompt = (
        "You classify GitHub pull request activity for a monitor UI. "
        "Return JSON only with keys kind, summary, suggestion. "
        "kind must be one of: feature, bugfix, docs, refactor, chore, test, "
        "breaking, hotfix, approval, changes_requested, comment, discussion, unknown. "
        "summary is 1 or 2 short sentences. "
        "suggestion is a practical next step: raise for review, merge, reply, "
        "request changes, resolve the thread, or wait. "
        "Do not invent diffs you were not given."
    )

    logger.info("Classifying %s with Groq model %s", event.get("event_type"), GROQ_MODEL)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                GROQ_URL,
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": GROQ_MODEL,
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": json.dumps(event)},
                    ],
                },
            )
        if response.status_code >= 400:
            logger.error("Groq HTTP %s: %s", response.status_code, response.text[:500])
            response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        return {
            "kind": data.get("kind") or "unknown",
            "summary": data.get("summary") or empty["summary"],
            "suggestion": data.get("suggestion") or empty["suggestion"],
        }
    except Exception:
        logger.exception("Groq classification failed")
        empty["summary"] = "Groq classification failed. Showing the raw event only."
        return empty


async def backfill_from_github() -> None:
    """Load existing PRs and conversation comments if nothing is on screen yet."""
    if live_events or not GITHUB_REPO:
        return

    logger.info("No events yet. Loading history from GitHub repo %s", GITHUB_REPO)
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "github-pr-monitor"}
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        pulls_response = await client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/pulls",
            params={"state": "all", "per_page": 50, "sort": "created", "direction": "asc"},
        )
        pulls_response.raise_for_status()
        pulls = pulls_response.json()

        for pull in pulls:
            event = {
                "event_type": "pull_request",
                "repository": GITHUB_REPO,
                "number": pull.get("number"),
                "title": pull.get("title"),
                "author": login(pull.get("user")),
                "action": "opened",
                "url": pull.get("html_url"),
                "detail": (pull.get("body") or "")[:500] or None,
                "review_state": None,
                "file_path": None,
                "actor": login(pull.get("user")),
            }
            event.update(await classify_event(event))
            live_events.insert(0, event)
            pending_supabase.append(event)

            comments_response = await client.get(
                f"https://api.github.com/repos/{GITHUB_REPO}/issues/{pull.get('number')}/comments"
            )
            comments_response.raise_for_status()
            for comment in comments_response.json():
                comment_event = {
                    "event_type": "issue_comment",
                    "repository": GITHUB_REPO,
                    "number": pull.get("number"),
                    "title": pull.get("title"),
                    "author": login(pull.get("user")),
                    "action": "created",
                    "url": comment.get("html_url"),
                    "detail": (comment.get("body") or "")[:500] or None,
                    "review_state": None,
                    "file_path": None,
                    "actor": login(comment.get("user")),
                }
                comment_event.update(await classify_event(comment_event))
                live_events.insert(0, comment_event)
                pending_supabase.append(comment_event)

    logger.info("Loaded %s events from GitHub history", len(live_events))


def load_sqlite_events() -> list[dict]:
    if not SQLITE_PATH.exists():
        return []
    with sqlite3.connect(SQLITE_PATH) as conn:
        rows = conn.execute(
            "SELECT payload FROM events ORDER BY id DESC"
        ).fetchall()
    events = []
    for row in rows:
        payload = row[0]
        events.append(json.loads(payload) if isinstance(payload, str) else payload)
    return events


@app.on_event("startup")
async def load_history() -> None:
    live_events.extend(load_sqlite_events())
    if live_events:
        logger.info("Restored %s historical events locally", len(live_events))
        pending_supabase.extend(reversed(list(live_events)))
    try:
        init_db()
        await flush_pending_supabase()
    except Exception:
        logger.warning("Supabase table is not ready yet. History will upload after pr_events exists.")
    try:
        await backfill_from_github()
    except Exception:
        logger.exception("Could not load GitHub history")
    asyncio.create_task(supabase_retry_loop())


@app.post("/github/webhook")
async def github_webhook(
    request: Request,
    x_github_event: str | None = Header(default=None),
):
    payload = await request.json()
    event_name = x_github_event or ""

    if event_name == "ping":
        logger.info("GitHub webhook ping received")
        return {"ok": True, "event": "ping"}

    if event_name not in HANDLED_EVENTS:
        logger.info("Ignored GitHub event: %s", event_name or "(missing X-GitHub-Event)")
        return {"ok": True, "ignored": event_name or None}

    event = parse_github_event(event_name, payload)
    if event is None:
        return {"ok": True, "ignored": event_name}

    decision = await classify_event(event)
    event.update(decision)

    logger.info("GitHub event: %s", event)
    await send_to_frontends(event)
    return {"ok": True, "event": event}
