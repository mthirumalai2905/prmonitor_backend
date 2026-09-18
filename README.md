# GitHub PR Monitor backend

FastAPI service that receives GitHub webhooks, classifies events with Groq, pushes live updates over WebSockets, and stores history in Supabase.

## Local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Render

1. New Web Service from `https://github.com/mthirumalai2905/prmonitor_backend`
2. Runtime: Python
3. Build: `pip install -r requirements.txt`
4. Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Add the environment variables listed in `.env.example`
6. Run `supabase_setup.sql` in the Supabase SQL editor once
7. Point the GitHub webhook to `https://YOUR-RENDER-SERVICE.onrender.com/github/webhook`

Free Render instances sleep. The first webhook after idle can be slow.
