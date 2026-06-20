# Schema Moderator Service (v2)

The always-on droplet authority for warehouse schema review. Wraps the phase-1 deterministic
engine (`core/schema_gate_lib.py`) as one service + an append-only approval ledger in Postgres,
and is the apply-time chokepoint: a DDL only lands if a content-hash-bound `moderator.approval_ledger`
row says it passed.

Spec: `../deliverables/2026-06-19-db-review-moderator-service/BUILD-SPEC-v2.md` (in the parent
Renaissance repo). Phase-1 substrate: branch `schema-gate-phase1`.

## Layout
- `bin/moderator_server.py` — Starlette + uvicorn service, `127.0.0.1:8901`, behind the Tailscale
  Funnel at `/moderator/*`. Two-layer gate: deterministic floor (free) + server-side LLM deep-review
  (paid, fail-closed). Endpoints: `/healthz` `/review` `/record-pass` `/rules` `/catalog` `/ledger`
  `/issues` `/judge-advisory`.
- `bin/moderator_common.py` — config, scoped bearer-token auth, Postgres + read-API helpers.
- `systemd/schema-moderator.service` — the unit (reuses `/opt/duckdb/venv`).
- `deploy.sh` — scp code + vendored engine + unit to the droplet (`renaissance-worker`).

## Store
- **Catalog/lineage** stays in DuckDB (`core.schema_catalog/_consumers/_aliases`), rebuilt nightly.
- **Rules + alias authority + approval ledger + issue ledger** live in Postgres schema `moderator`
  in the PIPELINE project `nmkaydqcnkjsehyqokgg` (Supavisor 6543). Append-only trigger on the ledger.

## Auth & scopes
`/opt/duckdb/allowed_tokens.txt` — `token<TAB>email[<TAB>scope]`, reloaded per request. Scope is a
3rd column; lines without it default to `reader`. `reader` ⊂ `editor` ⊂ `admin`.
- `reader` — `/catalog` `/ledger` `/issues` `/rules` (GET)
- `editor` — `/review` `/record-pass` `/judge-advisory` `/apply/enqueue` `/apply/process`
- `admin`  — `POST /rules`

`/healthz` is unauthenticated.

## Secrets — `/opt/moderator/moderator.env` (chmod 600; NOT in git, NOT created by deploy.sh)
```
MODERATOR_HOST=127.0.0.1
MODERATOR_PORT=8901
MODERATOR_ALLOWED_TOKENS=/opt/duckdb/allowed_tokens.txt
MODERATOR_ENGINE_DIR=/opt/moderator/engine
WAREHOUSE_REPO_ROOT=/root/renaissance-warehouse
MODERATOR_PG_DSN=postgresql://...:6543/postgres        # = PIPELINE_SUPABASE_DB_URL
WAREHOUSE_API_URL=https://renaissance-droplet.tailae5c80.ts.net
WAREHOUSE_API_TOKEN=<a reader token from allowed_tokens.txt>   # for /catalog (P2)
ANTHROPIC_API_KEY=<metered console key>                # server-side LLM deep-review (§5)
SCHEMA_GATE_SERVER_LLM=1
MODERATOR_LLM_MODEL=claude-opus-4-8
MODERATOR_LOG=/opt/moderator/logs/moderator.jsonl
```

## Deploy / operate
```
./deploy.sh                                    # scp code+engine+unit to /opt/moderator
ssh renaissance-worker 'systemctl enable --now schema-moderator'   # first time
curl -s http://127.0.0.1:8901/healthz          # on the droplet
curl -s https://renaissance-droplet.tailae5c80.ts.net/moderator/healthz   # via funnel
```
Funnel path mount (one-time): `tailscale serve --bg --https=443 --set-path /moderator http://127.0.0.1:8901`.
The service path-normalises, so it works whether Tailscale strips the `/moderator` prefix or not.

## venv deps (in `/opt/duckdb/venv`)
`starlette uvicorn httpx duckdb` (present) + `psycopg[binary]`, `sqlglot>=25`, `anthropic` (added).
