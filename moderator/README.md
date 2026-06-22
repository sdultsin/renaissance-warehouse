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
  `/issues` `/judge-advisory` `/apply/enqueue` `/apply/queue` `/apply/process` `/apply-now`.
- `bin/moderator_apply.py` — the ON-DEMAND substrate apply ("apply-now"): physically applies the
  ledger-approved enqueued DDLs to the LIVE warehouse under the warehouse-writer flock (the SAME
  tooth the nightly uses — `core.db.apply_ddl_file`, content-hash-bound to `moderator.approval_ledger`),
  then re-promotes the serving snapshot via the ONE promote mechanism (`/opt/duckdb/bin/publisher.py`)
  so the change is visible to readers in minutes instead of waiting for the ~03:30 UTC nightly.
  Flock-safe: queues behind the nightly / a running promote, never clobbers; fail-closed + reversible.
- `bin/moderator_common.py` — config, scoped bearer-token auth, Postgres + read-API helpers.
- `systemd/schema-moderator.service` — the unit (reuses `/opt/duckdb/venv`).
- `deploy.sh` — scp code + vendored engine + unit to the droplet (`renaissance-worker`).

## Store
- **Catalog/lineage** stays in DuckDB (`core.schema_catalog/_consumers/_aliases`), rebuilt nightly.
- **Rules + alias authority + approval ledger + issue ledger + version reservations** live in Postgres
  schema `moderator` in the PIPELINE project `nmkaydqcnkjsehyqokgg` (Supavisor 6543). Append-only
  trigger on the ledger. `moderator.version_reservation` (PK `version`) is the auto-assign allocator's
  reservation table (created idempotently on first `/apply/next-version`).

## Auth & scopes
`/opt/duckdb/allowed_tokens.txt` — `token<TAB>email[<TAB>scope]`, reloaded per request. Scope is a
3rd column; lines without it default to `reader`. `reader` ⊂ `editor` ⊂ `admin`.
- `reader` — `/catalog` `/ledger` `/issues` `/rules` (GET) `/apply/queue`
- `editor` — `/review` `/record-pass` `/judge-advisory` `/apply/enqueue` `/apply/next-version` `/apply/process` `/apply-now`
- `admin`  — `POST /rules`

`/healthz` is unauthenticated.

## The one-command writer front door (`SHIP-FLOW.md`)
Writers (David / Darcy / Thomas / Sam) and their agents use ONE invisible flow — they say "review and
ship these changes" once and the agent runs: `next-version` → gate review + auto-revise (escalate only
on a destructive/ambiguous call) → commit → PR → auto-merge → box `git pull --ff-only` →
`apply-now --pull-first` (single-writer-flocked apply + promote). The writer never sees a branch, picks
a version, or hand-coordinates concurrency. **Canonical procedure: `moderator/SHIP-FLOW.md`** (also the
`warehouse-ship` skill). The hardened gate below is what makes that final carry-through safe to automate.

## Auto-assigned DDL numbers (`/apply/next-version`) — no more collisions
DDL `version` numbers used to be hand-picked from the filename prefix; a writer on a STALE local
checkout could pick a number already taken (the 2026-06-22 v96 incident: applied as 96 while the repo
was at 113). **Always get your number from the service — the single authority across every writer
(human or agent), unlike the bus-local `ddl-number` lock a laptop bypasses:**
```
python scripts/moderator_client.py next-version    # reserves + prints the next free number
```
`next = 1 + max(committed sql/ddl at origin/main, applied core.schema_version, in-flight queue+ledger,
prior reservations)`, reserved atomically in `moderator.version_reservation` (never reuses a gap).
Name your new file `sql/ddl/<that-number>_<name>.sql`.

## Real-time apply (`/apply-now`) — the two-speed apply model
A recorded DDL applies on the **nightly (~03:30 UTC) by default**. To make it live in **minutes**:
```
python scripts/moderator_client.py next-version                             # reserve the DDL number
python scripts/moderator_client.py apply-enqueue --files sql/ddl/NN_x.sql   # if not already queued
# commit the file -> PR -> merge to origin/main -> box pull   (apply==commit; see below)
python scripts/moderator_client.py apply-now                                # apply live + re-promote
```
**`apply == commit` (refuse-if-uncommitted):** apply-now REFUSES a DDL whose exact content isn't
committed at `origin/main:sql/ddl/<file>` (`MODERATOR_REQUIRE_COMMITTED=enforce`, default; `warn` =
apply-but-flag, `off` = skip for local dev). So the live DB can never get ahead of the repo — commit
+ PR + merge + box-pull FIRST. It also REFUSES a version-number collision (a number already applied
under a different file) instead of silently no-op'ing the writer's DDL.
`apply-now` acquires the warehouse-writer flock, applies every queued DDL that has a content-hash-bound
`approval_ledger` pass (the same authority + tooth the nightly uses), then re-promotes the serving
snapshot so **readers** see it. **Latency:** the promote copies the ~50GB warehouse, so it takes
**several minutes (~10)** — acceptable for an on-demand "make it live now". There is no lighter correct
refresh: readers open the whole snapshot file via the `warehouse_current.duckdb` symlink, so visibility
requires a full snapshot promote (the publisher's own `LOCK_NB` guard prevents two concurrent promotes;
if one is already running, apply-now reports the apply landed + promote-busy and you re-run to confirm).

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
# apply==commit (refuse-if-uncommitted) — default ON. enforce | warn | off.
MODERATOR_REQUIRE_COMMITTED=enforce
MODERATOR_APPLY_GIT_REF=origin/main     # the authoritative code ref the apply must match
MODERATOR_APPLY_FETCH=1                  # best-effort `git fetch` before the commit check (see a just-merged PR)
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
