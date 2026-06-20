---
name: warehouse-access
description: The single source of truth for ACCESSING the Renaissance DuckDB warehouse — both READ (query API) and WRITE (the moderator / schema-editor path). Load this when you (or the person whose Claude you are) need to read OR edit the warehouse and setup isn't working, when a writer (David / Darcy / Thomas / Sam) is being onboarded to edit DDL/views/columns/syncs, or when you hit any "SSH connection error", "tailscale?", "need admin?", "only queues for nightly", or "how do I get write access" confusion. Run `doctor` FIRST. Supersedes the scattered WRITER-ONBOARDING / sending-volume / handoff docs. Self-contained and shareable.
---

# warehouse-access — the ONE way to read AND write the Renaissance DuckDB warehouse

This is the **single source of truth** for warehouse access. If you are setting up to **edit** the
warehouse and it isn't working, **do not** read the old onboarding docs and do not guess — run the
**doctor** (Section 2) and follow exactly what it says.

## TL;DR (the whole mental model)

- The warehouse lives on a droplet. You reach it over **plain public HTTPS** at
  **`https://renaissance-droplet.tailae5c80.ts.net`** (a Tailscale **Funnel** = a public URL with a
  real cert). **NO Tailscale client, NO VPN, NO SSH is needed** for either reading or writing.
- There are **two** things you can do, each with its **own token**:
  - **READ** → `POST /query` with a **reader** token (the `/data-warehouse` skill). Ungated for the
    team; anyone can read.
  - **WRITE / EDIT schema** → author files in a clone of the **renaissance-warehouse** repo, then
    `python scripts/moderator_client.py loop --files <paths>` with your personal **editor** token
    (`MODERATOR_API_TOKEN`). The moderator reviews the change and records it.
- **`editor` scope = FULL power** over the warehouse: columns, views, tables, data, syncs. You do
  **NOT** need `admin` — `admin` is ONLY the moderator's own rulebook, which writers never touch.
- **Two-speed apply (the mental model):** a recorded change **applies on the NIGHTLY rebuild
  (~03:30 UTC) by default** — *"recorded / queued for nightly"* is the **CORRECT, successful**
  outcome, not a sign of missing access. **OR, if you need it live now**, run
  `apply-enqueue` then `apply-now`: that **applies it to the live DB AND re-promotes the serving
  snapshot so READERS see it in minutes** — no SSH, no nightly wait. *"Make a change, it runs
  tonight normally; if you need to, make it live NOW."*
- **The #1 historical failure:** a writer who hadn't exported their token got a cryptic
  *"SSH connection error"* and chased SSH / Tailscale / admin access they never needed. That trail
  is dead. If anything looks wrong → **run `doctor`**, which tells you the exact one-line fix.

---

## 1. The three access paths (only two are ever needed)

| # | Path | Transport | What you need | Skill / tool |
|---|---|---|---|---|
| 1 | **READ** | public HTTPS `POST /query` | a **reader** token | `/data-warehouse` skill |
| 2 | **WRITE** (schema edits: cols/views/tables/data/syncs) | public HTTPS `POST /moderator/...` | your personal **editor** token + a repo clone | `moderator_client.py loop` (this skill) |
| 3 | SSH to the box | SSH | droplet root SSH (Sam only) | **NOT needed for read or write — ignore it** |

Path 3 exists for Sam's own machine only. **A writer does not have, and does not need, droplet SSH.**

---

## 2. WRITE setup — run the DOCTOR first (this is step one, always)

From inside your **renaissance-warehouse** clone:

```bash
python scripts/moderator_client.py doctor
```

It prints a deterministic ✅/❌ checklist of **your** setup with the **exact copy-paste fix** for
each failure. It checks: (a) token in env, (b) URL correct, (c) `/healthz` reachable over public
HTTPS — which *proves* no Tailscale/SSH/VPN is needed, (d) your token's scope (reader vs editor),
(e) you're inside a warehouse clone. It **never** attempts SSH and **never** prints your token.

If everything is ✅, you're done — go to Section 3. If anything is ❌, apply the FIX line it prints,
then re-run `doctor`.

### The one-time setup the doctor is checking for

Sam sent you a **personal editor token**. Export it (and the URL) in your shell — add to
`~/.zshrc` / `~/.bashrc` to persist:

```bash
export MODERATOR_API_TOKEN=<your-personal-editor-token>
export MODERATOR_API_URL=https://renaissance-droplet.tailae5c80.ts.net/moderator
```

That's the entire setup. (Prefer a file? Put those two lines in a `.env` and
`export RENAISSANCE_ENV=/abs/path/to/your/.env` — the client reads keys from that file. But the
shell export above is simplest and always works.) Your token identifies **you** in the approval
ledger, so use **your own**, never a shared one, and **never commit it.**

You do **NOT** need: SSH, Tailscale, a VPN, droplet access, or an `admin` token.

---

## 3. WRITE flow — author → loop → (applies nightly, OR `apply-now` to make it live now)

1. Clone the repo if you haven't (the doctor will tell you if cwd isn't a clone):
   ```bash
   git clone https://github.com/sdultsin/renaissance-warehouse.git
   cd renaissance-warehouse
   ```
2. Author your change as a file the moderator gates:
   - a DDL: `sql/ddl/NN_<name>.sql` (next free `NN`), or
   - a Python entity/source/sync whose INSERT column list changes:
     `entities/*.py`, `sources/*.py`, or `scripts/*.py`.
3. Submit it:
   ```bash
   python scripts/moderator_client.py loop --files sql/ddl/NN_<name>.sql
   ```
   - **PASS / pass-with-warn** → it **records** the approval to the content-hash-bound ledger and
     prints `recorded v… <file> pass`. The change is now approved. **By default it applies on the
     next nightly rebuild (~03:30 UTC)** — *"recorded / queued for nightly" is success, not a
     failure.* If that's fine, you're done.
   - **BLOCK** → it prints the findings + fixes. A mechanical fix (e.g. use the canonical name) is
     prescribed directly. A judgement call shows **"CHOOSE one (YOU decide)"** with the options —
     the gate never picks between materially-different options; you do. Apply your choice, re-run
     `loop` (≤6 rounds). Still stuck → route to the **orchestrator bus**, never up to Sam.
   - Note: it's currently **WARN-ONLY** — even a BLOCK does not stop your commit/apply yet. You'll be
     told before the enforce flip.
4. **Need it LIVE now (not tonight)?** After a PASS, enqueue + apply-now:
   ```bash
   python scripts/moderator_client.py apply-enqueue --files sql/ddl/NN_<name>.sql
   python scripts/moderator_client.py apply-now          # applies LIVE + re-promotes for readers
   ```
   `apply-now` physically applies your **ledger-approved** change to the live warehouse (under the
   single-writer lock — it **queues behind the nightly, never clobbers**) and then **re-promotes the
   serving snapshot so READERS see it.** It is **content-hash-bound** — only the exact content you
   got a PASS for can apply. **Latency:** the snapshot copy is large (~50GB) so apply-now takes
   **several minutes (~10)** — that's the cost of "make it live now"; **let it run, don't interrupt.**
   When it finishes it prints the new `snapshot_id` + live DDL version; query the read path to confirm.
   (No SSH, no Tailscale, no admin — same editor token, same public HTTPS.)

Handy commands (all over the same public HTTPS, your editor token):
```bash
python scripts/moderator_client.py doctor                    # self-diagnose setup (run first)
python scripts/moderator_client.py review --files <paths>     # check only (no record)
python scripts/moderator_client.py loop   --files <paths>     # review → (on pass) record
python scripts/moderator_client.py apply-enqueue --files ...  # queue a recorded DDL for apply
python scripts/moderator_client.py apply-now                  # apply queued+approved DDLs LIVE + re-promote (no nightly wait)
python scripts/moderator_client.py apply-queue                # what's queued / applying / committed
python scripts/moderator_client.py rules                      # live rule set + canonical aliases
python scripts/moderator_client.py ledger                     # who approved what
python scripts/moderator_client.py issues                     # open findings
```

---

## 4. READ flow (no setup gate — anyone on the team)

Use the `/data-warehouse` skill. POST SQL to the read-only query API:

```bash
BASE="https://renaissance-droplet.tailae5c80.ts.net"     # public HTTPS, no Tailscale/SSH
curl -sS -X POST "$BASE/query" \
  -H "Authorization: Bearer $WAREHOUSE_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"sql":"SELECT max(date) FROM raw_pipeline_campaign_daily_metrics"}'
```

Reader token resolution (the `/data-warehouse` skill has the full version): `$WAREHOUSE_API_TOKEN`
→ Renaissance repo `.env` (`WAREHOUSE_API_TOKEN`). The read path is **read-only by construction**
(write is physically impossible) — separate from the editor token, and separate from write access.
`GET /schema` lists tables; `GET /guide` returns the live navigation prompt; `GET /healthz` is an
unauthenticated liveness check.

---

## 5. ERROR → FIX (every failure mode, flat)

| Symptom / message | What's actually wrong | FIX |
|---|---|---|
| **"SSH connection error" / it tries to SSH** | You forgot to export `MODERATOR_API_TOKEN` (or there's a typo), and on an old client it silently fell back to SSH. **You do NOT need SSH.** | `export MODERATOR_API_TOKEN=<your editor token>` then `export MODERATOR_API_URL=https://renaissance-droplet.tailae5c80.ts.net/moderator`. Run `doctor`. |
| **"MODERATOR_API_TOKEN is not set …"** | Token not in shell env and not in your `$RENAISSANCE_ENV` file. | Export the two vars above (Section 2). Run `doctor`. |
| **"Do I need Tailscale / a VPN?"** | No. The URL is a public Tailscale **Funnel** = ordinary public HTTPS with a valid cert. | Nothing to install. `doctor` step (c) proves it by reaching `/healthz` over plain HTTPS. |
| **"Do I need `admin` scope?"** | No. **`editor` = full power** (cols/views/tables/data/syncs). `admin` is only the moderator's own rulebook. | If `doctor` (d) says **editor**, you're fully set. |
| **`doctor` (d) says scope = READER** | You're using a read-only token (e.g. `cc-service-reader` / `WAREHOUSE_API_TOKEN`) for the moderator. | Use the **personal EDITOR token** Sam sent you for the moderator, not the read token. |
| **401 unauthorized / "unknown or revoked"** | Token is wrong, has stray quotes/spaces, or was revoked. | Re-paste your editor token exactly (no quotes/spaces). If it still 401s, ask Sam to re-issue. |
| **"recorded …" / "queued for nightly" / change not visible yet** | Nothing is wrong — this is **success**. | The change is approved; by default it applies on the **nightly rebuild (~03:30 UTC)**. To make it live in **minutes** instead: `apply-enqueue --files <path>` then `apply-now` (Section 3 step 4), then verify via the READ path. |
| **`apply-now` "timed out" / seems to hang** | The serving re-promote copies the ~50GB warehouse (~10 min) — longer than a default socket timeout. | **Let it run — do not interrupt.** The client now waits up to 30 min. The apply itself lands in seconds; the wait is the snapshot copy. If your shell did time out, the apply still committed server-side — re-run `apply-now` (idempotent; it'll report "nothing queued" + the fresh snapshot) or check `apply-queue`. |
| **`apply-now` says "another promote is already running"** | A nightly/scheduled/other promote holds the publish lock. | Your apply **already landed** in the live DB; that in-flight promote (or the next one) will serve it. Re-run `apply-now` to confirm, or just wait. |
| **`doctor` (b) URL doesn't match canonical** | `MODERATOR_API_URL` is unset or wrong. | `export MODERATOR_API_URL=https://renaissance-droplet.tailae5c80.ts.net/moderator` |
| **`doctor` (e) "cwd is NOT a renaissance-warehouse clone"** | You're running from the wrong directory, or haven't cloned. | `git clone https://github.com/sdultsin/renaissance-warehouse.git && cd renaissance-warehouse` |
| **`doctor` (c) "could NOT reach /healthz over HTTPS"** | Bad URL (fix b) or your internet. **NOT** a Tailscale/SSH problem. | Fix the URL; check connectivity. Do **not** install Tailscale or try SSH. |
| **`doctor` (c) reachable but UNHEALTHY (ok!=true)** | The service is degraded — **your setup is fine.** | Escalate to the **orchestrator bus**, not Sam. |
| **`loop` says BLOCK** | The change would break something / isn't canonical. | Apply the printed fix (or CHOOSE an option), re-run `loop` (≤6×). Stuck → orchestrator bus. |
| **"moderator service error" on review/loop** | Transient transport error. | Re-run. It never blocks your commit on a transport error during the WARN phase. |

---

## 6. What a writer must literally do (the short list)

1. `export MODERATOR_API_TOKEN=<your editor token>` and
   `export MODERATOR_API_URL=https://renaissance-droplet.tailae5c80.ts.net/moderator`
   (add both to `~/.zshrc` to persist).
2. `git clone https://github.com/sdultsin/renaissance-warehouse.git && cd renaissance-warehouse`
3. `python scripts/moderator_client.py doctor` → all ✅.
4. Author `sql/ddl/NN_*.sql` (or an `entities|sources|scripts/*.py`) →
   `python scripts/moderator_client.py loop --files <your-files>`.
5. On PASS: it lands on the **nightly (~03:30 UTC)** by default — OR, to make it live now,
   `apply-enqueue --files <your-files>` then `apply-now` (waits ~10 min for the snapshot re-promote,
   then readers see it). Verify via the READ path.

No SSH. No Tailscale. No VPN. No admin token.

---

## Notes
- This skill is **local** (`~/.claude/skills/warehouse-access/SKILL.md`). The doctor + token-missing
  behavior it relies on live in `renaissance-warehouse/scripts/moderator_client.py`.
- The older docs are **superseded by this skill**:
  `deliverables/2026-06-19-db-review-moderator-service/WRITER-ONBOARDING.md`,
  `deliverables/2026-06-19-sending-volume-dashboard/SKILL-ADDENDUM-sending-volume.md`,
  `handoffs/2026-06-19-sending-volume-dashboard-handoff.md`.
- Read path details (query API, schema navigation, anti-hallucination rules): `/data-warehouse`.
