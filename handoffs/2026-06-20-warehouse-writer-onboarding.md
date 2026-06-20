# Warehouse writer onboarding — paste this into your Claude [2026-06-20]

> **For Sam:** paste everything below the line into each writer's (David / Darcy / Thomas) Claude
> Code session, **after** you've sent them their personal **editor** token out-of-band (Slack/1Pass).
> It installs the current skills from the repo, removes the stale `data-warehouse` copy, runs the
> doctor, and teaches the full read + write + apply-now loop. It is self-contained.

---

You are being onboarded to **read and edit the Renaissance DuckDB warehouse**. Follow these steps
exactly. This corrects specific confusions earlier sessions hit — read the "Known confusions" box.

## 0. What you have / don't have (mental model — read first)
- You reach the warehouse over **plain public HTTPS** at
  `https://renaissance-droplet.tailae5c80.ts.net`. **You do NOT need SSH, Tailscale, a VPN, or
  droplet/admin access for anything** — reading or writing. If you ever find yourself trying to SSH
  or install Tailscale, **stop — that is the wrong trail** (see Known confusions #1).
- **Two tokens, two jobs:** a **reader** token for queries, and your personal **editor** token
  (`MODERATOR_API_TOKEN`) for schema edits. `editor` scope = **full** power over the warehouse
  (columns, views, tables, data, syncs). You do **NOT** need `admin` (that's only the moderator's own
  rulebook).

## 1. Set your token + URL (use the personal EDITOR token Sam sent you — never commit it)
```bash
export MODERATOR_API_TOKEN=<your-personal-editor-token>
export MODERATOR_API_URL=https://renaissance-droplet.tailae5c80.ts.net/moderator
# add both lines to ~/.zshrc (or ~/.bashrc) so they persist
```

## 2. Clone the repo and install the CURRENT skills from it (this overwrites any stale local copy)
```bash
git clone https://github.com/sdultsin/renaissance-warehouse.git   # or: cd existing clone && git pull
cd renaissance-warehouse

# install BOTH current skills from the repo (overwrites stale local forks — that is intended)
mkdir -p ~/.claude/skills/warehouse-access ~/.claude/skills/data-warehouse
cp skills/warehouse-access/SKILL.md ~/.claude/skills/warehouse-access/SKILL.md
cp skills/data-warehouse/SKILL.md   ~/.claude/skills/data-warehouse/SKILL.md
```
If you had an OLD `data-warehouse` skill that said the warehouse is "read-only" or that changes are
"nightly only," the copy above **replaces it with the current version**. Do not keep the old one.

## 3. Run the DOCTOR (always step one; it never uses SSH, never prints your token)
```bash
python scripts/moderator_client.py doctor
```
It prints a ✅/❌ checklist of YOUR setup with the exact copy-paste fix for each failure: (a) token in
env, (b) URL correct, (c) `/healthz` reachable over public HTTPS (this *proves* no SSH/Tailscale/VPN
is needed), (d) your token's scope (must say **editor**), (e) you're inside the clone. Get all ✅
before continuing. If anything is ❌, apply the FIX line it prints and re-run.

## 4. READ the warehouse (anyone on the team; load the `data-warehouse` skill for the navigation rules)
```bash
BASE="https://renaissance-droplet.tailae5c80.ts.net"     # public HTTPS, no SSH/Tailscale
curl -sS -X POST "$BASE/query" \
  -H "Authorization: Bearer $WAREHOUSE_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"sql":"SELECT max(date) FROM raw_pipeline_campaign_daily_metrics"}'
```
The response carries a `snapshot_id` — cite it next to any number. The `data-warehouse` skill has the
table map, the canonical source per metric, the NEVER list, and the anti-hallucination rules. Note:
the **query API is read-only by design** — that's the read path being read-only, **not** the warehouse
being unwritable (you write through the moderator pipeline below).

## 5. WRITE / EDIT the schema — author → loop → (nightly by default, OR apply-now to make it live now)
1. Author your change as a file the moderator gates:
   - a DDL: `sql/ddl/NN_<name>.sql` (next free `NN`; tag intent at the top:
     `-- @gate: add | rename A->B | drop | alter-type` and `-- Depends on NN`), or
   - a Python entity/source/sync whose INSERT column list changes:
     `entities/*.py`, `sources/*.py`, or `scripts/*.py`.
2. Submit it through the gate (review → on pass, record):
   ```bash
   python scripts/moderator_client.py loop --files sql/ddl/NN_<name>.sql
   ```
   - **PASS / pass-with-warn** → it records a content-hash-bound approval and prints
     `recorded v… <file> pass`. **This is success.** By default the change applies on the **nightly
     rebuild (~03:30 UTC)**. *"recorded / queued for nightly" is NOT a failure or missing access.*
   - **BLOCK** → it prints the fixes. Mechanical ones (e.g. use the canonical name) are prescribed;
     a judgement call shows **"CHOOSE one (YOU decide)"** — you pick. Apply, re-run `loop` (≤6×).
     Still stuck after ~6 → route to the **orchestrator bus**, do not escalate to Sam directly.
3. **Need it live NOW (not tonight)?** After a PASS:
   ```bash
   python scripts/moderator_client.py apply-enqueue --files sql/ddl/NN_<name>.sql
   python scripts/moderator_client.py apply-now
   ```
   `apply-now` applies your **ledger-approved** change to the live warehouse (under the single-writer
   lock — it **queues behind the nightly, never clobbers**) and **re-promotes the serving snapshot so
   readers see it.** **It takes ~10 minutes** (it copies the ~50GB warehouse) — **let it run, don't
   interrupt.** It prints the new `snapshot_id` + live DDL version when done; confirm via a read query
   (step 4). Same editor token, same public HTTPS — no SSH.

Useful: `apply-queue` (what's queued/applying/committed), `ledger` (who approved what), `issues`
(open findings), `rules` (live rules + canonical aliases).

---

## Known confusions (these are the exact ones earlier sessions hit — don't repeat them)

1. **"SSH connection error" / "do I need Tailscale / admin?"** → **No.** That error means your
   `MODERATOR_API_TOKEN` wasn't exported (or has a typo). The service is **public HTTPS**. Export the
   token (step 1) and run `doctor`. You never need SSH, Tailscale, a VPN, droplet access, or `admin`.
2. **"read_only" in a query response / "the warehouse is read-only"** → The **query API** is read-only
   **by design** (so anyone can read safely). The **warehouse is writable** — you edit it through the
   moderator pipeline (step 5). "read_only" is about the read path, not a limit on changing the schema.
3. **"recorded / queued for nightly"** → That is **SUCCESS**, not a problem. The change is approved
   and will apply on the nightly. If you need it sooner, that's exactly what `apply-now` is for
   (step 5.3) — applied + visible to readers in minutes.
4. **`apply-now` "hangs" / "times out"** → It's copying the ~50GB snapshot (~10 min). **Let it run.**
   The apply itself is instant; the wait is the re-promote. If your shell did time out, the apply
   still committed — re-run `apply-now` (it'll say "nothing queued" + show the fresh snapshot) or
   check `apply-queue`.
5. **`apply-now` says "another promote is already running"** → Your apply **already landed**; an
   in-flight promote (or the next one) will serve it. Re-run `apply-now` to confirm, or just wait.

## If you're stuck
Run `python scripts/moderator_client.py doctor` first — it diagnoses 95% of setup issues with the
exact fix. A genuine gate fork or a service-degraded ("healthz ok!=true") issue → route to the
**orchestrator bus**, not Sam. The full reference is the **`warehouse-access`** skill you just installed.
