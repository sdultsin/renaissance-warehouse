# Warehouse Disaster Recovery & Durability

_Last updated 2026-06-23. Owner: warehouse-ops. This is the runbook for "the warehouse box died / the DB is corrupt / we lost data."_

## What the warehouse actually is

Two things at once, and the distinction decides recoverability:

1. **A derived cache** of upstream systems. Most tables are re-pullable by re-running the nightly
   orchestrator from source (pipeline-supabase, Close, comms-supabase, Instantly, Sheets, Sendivo, OTD).
2. **An accumulating archive** for a minority of high-value data that upstream no longer holds.
   The `raw_*` mirrors are **freeze-on-delete** (`entities/pipeline_mirror.py`): they keep rows even
   after the row vanishes upstream. For those slices the warehouse is the **only** copy.

### Recoverable by re-running nightly (NOT lost if the DB dies)
- Sends / opps / reply **counts**, full timeline → pipeline-supabase (holds full history, verified).
- Meetings (back to 2024-04) → pipeline-supabase (`meetings_booked_raw`, exact-match verified).
- Close calls / transcripts → Close API (full history, incremental watermark).
- comms / SMS within the upstream retention window.

### Warehouse-only — PERMANENTLY LOST if the DB **and** its backups are gone
- **Verbatim reply TEXT** for deleted/cancelled campaigns (`raw_instantly_email`). Instantly's API
  drops deleted campaigns; ~57% of reply-bearing campaigns are already gone upstream. Counts survive
  in pipeline-supabase; the actual prose does not.
- **Instantly lifetime `campaign_analytics` + `core.campaign` dimension** for deleted campaigns.
- **SMS delivery metrics older than 30 days** (Sendivo `/delivery-metrics` caps at 30 days).
- **`seed_data/`** — git-ignored, exists only on the droplet + (now) the backups:
  - `partner_deal_outcomes/gbc.csv` — the **$11.16M funded-deals revenue truth**. Ultimate upstream
    is GBC themselves (a manual partner round-trip), NOT an automated sync.
  - LLM-labeled reply corpora (`reply-is-positive-*`, `reply-offer`, `sms-*`) — re-deriving means
    re-running the labeling jobs (compute + time), not a sync.

## Where the backups live (as of 2026-06-23)

| Copy | Location | Contents | Retention | Frequency |
|---|---|---|---|---|
| Live DB | `/mnt/volume_nyc1_…/core/warehouse.duckdb` (symlinked from `/root/core/`) | everything | — | — |
| Local backup | `/mnt/volume_nyc1_…/backups/warehouse-YYYY-MM-DD.duckdb` + `seed_data-*.tar.gz` | DB + seed_data | ~4 days (volume) | nightly 05:45 UTC, `scripts/backup.sh` |
| **Off-box** | **Google Drive `sdultsin@gmail.com:Renaissance/warehouse-offbox-backups/`** (`duckdb/`, `seed_data/`, additive `seed_data_live/`) | DB + seed_data | 10 days dated; `seed_data_live` never pruned | nightly via `backup.sh` (rclone) |
| Laptop | `…/Renaissance/renaissance-warehouse/seed_data/` | seed_data only | manual | pulled 2026-06-23 |
| Code/DDL | GitHub `sdultsin/renaissance-warehouse` (+ box local git) | all code, no data | git history | on change |

3-2-1 status: ✅ 3 copies (live + volume + Drive), 2 media, 1 off-box. The off-box copy is the one that
survives a full droplet/volume loss.

## Recovery procedures

### A) DB corrupt but droplet/volume intact (most likely — kill-9 mid-write, torn write)
1. Stop writers: `flock` is held by nightly; ensure no orchestrator running (`pgrep -af orchestrator`).
2. Restore newest good local backup:
   `cp /mnt/volume_nyc1_*/backups/warehouse-<newest>.duckdb /root/core/.warehouse-restore.duckdb`
   then verify: `duckdb -readonly /root/core/.warehouse-restore.duckdb "SELECT 1"`.
3. Swap into place (preserve the symlink target on the volume):
   `mv /root/core/.warehouse-restore.duckdb /mnt/volume_nyc1_*/core/warehouse.duckdb` (the
   `/root/core/warehouse.duckdb` symlink keeps pointing at it).
4. Re-run nightly to top up the last day: `/root/renaissance-warehouse/scripts/nightly.sh`.

### B) Droplet or volume lost entirely (the scenario backups exist for)
1. Provision a new droplet (Ubuntu, ≥16GB RAM) + block-storage volume; clone the repo:
   `git clone https://github.com/sdultsin/renaissance-warehouse && cd renaissance-warehouse`.
2. Restore secrets: `.env` is NOT in git — recover from the password manager / Sam. (`.env.example` lists keys.)
3. Pull the newest off-box DB + seed_data from Drive:
   `rclone copy "sdultsin@gmail.com:Renaissance/warehouse-offbox-backups/duckdb" /root/core/`
   `rclone copy "sdultsin@gmail.com:Renaissance/warehouse-offbox-backups/seed_data_live" /root/renaissance-warehouse/seed_data/`
   (or expand the newest `seed_data/seed_data-*.tar.gz`).
4. Point `/root/core/warehouse.duckdb` at the restored file; run `setup_db.py` then `nightly.sh`.
5. Re-attach dependents (Lens dashboards, read MCP/query API, CC D1 publish) — see `nightly.sh` tail.

### C) Rebuild-from-scratch (no DB backup at all — last resort)
`python -m core.orchestrator` rebuilds all `core.*` from upstream in ~2h. **This does NOT recover the
warehouse-only slices** listed above — that data is only as safe as the most recent backup. This is
exactly why off-box backups are mandatory, not optional.

## Known open risks (2026-06-23)

- **Compaction is silently dead since the 2026-06-16 volume migration.** `compact_warehouse.sh` reads
  the DB size via `stat -c%s` on the **symlink** (returns ~52 bytes → "0GB" → skips nightly). The DB has
  bloated 60GB → 96GB unchecked. Fixing requires (a) `stat -Lc%s` to follow the symlink, (b) free-space
  check against the **volume** not `/root`, (c) scratch (`$EXP`/`$NEW`) on the volume, and (d) the swap
  must write the volume target, **not** `mv` a real file onto the `/root/core/` symlink (which would undo
  the migration and refill root). It also failed "import" on Jun 15/16 — verify the EXPORT/IMPORT path on
  this DB before trusting an automated run. **Not durability-critical** (volume has room), but it inflates
  backup size + upload time ~3x.
- **pipeline-supabase backup/PITR unconfirmed.** ~85% of the analytical record is recoverable *because*
  pipeline-supabase retains full history — so it is now a co-equal system of record. Confirm PITR (or at
  least daily backups) is enabled on that Supabase project. Mitigating factor: the off-box warehouse
  backup is itself a de-facto backup of the pipeline-supabase tables the warehouse mirrors.
- **`.env` is not backed up off-box** (secrets). Keep it in the password manager; recovery (B) depends on it.

## Secrets backup (added 2026-06-23) — supersedes the ".env not backed up" risk note above

Secrets ARE now backed up off-box, encrypted. `scripts/backup_secrets.sh` (nightly 08:00 UTC) bundles
all ~64 secret files (every `.env*`, API keys, query-API tokens, `~/.ssh/*`, rclone config), encrypts
with **age public-key** encryption, and pushes the ciphertext to Drive `Renaissance/secrets-encrypted/`
(30-day retention). The box holds only the PUBLIC key, so the bundle is safe even if the box or Drive
is compromised.

- Public (recipient) key: `age1qr2cuzuag9rrhmersjyd8jp5w4pk243fdlfnsw8g0he7y3xfmgjs498kcs`
- PRIVATE key: `/root/.config/secrets-backup/identity.key` on the box AND in Sam's password manager
  (custody = Sam). Without it the encrypted bundles cannot be decrypted.
- **Restore:** `rclone copy "sdultsin@gmail.com:Renaissance/secrets-encrypted/secrets-<newest>.tar.age" .`
  then `age -d -i identity.key secrets-<newest>.tar.age | tar -xzf - -C /restore-root` (paths are
  rooted at `root/...`). Round-trip verified working 2026-06-23.
