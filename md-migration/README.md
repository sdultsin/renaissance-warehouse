# md-migration/ — MotherDuck migration scripts (version-controlled home)

Runtime home: **`/root/md-migration/` on the droplet** (outside the repo by design — no
git-divergence-guard exposure). This directory is the same code under version control, per the
droplet-exit rule that box-only code is what made the droplet undisposable. **Edit here → copy to
`/root/md-migration/` → commit+push** (or edit on the box and mirror back the same session).

## Live today (crons on the droplet)
| Script | Cron | Role |
|---|---|---|
| `escrow_export.py` | 10:00Z | immutable R2 escrow — daily full recovery point (never-lose-data floor) |
| `escrow_watchdog.py` | */4h | escrow complete+fresh + read-API health; self-heals partial days |
| `md_load_tables_v3.py` | 09:00Z | daily count-skip storefront refresh of `md:warehouse` — **retired at write-path go-live** (RUNBOOK a.5) |
| `migration_shepherd.py` | 14:00Z | posts the migration's current next action to #cc-sam until done (`migration_next.txt`) |

## Staged for Tuesday 2026-07-14 (write-path go-live; NOT yet cron-installed)
| File | Role |
|---|---|
| `md_write_path.py` | THE write-path runner: gate on committed nightly → full-fidelity build into inactive color → validate (exact parity + views + `canaries.json`) → atomic pointer flip + canonical `md:warehouse` zero-copy republish. Final form of v3+publish. |
| `md_write_path.sh` | cron wrapper (per-day stamp; gate makes it fire once per green nightly) |
| `canaries.json` | canary floors (from 07-12 actuals) run against BOTH the source snapshot and the built color |
| `reader_flip.sh` | apply/rollback/status for the query-API reader flip (systemd drop-in; auto-rollback on failed verify) |
| `ddl_replay.py` | replay macros+views from a catalog (setup_db.py is version-gated → no-ops on copied DBs) |
| `cron-md-write-path.cron` | the exact cron lines to install |
| `RUNBOOK-TUESDAY.md` | ordered go-live commands: enable → dual-run checklist → reader flip → rollbacks |

## Parked / historical (kept for provenance)
`md_publish.py` (blue/green publish, superseded by md_write_path.py) · `md_health_watchdog.py`
(re-armed at go-live) · `md_autoflip.py`, `md_flip_rehearsal.py`, `apply_shim_staging.py`,
`patch_query_logging.py`, `port_macros_views.py`, `md_read_and_swap_proof.py`,
`md_shim_smoketest*.py`, `load_tables*.py` (v1/v2/v3 loader lineage), `escrow_coverage.py`.

## shim/
Version-controlled copies of the live query-API shim (`/opt/duckdb/bin/mcp_server.py`,
`common.py`, `config.yaml` — secret-free; bearer tokens live in `/opt/duckdb/allowed_tokens.txt`,
NOT in git). `WAREHOUSE_BACKEND=local|md` env switch; `reader_flip.sh` is the operational wrapper.
If you change the shim: edit here → review → copy to `/opt/duckdb/bin/` → restart mcp-server.

Context docs: `handoffs/2026-07-10-motherduck-migration-state-and-next.md` ·
`deliverables/2026-07-09-motherduck-migration/` · `deliverables/2026-07-08-mof-orchestrator/DROPLET-EXIT-PLAN.md`
(all in the Renaissance memory directory, not this repo).
