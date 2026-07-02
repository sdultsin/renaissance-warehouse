# Daily RevOps Report — source registry & change-capture process

`config/daily_report_sources.json` is the **single, machine-readable, self-verifiable** map of
**metric → {source type, exact id, tab, desk/owner, dedup key, reconciliation anchor}** for the
Daily RevOps Report. `scripts/render_daily.py` resolves its human-managed / drift-prone sources
(Pre-IPO desks, the consolidated booking sheet, sendivo sub-accounts, the workspace roster) **from
this file**, so the registry **is** what the renderer uses — no comment-vs-code drift.

This exists because of a real incident (`handoffs/2026-06-30-DATA-TICKET-preipo-source-mapping-incompleteness.md`):
the Pre-IPO desks were known only in people's heads / Slack, the source map was prose and partial,
and a chat building the report **could not confirm it was reading the right source** — Sam had to
hand-feed "Collins + Summit." The registry + the `--verify` self-check kill that class of bug.

## How a chat / QA self-verifies a metric (zero hand-feeding)
1. **Read the registry** for the metric's declared source(s): `config/daily_report_sources.json`.
2. **Run the self-check** (read-only, writes no sheet):
   ```bash
   .venv/bin/python scripts/render_daily.py 2026-06-29 --verify
   ```
   It probes **every** registered source is reachable + correctly-shaped (right sheet/tab, required
   columns present, warehouse relation exists), and **reconciles Pre-IPO** per-desk counts to the
   team's own `#pre-ipo-success` counter. Output is a per-metric `✓ OK / ! WARN / ✗ DRIFT` report.
3. **Trust the exit code:** `0` = every source confirmed; `2` = at least one source **drifted**
   (flagged loudly — never silently rendered). `WARN` = reachable but the cross-tally anchor was
   unavailable (no Slack read creds / not a known-good date) → structural check only.

The normal nightly render ALSO runs the Pre-IPO reconciliation *after* writing the tab and posts a
Slack drift alert (via `scripts/alert_slack.py`) if the desks don't reconcile — it never blocks the
tab, but it never renders a drifted Pre-IPO number silently either.

## The reconciliation anchor (Pre-IPO)
Pre-IPO ("SMS IPO") meetings = the **sum of the additive desks** (currently **Collins + Summit**).
Each desk posts a running per-desk counter in **#pre-ipo-success** (`C0B0K3VCDMX`) that **resets to 1
each ET calendar day**; multiple bookers each run their own `1..N`. The day's tally per desk is the
**max N** (NOT the sum of lines), bucketed by message **post date in ET** (the "June DD" inside a
line is the *script name*, not the booking date). `--verify` reads the channel live and reconciles the
rendered sheet count to that max-counter. A `<Desk> Booked N` label seen in the channel that is **not
in the registry** is itself surfaced as drift (a new/renamed desk → update the registry).

Sheet→desk labels are confirmed: `1IZzmCXt…` = **Collins**, `1oKlY_2qI…` = **Summit** (Grace → Sam
DM 2026-06-29). The old "Craig Diana" label on `1oKlY…` was wrong — do not re-mislabel it.

## Change-capture — when to update this registry (and who)
Update `config/daily_report_sources.json` **in the same PR** that needs it (or before), the moment any
of these changes — never let the report discover drift by Sam eyeballing a wrong number:

| Change | Edit |
|---|---|
| A Pre-IPO desk is **added / renamed / retired** | `metrics.preipo_meetings.desks[]` (+ its `known_good`) |
| A booking sheet **ID or tab** changes | `metrics.email_meetings_leadtype.sheet` / `bookings_by_partner.sheet` |
| A **sendivo sub-account** id changes | `metrics.sms_sent.api.sub_accounts[]` |
| A **workspace** is added / its slug changes | `workspaces.roster[]` |
| A canonical **warehouse relation** is renamed | the metric's `warehouse.relation` |

A desk add/rename usually shows up FIRST as a new `<Desk> Booked` label in `#pre-ipo-success` → the
nightly drift alert (or `--verify`) will surface it as an unknown desk. Add it here, with its sheet ID
+ tab, and it flows into the Pre-IPO total automatically.

## Open question (routed to Sam → Grace/Darcy, 2026-06-30)
There is **no canonical desk roster / pinned doc** in Slack — a new desk is only discoverable from the
post stream. Asked: is there (or can the team maintain) an authoritative list of all Pre-IPO desks +
sheet IDs, and will the new **"Renny"** booking automation (Darcy, ~2026-06-30, posts partner+channel
per booking) become the canonical machine-readable Pre-IPO source — in which case the renderer should
read Pre-IPO meetings from Renny instead of scraping the two desk sheets. See `open_questions[0]` in
the JSON and `reference_people_pending_asks_20260629`.
