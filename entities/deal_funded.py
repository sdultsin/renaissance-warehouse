"""core.deal_funded loader — partner funded-deal outcome CSVs → the (DDL-73) empty-by-design node.

First source wired into core.deal_funded. Loads seed_data/partner_deal_outcomes/<partner>.csv
(git-ignored PII; filename stem = partner key) and projects the outcome='funded' rows DIRECTLY
into the existing core.deal_funded (no new DDL/schema change — only an INSERT), with an explicit
alias for every target column (CSV email/date_funded → grain lead_email/funded_date, visibly).

Idempotent: DELETE WHERE source='partner_csv' THEN re-INSERT (full per-source refresh — re-runs
never double-count). Attribution: lead_email always; lead_key via core.lead; campaign_id via the
lead's most-recent core.reply (the 100%-healthy reply→lead hop), email normalized identically on
both sides + a deterministic tiebreaker; tier recorded in `notes`. Partial by design (one partner
today) → 100%-or-WIPE: real funded rows only, tier-tagged, NO fleet fund-rate (portal tile HIDDEN).
Runs in the 'derived' phase — AFTER 'canonical' builds reply/lead (required for the joins). Full
rationale + coverage: deliverables/2026-06-22-warehouse-unification/PIECE2-deal-tail-findings.md.
"""
from __future__ import annotations

import glob
import logging
import os

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.deal_funded")

_SEED_DIR = REPO_ROOT / "seed_data" / "partner_deal_outcomes"

# partner-key (CSV filename stem, lower-case) -> display label. Extend as partners are added.
_PARTNER_LABEL = {
    "gbc": "GBC",
}

# Columns the partner-outcome CSV MUST carry (fail loud on drift — a silent column shift would
# mis-attribute money). Order-independent; matched by name against the CSV header.
_EXPECTED_COLS = {
    "email", "outcome", "company_name", "first_name", "last_name",
    "amount_funded", "commission", "date_funded", "meeting_date", "stage", "lead_status",
}

_FUNDED_OUTCOME = "funded"  # the outcome value that means a deal actually funded


def _label_expr(col: str) -> str:
    """SQL: partner-key column -> display label (static CASE; falls back to UPPER(key))."""
    whens = " ".join(
        f"WHEN '{k.replace(chr(39), chr(39)*2)}' THEN '{v.replace(chr(39), chr(39)*2)}'"
        for k, v in _PARTNER_LABEL.items()
    )
    return f"CASE {col} {whens} ELSE upper({col}) END" if whens else f"upper({col})"


def run_deal_funded(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    csvs = sorted(glob.glob(str(_SEED_DIR / "*.csv")))
    if not csvs:
        logger.warning("No partner-outcome CSVs in %s — skipping (core.deal_funded left as-is)", _SEED_DIR)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_csv"})

    # Idempotent full rebuild of THIS source only (scoped; never touches other sources' rows).
    db.execute("DELETE FROM core.deal_funded WHERE source = 'partner_csv'")

    loaded: dict[str, int] = {}
    for csv_path in csvs:
        partner = os.path.splitext(os.path.basename(csv_path))[0].lower().replace("'", "")
        db.execute(
            "CREATE OR REPLACE TEMP TABLE _df_stage AS "
            "SELECT * FROM read_csv(?, header=true, all_varchar=true)",
            [csv_path],
        )
        have = {r[0] for r in db.execute("DESCRIBE _df_stage").fetchall()}
        missing = _EXPECTED_COLS - have
        if missing:
            raise ValueError(f"{csv_path}: partner-outcome CSV missing columns {sorted(missing)} "
                             f"(have {sorted(have)}) — refusing to load (mis-attribution guard)")

        # Project the FUNDED rows straight into the EXISTING core.deal_funded. Every target column
        # is explicitly aliased; the CSV->warehouse name maps (email->lead_email, date_funded->
        # funded_date) happen visibly here, never by implicit positional/name binding.
        db.execute(
            f"""
            INSERT INTO core.deal_funded
              (deal_id, source, source_event_id, meeting_id, lead_key, lead_email, campaign_id,
               campaign_name_raw, workspace_id, channel, cm, advisor, advisor_name, advisor_partner,
               inbox_manager, partner_key, partner_label, funded_date, amount_funded, commission_rate,
               commission_amount, currency, deal_status, notes, raw_text, row_hash,
               _first_run_id, _last_run_id, _first_seen_at, _last_seen_at, _loaded_at)
            WITH funded AS (
              SELECT
                ? AS partner_key,
                lower(trim(email))                AS lead_email,
                TRY_CAST(amount_funded AS DOUBLE) AS amount_funded,
                TRY_CAST(commission AS DOUBLE)    AS commission_amount,
                TRY_CAST(date_funded AS DATE)     AS funded_date
              FROM _df_stage
              WHERE outcome = '{_FUNDED_OUTCOME}' AND email IS NOT NULL AND trim(email) <> ''
            ),
            rc AS (   -- most-recent reply campaign per lead_email (the 100%-healthy reply->lead hop).
                      -- email normalized identically to the deal side; campaign_id as a stable
                      -- secondary sort so a same-timestamp tie is DETERMINISTIC across runs.
              SELECT lower(trim(lead_email)) AS lead_email, campaign_id, workspace_id,
                     row_number() OVER (PARTITION BY lower(trim(lead_email))
                                        ORDER BY reply_timestamp DESC NULLS LAST, campaign_id) AS rn
              FROM core.reply WHERE campaign_id IS NOT NULL AND lead_email IS NOT NULL
            )
            SELECT
              md5(o.partner_key || ':' || o.lead_email || ':' || COALESCE(CAST(o.funded_date AS VARCHAR), '')) AS deal_id,
              'partner_csv'                                  AS source,
              o.partner_key || ':' || o.lead_email           AS source_event_id,
              NULL                                           AS meeting_id,
              l.lead_key                                     AS lead_key,
              o.lead_email                                   AS lead_email,
              rc.campaign_id                                 AS campaign_id,
              NULL                                           AS campaign_name_raw,
              rc.workspace_id                                AS workspace_id,
              NULL                                           AS channel,
              NULL                                           AS cm,
              NULL                                           AS advisor,
              NULL                                           AS advisor_name,
              NULL                                           AS advisor_partner,
              NULL                                           AS inbox_manager,
              o.partner_key                                  AS partner_key,
              {_label_expr('o.partner_key')}                 AS partner_label,
              o.funded_date                                  AS funded_date,
              o.amount_funded                                AS amount_funded,
              NULL                                           AS commission_rate,
              o.commission_amount                            AS commission_amount,
              'USD'                                          AS currency,
              'funded'                                       AS deal_status,
              'attr=' || CASE WHEN l.lead_key IS NOT NULL AND rc.campaign_id IS NOT NULL THEN 'lead+campaign'
                              WHEN l.lead_key IS NOT NULL THEN 'lead'
                              ELSE 'email_only' END           AS notes,
              NULL                                           AS raw_text,
              md5(COALESCE(CAST(o.amount_funded AS VARCHAR), '') || '|' || COALESCE(CAST(o.commission_amount AS VARCHAR), '')
                  || '|' || COALESCE(CAST(o.funded_date AS VARCHAR), '')) AS row_hash,
              ?                                              AS _first_run_id,
              ?                                              AS _last_run_id,
              now()                                          AS _first_seen_at,
              now()                                          AS _last_seen_at,
              now()                                          AS _loaded_at
            FROM funded o
            -- both sides normalized identically (lower+trim) so a casing/whitespace difference
            -- can never silently downgrade a real lead match to 'email_only'.
            LEFT JOIN core.lead l ON lower(trim(l.email)) = o.lead_email
            LEFT JOIN rc ON rc.lead_email = o.lead_email AND rc.rn = 1
            -- one row per deal_id (a lead with multiple funded rows on the same funded_date collapses
            -- to the largest-amount row — deterministic, no double-count of a single funded event).
            QUALIFY row_number() OVER (
              PARTITION BY md5(o.partner_key || ':' || o.lead_email || ':' || COALESCE(CAST(o.funded_date AS VARCHAR), ''))
              ORDER BY o.amount_funded DESC NULLS LAST) = 1
            """,
            [partner, ctx.run_id, ctx.run_id],
        )
        n = db.execute(
            "SELECT count(*) FROM _df_stage WHERE outcome = ? AND email IS NOT NULL AND trim(email) <> ''",
            [_FUNDED_OUTCOME],
        ).fetchone()[0]
        loaded[os.path.basename(csv_path)] = n

    funded = db.execute("SELECT count(*) FROM core.deal_funded WHERE source='partner_csv'").fetchone()[0]
    tiers = dict(db.execute(
        "SELECT notes, count(*) FROM core.deal_funded WHERE source='partner_csv' GROUP BY 1"
    ).fetchall())
    logger.info("deal_funded: %d funded rows across %d partner file(s); %d deals loaded (tiers=%s)",
                sum(loaded.values()), len(csvs), funded, tiers)
    return PhaseResult(rows_in=sum(loaded.values()), rows_out=funded, notes={"files": loaded, "tiers": tiers})


def register(registry: Registry) -> None:
    # 'derived' runs after 'canonical' (core.reply/lead/meeting) — required for the attribution joins.
    registry.add_phase("derived", "deal_funded", run_deal_funded)
