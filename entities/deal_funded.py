"""core.deal_funded loader — partner funded-deal outcomes (the terminal funnel + revenue node).

Wires the FIRST source into the empty-by-design core.deal_funded (sql/ddl/73_deals_funded.sql):
partner-reported funded-deal outcome CSVs. GBC is the first partner; more partners drop a CSV
into seed_data/partner_deal_outcomes/<partner>.csv (filename stem = partner key) and are picked
up automatically — the "fills out as data arrives" architecture Sam asked for (2026-06-22).

⚠ PII: lead emails. The seed dir is git-ignored; this loader reads it locally, commits nothing.

PARTIAL BY DESIGN (one partner today; partners still sending data). Per the 100%-or-WIPE rule
(feedback_partial_data_100pct_or_wipe_20260614) we therefore: (a) load ONLY real funded rows
(no placeholders/zeros-as-real); (b) tag every row with its attribution tier in `notes` so the
partiality is explicit; (c) do NOT compute any fleet fund-rate off it — the portal deals-funded
tile stays HIDDEN (scripts/portal_data.py). The node is *connected + queryable* for the deals we
have and lights up further as more partner CSVs land. NO numbered DDL: core.deal_funded + its
views already exist (DDL 73); the raw staging table is created inline (IF NOT EXISTS).

Attribution — lead-level wherever the data allows, campaign fallback, honest about the rest:
  lead_email   always (lower-cased) — the lead-grain join key.
  lead_key     core.lead.lead_key when the email matches an engaged lead.
  campaign_id  the lead's MOST-RECENT reply campaign via core.reply (lead_email->campaign_id, the
               100%-healthy hop; reply, not meeting — meeting.lead_email is 89% NULL).
  notes        'attr=lead+campaign' | 'attr=lead' | 'attr=email_only' (tier, for transparency).
Measured GBC coverage (180 funded): 92 lead+campaign, 88 email_only ($11.16M funded / $448K comm).

Runs in the 'derived' phase — strictly AFTER 'canonical' builds core.reply/lead/meeting, which the
attribution joins read. Auto-discovery imports entities in filename-sorted order, so 'deal_funded'
would precede 'lead_spine'/'reply_canonical' inside 'canonical' — riding a LATER phase is REQUIRED.
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
# mis-attribute money). Order-independent; matched by name.
_EXPECTED_COLS = {
    "email", "outcome", "company_name", "first_name", "last_name",
    "amount_funded", "commission", "date_funded", "meeting_date", "stage", "lead_status",
}

# Outcome value that means a deal actually funded (the terminal conversion).
_FUNDED_OUTCOME = "funded"


def run_deal_funded(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    # Raw staging: every stage/outcome for every partner (the full bottom-of-funnel ladder),
    # partner-tagged. core.deal_funded is the FUNDED projection of this; the rest is kept for the
    # broader outcome lens + so coverage is auditable.
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS main.raw_partner_deal_outcomes (
          source_partner VARCHAR NOT NULL,
          lead_email     VARCHAR,
          outcome        VARCHAR,
          company_name   VARCHAR,
          first_name     VARCHAR,
          last_name      VARCHAR,
          amount_funded  DOUBLE,
          commission     DOUBLE,
          funded_date    DATE,
          meeting_date   DATE,
          stage          VARCHAR,
          lead_status    VARCHAR,
          _loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
          _run_id        VARCHAR
        )
        """
    )

    csvs = sorted(glob.glob(str(_SEED_DIR / "*.csv")))
    if not csvs:
        logger.warning("No partner-outcome CSVs in %s — skipping (core.deal_funded left as-is)", _SEED_DIR)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_csv"})

    loaded: dict[str, int] = {}
    for csv_path in csvs:
        partner = os.path.splitext(os.path.basename(csv_path))[0].lower()
        db.execute(
            "CREATE OR REPLACE TEMP TABLE _pdo_stage AS "
            "SELECT * FROM read_csv(?, header=true, all_varchar=true)",
            [csv_path],
        )
        have = {r[0] for r in db.execute("DESCRIBE _pdo_stage").fetchall()}
        missing = _EXPECTED_COLS - have
        if missing:
            raise ValueError(f"{csv_path}: partner-outcome CSV missing columns {sorted(missing)} "
                             f"(have {sorted(have)}) — refusing to load (mis-attribution guard)")
        # Idempotent per partner: clear this partner's rows, then insert.
        db.execute("DELETE FROM main.raw_partner_deal_outcomes WHERE source_partner = ?", [partner])
        db.execute(
            """
            INSERT INTO main.raw_partner_deal_outcomes
              (source_partner, lead_email, outcome, company_name, first_name, last_name,
               amount_funded, commission, funded_date, meeting_date, stage, lead_status, _run_id)
            SELECT ?, lower(trim(email)), outcome, company_name, first_name, last_name,
                   TRY_CAST(amount_funded AS DOUBLE), TRY_CAST(commission AS DOUBLE),
                   TRY_CAST(date_funded AS DATE), TRY_CAST(meeting_date AS DATE),
                   stage, lead_status, ?
            FROM _pdo_stage
            WHERE email IS NOT NULL AND trim(email) <> ''
            """,
            [partner, ctx.run_id],
        )
        n = db.execute(
            "SELECT count(*) FROM _pdo_stage WHERE email IS NOT NULL AND trim(email) <> ''"
        ).fetchone()[0]
        loaded[os.path.basename(csv_path)] = n

    # ── Project the FUNDED rows -> core.deal_funded, attributed. Idempotent full rebuild of the
    # partner_csv source (DELETE that source, re-INSERT from raw — same shape as meeting.py). ──
    # partner-key -> label CASE for in-SQL labelling (small, static).
    label_case = " ".join(
        f"WHEN '{k.replace(chr(39), chr(39)*2)}' THEN '{v.replace(chr(39), chr(39)*2)}'"
        for k, v in _PARTNER_LABEL.items()
    )
    label_expr = f"CASE o.source_partner {label_case} ELSE upper(o.source_partner) END" if label_case \
        else "upper(o.source_partner)"

    db.execute("DELETE FROM core.deal_funded WHERE source = 'partner_csv'")
    db.execute(
        f"""
        INSERT INTO core.deal_funded
          (deal_id, source, source_event_id, meeting_id, lead_key, lead_email, campaign_id,
           campaign_name_raw, workspace_id, channel, cm, advisor, advisor_name, advisor_partner,
           inbox_manager, partner_key, partner_label, funded_date, amount_funded, commission_rate,
           commission_amount, currency, deal_status, notes, raw_text, row_hash,
           _first_run_id, _last_run_id, _first_seen_at, _last_seen_at, _loaded_at)
        WITH funded AS (
          SELECT * FROM main.raw_partner_deal_outcomes
          WHERE outcome = '{_FUNDED_OUTCOME}' AND lead_email IS NOT NULL AND lead_email <> ''
        ),
        rc AS (   -- most-recent reply campaign per lead_email (the 100%-healthy reply->lead hop)
          SELECT lead_email, campaign_id, workspace_id,
                 row_number() OVER (PARTITION BY lead_email ORDER BY reply_timestamp DESC NULLS LAST) AS rn
          FROM core.reply WHERE campaign_id IS NOT NULL
        )
        SELECT
          md5(o.source_partner || ':' || o.lead_email || ':' || COALESCE(CAST(o.funded_date AS VARCHAR), '')) AS deal_id,
          'partner_csv'                                  AS source,
          o.source_partner || ':' || o.lead_email        AS source_event_id,
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
          o.source_partner                               AS partner_key,
          {label_expr}                                   AS partner_label,
          o.funded_date                                  AS funded_date,
          o.amount_funded                                AS amount_funded,
          NULL                                           AS commission_rate,
          o.commission                                   AS commission_amount,
          'USD'                                          AS currency,
          'funded'                                       AS deal_status,
          'attr=' || CASE WHEN l.lead_key IS NOT NULL AND rc.campaign_id IS NOT NULL THEN 'lead+campaign'
                          WHEN l.lead_key IS NOT NULL THEN 'lead'
                          ELSE 'email_only' END           AS notes,
          NULL                                           AS raw_text,
          md5(COALESCE(CAST(o.amount_funded AS VARCHAR), '') || '|' || COALESCE(CAST(o.commission AS VARCHAR), '')
              || '|' || COALESCE(CAST(o.funded_date AS VARCHAR), '')) AS row_hash,
          ?                                              AS _first_run_id,
          ?                                              AS _last_run_id,
          now()                                          AS _first_seen_at,
          now()                                          AS _last_seen_at,
          now()                                          AS _loaded_at
        FROM funded o
        LEFT JOIN core.lead l ON l.email = o.lead_email
        LEFT JOIN rc ON rc.lead_email = o.lead_email AND rc.rn = 1
        -- one row per deal_id (a lead with multiple funded rows on the same funded_date collapses)
        QUALIFY row_number() OVER (PARTITION BY md5(o.source_partner || ':' || o.lead_email || ':'
                || COALESCE(CAST(o.funded_date AS VARCHAR), '')) ORDER BY o.amount_funded DESC NULLS LAST) = 1
        """,
        [ctx.run_id, ctx.run_id],
    )

    raw_total = sum(loaded.values())
    funded = db.execute("SELECT count(*) FROM core.deal_funded WHERE source='partner_csv'").fetchone()[0]
    tiers = dict(db.execute(
        "SELECT notes, count(*) FROM core.deal_funded WHERE source='partner_csv' GROUP BY 1"
    ).fetchall())
    logger.info("deal_funded: %d raw outcome rows from %d file(s); %d funded deals loaded (tiers=%s)",
                raw_total, len(csvs), funded, tiers)
    return PhaseResult(rows_in=raw_total, rows_out=funded, notes={"files": loaded, "tiers": tiers})


def register(registry: Registry) -> None:
    # 'derived' runs after 'canonical' (core.reply/lead/meeting) — required for the attribution joins.
    registry.add_phase("derived", "deal_funded", run_deal_funded)
