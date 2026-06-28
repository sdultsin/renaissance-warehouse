#!/usr/bin/env python3
"""Unit tests for the email-thread-sync PURE functions — NO network, NO db.

Covers the spec's load-bearing transforms (FINALIZED-SPEC §0 resolutions + §7):
  * cleaner: spintax {a|b} and merge {{field}} NEVER survive; quoted history is cut (G3).
  * thread_key derivation: = campaign_id (R1), NOT the per-lead suffix; null-campaign IM
    reply falls back to 'unattributed:'||anchor (R1a). lead_anchor_key = the suffix.
  * direction from ue_type ALONE (R9): 2->inbound; 1,3 (and a forwarded ue=3 where neither
    from/to is the lead) -> outbound.
  * step_path stays a raw STRING ('0_0_2'), never int()'d to NULL (R5).
  * lead_email lowercased+trimmed; PK from item['id'] not the RFC822 header (R6).

Run from the renaissance-warehouse root with the repo venv python:
    python scripts/test_email_thread_sync_units.py
(exit 0 = all pass; prints a per-test ✓ line.)
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable when run as a bare script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.email_body_clean import (  # noqa: E402
    clean_body,
    clean_html,
    clean_subject,
    clean_text,
    has_spintax_or_merge,
)
from entities.email_thread_sync import transform_item  # noqa: E402

_PASS = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"  ✓ {name}")
    else:
        _FAIL += 1
        print(f"  ✗ {name}  {detail}")


# ── cleaner (G3 — spintax/merge never survive) ──────────────────────────────────
def test_cleaner_spintax_never_survives():
    # spintax collapses to its first option; result has no brace-pipe.
    out = clean_text("Hi {there|y'all|folks}, quick question {{first_name}} about funding")
    check("spintax collapsed (no brace-pipe)", not has_spintax_or_merge(out), repr(out))
    check("merge field dropped", "{{first_name}}" not in out, repr(out))
    check("spintax first-option kept", "there" in out, repr(out))

    # nested spintax fully collapses.
    out2 = clean_text("{a {b|c}|d}")
    check("nested spintax collapses", not has_spintax_or_merge(out2), repr(out2))

    # an HTML body with spintax-looking text still ends brace-free (template path, G3).
    out3 = clean_html("<html><body><p>Offer {now|today}</p></body></html>")
    check("html spintax stripped", not has_spintax_or_merge(out3), repr(out3))


def test_cleaner_cuts_quoted_history():
    html = (
        "<html><body><div>Sounds great, let's talk.</div>"
        '<div class="reply-timestamp-box">On Tue wrote:</div>'
        "<blockquote>quoted older email here</blockquote></body></html>"
    )
    out = clean_html(html)
    check("top message kept", "Sounds great" in out, repr(out))
    check("quoted history cut", "quoted older email" not in out, repr(out))

    # plain-text "On ... wrote:" marker is cut too.
    txt = "Yes I'm interested.\n\nOn Mon, Jun 2, 2026 at 9am John <j@x.com> wrote:\n> old stuff"
    out2 = clean_text(txt)
    check("text quote marker cut", "old stuff" not in out2 and "interested" in out2, repr(out2))


def test_subject_spintax_never_survives():
    # G3 scans `subject` too (incl. source='template'); a spintaxed/merge subject must be
    # brace-free after clean_subject, and transform_item must route the subject through it.
    raw = "Re: {funding|capital} for {{company}} — quick q"
    cs = clean_subject(raw)
    check("clean_subject strips spintax/merge", not has_spintax_or_merge(cs), repr(cs))
    check("clean_subject keeps first option", "funding" in cs, repr(cs))
    check("clean_subject None -> None (column stays NULL)", clean_subject(None) is None)
    # transform_item must apply it (a raw spintaxed subject must not survive into the row).
    row = transform_item(_item(subject="{a|b} hello {{x}}"), "o", "w", "n")
    check("transform_item subject is spintax-free (G3)",
          not has_spintax_or_merge(row["subject"]), repr(row["subject"]))


def test_clean_body_dispatch():
    check("dict body prefers html", clean_body({"html": "<p>hello</p>", "text": "x"}) == "hello")
    check("str body cleaned", clean_body("plain {a|b} text").find("{") == -1)
    check("none body -> empty", clean_body(None) == "")


# ── transform: thread_key (R1/R1a) + lead_anchor_key ────────────────────────────
def _item(**over):
    base = {
        "id": "uuid-1", "message_id": "<RFC-HEADER@x>", "thread_id": "34-iZEyABC",
        "campaign_id": "34abc-campaign", "lead": "JESI@Vallure.com ", "ue_type": 1,
        "step": "0_0_2", "subject": "S", "body": {"html": "<p>hi</p>"},
        "from_address_email": "us@inbox.com", "to_address_email_list": ["jesi@vallure.com"],
        "eaccount": "us@inbox.com", "timestamp_email": "2026-06-28T10:00:00Z",
        "organization_id": "org-9",
    }
    base.update(over)
    return base


def test_thread_key_is_campaign_not_suffix():
    row = transform_item(_item(), org_id="org-9", ws_slug="renaissance-4", fetched_at="now")
    check("thread_key == campaign_id (R1)", row["thread_key"] == "34abc-campaign", row["thread_key"])
    check("lead_anchor_key == suffix (R1)", row["lead_anchor_key"] == "iZEyABC", row["lead_anchor_key"])
    check("thread_key NOT the suffix", row["thread_key"] != "iZEyABC")


def test_thread_key_null_campaign_falls_back_to_anchor():
    row = transform_item(_item(campaign_id=None, thread_id="aa-SUF123"),
                         org_id="org-9", ws_slug="renaissance-4", fetched_at="now")
    check("null campaign -> unattributed:anchor (R1a)",
          row["thread_key"] == "unattributed:SUF123", row["thread_key"])


# ── transform: direction from ue_type ALONE (R9) ────────────────────────────────
def test_direction_from_ue_type_only():
    r1 = transform_item(_item(ue_type=1), "o", "w", "n")
    r2 = transform_item(_item(ue_type=2), "o", "w", "n")
    r3 = transform_item(_item(ue_type=3), "o", "w", "n")
    check("ue 1 -> outbound", r1["direction"] == "outbound", r1["direction"])
    check("ue 2 -> inbound", r2["direction"] == "inbound", r2["direction"])
    check("ue 3 -> outbound", r3["direction"] == "outbound", r3["direction"])
    # forwarded ue=3 where neither from nor to is the lead must STILL be outbound (R9 — no heuristic).
    r3f = transform_item(
        _item(ue_type=3, from_address_email="alias@third.com",
              to_address_email_list=["other@third.com"]),
        "o", "w", "n",
    )
    check("forwarded ue=3 still outbound (no from/to heuristic)", r3f["direction"] == "outbound")


# ── transform: step_path raw string (R5) + lead lowercase + PK from id (R6) ──────
def test_step_path_stays_string():
    row = transform_item(_item(step="0_0_2"), "o", "w", "n")
    check("step_path is the raw string '0_0_2' (R5)", row["step_path"] == "0_0_2", repr(row["step_path"]))
    check("step_path not int-coerced to NULL", row["step_path"] is not None)
    # a NULL step (reply) stays NULL.
    rown = transform_item(_item(step=None, ue_type=2), "o", "w", "n")
    check("null step stays None on reply", rown["step_path"] is None)


def test_lead_lowercased_and_pk_from_id():
    row = transform_item(_item(), "o", "w", "n")
    check("lead lowercased+trimmed", row["lead_email"] == "jesi@vallure.com", repr(row["lead_email"]))
    check("PK message_id from item['id'] (R6)", row["message_id"] == "uuid-1", row["message_id"])
    check("rfc_message_id = RFC822 header (R6)", row["rfc_message_id"] == "<RFC-HEADER@x>")
    check("workspace_id = the SLUG passed (R2)", row["workspace_id"] == "w")


def test_missing_id_dropped():
    check("item with no id -> None (cannot be a PK)",
          transform_item(_item(id=None), "o", "w", "n") is None)


# ── apply: ceiling-hit quarantine + idempotency (integration, temp DuckDB) ───────
def _atom_ddl() -> str:
    """Minimal raw_instantly_email_message DDL (the columns _apply_core writes)."""
    return (
        "CREATE TABLE raw_instantly_email_message ("
        "message_id VARCHAR PRIMARY KEY, rfc_message_id VARCHAR, thread_id VARCHAR, "
        "thread_key VARCHAR, lead_anchor_key VARCHAR, workspace_id VARCHAR NOT NULL, "
        "organization_id VARCHAR, campaign_id VARCHAR, lead_email VARCHAR NOT NULL, "
        "direction VARCHAR NOT NULL, ue_type INTEGER, step_path VARCHAR, subject VARCHAR, "
        "body_text VARCHAR, body_html VARCHAR, from_email VARCHAR, to_emails VARCHAR, "
        "eaccount VARCHAR, message_at TIMESTAMPTZ, source VARCHAR DEFAULT 'instantly', "
        "api_response_raw VARCHAR, _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR)"
    )


def _stage_row(mid, ws, lead, ts):
    return {
        "message_id": mid, "rfc_message_id": None, "thread_id": "34-suf", "thread_key": "c1",
        "lead_anchor_key": "suf", "workspace_id": ws, "organization_id": "o", "campaign_id": "c1",
        "lead_email": lead, "direction": "outbound", "ue_type": 1, "step_path": "0_0_1",
        "subject": "s", "body_text": "hi", "body_html": None, "from_email": "us@x.com",
        "to_emails": lead, "eaccount": "us@x.com", "message_at": ts, "source": "instantly",
        "api_response_raw": "{}", "fetched_at": "2026-06-28T10:00:00Z",
    }


def test_ceiling_hit_excludes_ws_and_idempotent():
    """A ceiling-hit workspace's rows must NOT commit (so its max(message_at) never advances),
    while a clean workspace DOES; and a no-op re-apply reports messages_upserted_changed=0."""
    import json as _json
    import tempfile

    import duckdb  # available in the droplet venv

    from entities import email_thread_sync as ets

    tmpdir = tempfile.mkdtemp(prefix="ets_test_")
    stage = Path(tmpdir) / "stage.jsonl"
    rows = [
        _stage_row("clean-1", "ws-good", "a@x.com", "2026-06-28T10:00:00Z"),
        _stage_row("trunc-1", "ws-ceiling", "b@x.com", "2026-06-28T11:00:00Z"),
    ]
    stage.write_text("\n".join(_json.dumps(r) for r in rows) + "\n")

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute(_atom_ddl())
    # core.email_thread is referenced by interest_status_guard; stub a trivial view so it
    # doesn't error (guard is best-effort and degrades to a warning anyway).
    con.execute(
        "CREATE VIEW core.email_thread AS SELECT workspace_id, lead_email, "
        "NULL::VARCHAR AS lead_interest_status FROM raw_instantly_email_message"
    )

    res = ets._apply_core(con, str(stage), run_id="rid1", ceiling_excluded={"ws-ceiling"})

    good = con.execute(
        "SELECT count(*) FROM raw_instantly_email_message WHERE workspace_id='ws-good'"
    ).fetchone()[0]
    bad = con.execute(
        "SELECT count(*) FROM raw_instantly_email_message WHERE workspace_id='ws-ceiling'"
    ).fetchone()[0]
    bad_wm = con.execute(
        "SELECT max(message_at) FROM raw_instantly_email_message WHERE workspace_id='ws-ceiling'"
    ).fetchone()[0]

    check("clean ws committed", good == 1, f"good={good}")
    check("ceiling-hit ws NOT committed (watermark not advanced)", bad == 0, f"bad={bad}")
    check("ceiling-hit ws max(message_at) stays NULL", bad_wm is None, repr(bad_wm))
    check("ceiling_excluded reported", res.get("ceiling_excluded") == ["ws-ceiling"], repr(res))
    check("messages_upserted == manifest line count", res["messages_upserted"] == 1, repr(res))
    check("first apply: 1 changed (new row)", res["messages_upserted_changed"] == 1, repr(res))

    # Re-apply the SAME stage (no ceiling now) — clean ws is a no-op (changed=0 for it); a
    # genuine no-op re-run of an already-committed payload reports messages_upserted_changed=0.
    only_good = Path(tmpdir) / "stage_good.jsonl"
    only_good.write_text(_json.dumps(rows[0]) + "\n")
    res2 = ets._apply_core(con, str(only_good), run_id="rid2", ceiling_excluded=set())
    check("re-apply no-op: messages_upserted_changed=0 (G2)",
          res2["messages_upserted_changed"] == 0, repr(res2))
    con.close()


# ── core.email_thread n_seq_sends: deduped WITHIN A LEAD, not across leads ───────
def _seq_dedup_thread_view_sql() -> str:
    """The EXACT core.email_thread seq_dedup + rollup logic from sql/ddl/1036, on a base table
    named `m` (so the test exercises the SAME windowed-dedup grain the shipped DDL uses).
    Partition MUST be (workspace_id, lead_email, thread_key, step_path) — NOT (thread_key,
    step_path) — or a multi-lead campaign undercounts every lead but one."""
    return """
    WITH seq_dedup AS (
        SELECT workspace_id, lead_email, thread_key, count(*) AS n_seq_sends
        FROM (
            SELECT workspace_id, lead_email, thread_key, step_path,
                   row_number() OVER (
                       PARTITION BY workspace_id, lead_email, thread_key, step_path
                       ORDER BY message_at DESC NULLS LAST
                   ) AS rn
            FROM m WHERE ue_type = 1
        ) s WHERE rn = 1
        GROUP BY workspace_id, lead_email, thread_key
    )
    SELECT m.lead_email, coalesce(any_value(sd.n_seq_sends), 0) AS n_seq_sends
    FROM m
    LEFT JOIN seq_dedup sd
           ON sd.workspace_id = m.workspace_id
          AND sd.lead_email   = m.lead_email
          AND sd.thread_key   = m.thread_key
    GROUP BY m.workspace_id, m.lead_email, m.thread_key
    """


def test_n_seq_sends_dedups_within_lead_not_across_leads():
    """REGRESSION (round-3 blocking finding): thread_key=campaign_id is SHARED across every lead
    in a campaign. Two leads in camp1 each sent steps 0_0_0 & 0_0_1 must EACH report
    n_seq_sends=2. With the buggy (thread_key, step_path)-only partition, one lead reads 2 and the
    other reads 0/undercounted. The fixed (workspace_id, lead_email, thread_key, step_path)
    partition makes each lead report its own distinct-step count."""
    import duckdb  # droplet venv

    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE m (workspace_id VARCHAR, lead_email VARCHAR, thread_key VARCHAR, "
        "step_path VARCHAR, ue_type INTEGER, message_at TIMESTAMPTZ)"
    )
    # Two leads, SAME campaign (thread_key='camp1'), each with the same two step_paths.
    con.execute("""
        INSERT INTO m VALUES
          ('ws','leadA@x.com','camp1','0_0_0',1,'2026-06-01T10:00:00Z'),
          ('ws','leadA@x.com','camp1','0_0_1',1,'2026-06-02T10:00:00Z'),
          ('ws','leadB@x.com','camp1','0_0_0',1,'2026-06-03T10:00:00Z'),
          ('ws','leadB@x.com','camp1','0_0_1',1,'2026-06-04T10:00:00Z')
    """)
    rows = dict(con.execute(_seq_dedup_thread_view_sql()).fetchall())
    con.close()
    check("leadA n_seq_sends == 2 (its own distinct steps)", rows.get("leadA@x.com") == 2, repr(rows))
    check("leadB n_seq_sends == 2 (NOT 0 — not deduped across leads)", rows.get("leadB@x.com") == 2, repr(rows))

    # And a genuine RESEND (same step, new id, later ts) within ONE lead still counts ONCE.
    con2 = duckdb.connect(":memory:")
    con2.execute(
        "CREATE TABLE m (workspace_id VARCHAR, lead_email VARCHAR, thread_key VARCHAR, "
        "step_path VARCHAR, ue_type INTEGER, message_at TIMESTAMPTZ)"
    )
    con2.execute("""
        INSERT INTO m VALUES
          ('ws','leadA@x.com','camp1','0_0_0',1,'2026-06-01T10:00:00Z'),
          ('ws','leadA@x.com','camp1','0_0_0',1,'2026-06-05T10:00:00Z'),
          ('ws','leadA@x.com','camp1','0_0_1',1,'2026-06-02T10:00:00Z')
    """)
    rows2 = dict(con2.execute(_seq_dedup_thread_view_sql()).fetchall())
    con2.close()
    check("resend of same step counts once (2 distinct steps)", rows2.get("leadA@x.com") == 2, repr(rows2))


# ── enumerate_orgs: dedup by the WORKSPACE id, not organization_id (FIX 1) ───────
class _FakeWorkspaceClient:
    """Stand-in for sources.instantly.InstantlyClient used as a context manager. Returns a
    canned /workspaces/current payload (keyed by the api_key it was constructed with)."""

    _BY_KEY: dict = {}  # api_key -> get_current_workspace() payload

    def __init__(self, api_key: str):
        self._key = api_key

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_current_workspace(self) -> dict:
        return dict(self._BY_KEY[self._key])


class _FakeCreds:
    def __init__(self, slug_to_key: dict):
        self._m = slug_to_key

    def instantly_workspace_keys(self) -> dict:
        return dict(self._m)


def test_enumerate_dedups_by_workspace_id_not_org():
    """FIX 1 (completeness, blocking): /workspaces/current returns BOTH `id` (workspace UUID)
    and `organization_id` (parent org) as DISTINCT values. Two DISTINCT workspaces that SHARE
    one organization_id must BOTH be enumerated (under their own slugs) — deduping by
    organization_id would silently DROP the second workspace's replied-lead threads (DoD-2/G6).
    Two keys for the SAME workspace id must collapse to one. Mirrors entities/instantly_replies.py
    + entities/workspace.py, which both dedup on w['id']."""
    import duckdb  # droplet venv

    from entities import email_thread_sync as ets

    # 3 env slugs:
    #   ren4  -> ws-uuid-A, org-shared      (distinct workspace, shared org)
    #   ren5  -> ws-uuid-B, org-shared      (distinct workspace, SAME org as ren4)
    #   ren5b -> ws-uuid-B, org-shared      (DUPLICATE of ren5 — same workspace id -> collapses)
    payloads = {
        "k-ren4":  {"id": "ws-uuid-A", "organization_id": "org-shared"},
        "k-ren5":  {"id": "ws-uuid-B", "organization_id": "org-shared"},
        "k-ren5b": {"id": "ws-uuid-B", "organization_id": "org-shared"},
    }
    _FakeWorkspaceClient._BY_KEY = payloads
    creds = _FakeCreds({"ren4": "k-ren4", "ren5": "k-ren5", "ren5b": "k-ren5b"})

    # core.workspace maps each DISTINCT workspace UUID to its own canonical slug.
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute("CREATE TABLE core.workspace (workspace_id VARCHAR, slug VARCHAR)")
    con.execute(
        "INSERT INTO core.workspace VALUES ('ws-uuid-A','renaissance-4'),('ws-uuid-B','renaissance-5')"
    )

    orig = ets.InstantlyClient
    ets.InstantlyClient = _FakeWorkspaceClient
    try:
        workspaces, diag = ets.enumerate_orgs(creds, con)
    finally:
        ets.InstantlyClient = orig
    con.close()

    # BOTH distinct workspaces enumerated (shared org did NOT collapse them) -> completeness.
    check("both distinct workspaces enumerated (shared org NOT collapsed)",
          set(workspaces.keys()) == {"ws-uuid-A", "ws-uuid-B"}, repr(sorted(workspaces.keys())))
    check("dedup keyed by WORKSPACE id, not organization_id (2 workspaces, not 1)",
          diag["distinct_workspaces"] == 2, repr(diag))
    # each pulled under its OWN canonical slug.
    slugs = {wsid: tup[0] for wsid, tup in workspaces.items()}
    check("ws-uuid-A -> renaissance-4 slug", slugs["ws-uuid-A"] == "renaissance-4", repr(slugs))
    check("ws-uuid-B -> renaissance-5 slug", slugs["ws-uuid-B"] == "renaissance-5", repr(slugs))
    # real organization_id retained as provenance (3rd tuple element), NOT the dedup key.
    orgs = {wsid: tup[2] for wsid, tup in workspaces.items()}
    check("organization_id retained as provenance (A)", orgs["ws-uuid-A"] == "org-shared", repr(orgs))
    check("organization_id retained as provenance (B)", orgs["ws-uuid-B"] == "org-shared", repr(orgs))
    # the DUPLICATE workspace-id key (ren5b) collapsed to one (same workspace, second slug).
    check("same workspace id under 2 slugs collapses to one (dup_collapsed records it)",
          diag["dup_collapsed"] == ["ren5b"], repr(diag["dup_collapsed"]))


# ── apply: a NULL-degraded re-pull must NOT wipe committed-good columns (FIX 2) ───
def test_degraded_repull_does_not_null_out_columns():
    """FIX 2 (idempotency, blocking): a later re-pull of an EXISTING message_id that arrives with
    NULL/'' values for mutable columns must NOT clobber the committed-good values. Mirrors the
    existing G2 ceiling/idempotency test; runs on DuckDB. Asserts message_at, campaign_id,
    from_email, to_emails, eaccount, thread_key are all PRESERVED on a degraded re-apply (only
    api_response_raw/_loaded_at/_run_id may change)."""
    import json as _json
    import tempfile

    import duckdb  # droplet venv

    from entities import email_thread_sync as ets

    tmpdir = tempfile.mkdtemp(prefix="ets_degrade_")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute(_atom_ddl())
    con.execute(
        "CREATE VIEW core.email_thread AS SELECT workspace_id, lead_email, "
        "NULL::VARCHAR AS lead_interest_status FROM raw_instantly_email_message"
    )

    # 1) seed a FULLY-POPULATED row.
    good = _stage_row("mid-degrade", "ws-good", "lead@x.com", "2026-06-28T10:00:00Z")
    good.update({
        "rfc_message_id": "<RFC@x>", "thread_id": "34-suf", "thread_key": "camp-1",
        "lead_anchor_key": "suf", "organization_id": "org-9", "campaign_id": "camp-1",
        "from_email": "us@inbox.com", "to_emails": "lead@x.com", "eaccount": "us@inbox.com",
        "subject": "Hello", "body_text": "real body", "body_html": "<p>real body</p>",
    })
    seed = Path(tmpdir) / "seed.jsonl"
    seed.write_text(_json.dumps(good) + "\n")
    ets._apply_core(con, str(seed), run_id="seed", ceiling_excluded=set())

    # Snapshot the committed business payload BEFORE the degraded re-pull (everything except
    # api_response_raw/_loaded_at/_run_id, which are the only legitimately-mutable columns). The
    # non-destructive guarantee = this payload is byte-identical after a degraded re-pull.
    _payload_cols = (
        "message_id, rfc_message_id, thread_id, thread_key, lead_anchor_key, workspace_id, "
        "organization_id, campaign_id, lead_email, direction, ue_type, step_path, subject, "
        "body_text, body_html, from_email, to_emails, eaccount, message_at, source"
    )
    payload_before = con.execute(
        f"SELECT {_payload_cols} FROM raw_instantly_email_message WHERE message_id='mid-degrade'"
    ).fetchone()

    # 2) a DEGRADED re-pull: SAME message_id, but message_at/campaign_id/from_email/to_emails/
    #    eaccount/thread_key/subject/body_* all NULL or '' (simulating a later empty/partial pull).
    degraded = _stage_row("mid-degrade", "ws-good", "lead@x.com", None)  # message_at NULL
    degraded.update({
        "rfc_message_id": None, "thread_id": None, "thread_key": "", "lead_anchor_key": "",
        "organization_id": "", "campaign_id": "", "from_email": "", "to_emails": "",
        "eaccount": "", "subject": "", "body_text": "", "body_html": "",
        "api_response_raw": "{\"degraded\":true}",
    })
    deg = Path(tmpdir) / "degraded.jsonl"
    deg.write_text(_json.dumps(degraded) + "\n")
    res = ets._apply_core(con, str(deg), run_id="degrade", ceiling_excluded=set())

    payload_after = con.execute(
        f"SELECT {_payload_cols} FROM raw_instantly_email_message WHERE message_id='mid-degrade'"
    ).fetchone()
    row = con.execute(
        "SELECT message_at, campaign_id, from_email, to_emails, eaccount, thread_key, "
        "thread_id, organization_id, rfc_message_id, subject, body_text, source, "
        "api_response_raw FROM raw_instantly_email_message WHERE message_id='mid-degrade'"
    ).fetchone()
    con.close()
    (message_at, campaign_id, from_email, to_emails, eaccount, thread_key, thread_id,
     organization_id, rfc_message_id, subject, body_text, source, api_raw) = row

    check("message_at NOT nulled (watermark/order key preserved)", message_at is not None, repr(message_at))
    check("campaign_id NOT nulled", campaign_id == "camp-1", repr(campaign_id))
    check("from_email NOT nulled", from_email == "us@inbox.com", repr(from_email))
    check("to_emails NOT nulled", to_emails == "lead@x.com", repr(to_emails))
    check("eaccount NOT nulled", eaccount == "us@inbox.com", repr(eaccount))
    check("thread_key NOT nulled", thread_key == "camp-1", repr(thread_key))
    check("thread_id NOT nulled", thread_id == "34-suf", repr(thread_id))
    check("organization_id NOT nulled", organization_id == "org-9", repr(organization_id))
    check("rfc_message_id NOT nulled", rfc_message_id == "<RFC@x>", repr(rfc_message_id))
    check("subject NOT nulled (existing G2 body-protect parity)", subject == "Hello", repr(subject))
    check("body_text NOT nulled", body_text == "real body", repr(body_text))
    check("source NOT nulled", source == "instantly", repr(source))
    # the ONLY column a degraded re-pull legitimately overwrites: api_response_raw.
    check("api_response_raw IS overwritten (latest raw, by design)",
          api_raw == "{\"degraded\":true}", repr(api_raw))
    # The committed business payload (every column except api_response_raw/_loaded_at/_run_id) is
    # byte-identical before vs after the degraded re-pull — the true non-destructive invariant
    # (FIX 2). messages_upserted_changed legitimately reports 1 here because the staged INPUT is
    # genuinely degraded/different; the GUARANTEE is that the merge preserved the committed row.
    check("committed business payload byte-identical after degraded re-pull (non-destructive)",
          payload_after == payload_before, f"before={payload_before!r} after={payload_after!r}")
    check("manifest still records the touched id (rollback parity)",
          res["messages_upserted"] == 1, repr(res))


def main() -> int:
    tests = [
        test_cleaner_spintax_never_survives,
        test_cleaner_cuts_quoted_history,
        test_subject_spintax_never_survives,
        test_clean_body_dispatch,
        test_thread_key_is_campaign_not_suffix,
        test_thread_key_null_campaign_falls_back_to_anchor,
        test_direction_from_ue_type_only,
        test_step_path_stays_string,
        test_lead_lowercased_and_pk_from_id,
        test_missing_id_dropped,
        test_n_seq_sends_dedups_within_lead_not_across_leads,
        test_ceiling_hit_excludes_ws_and_idempotent,
        test_enumerate_dedups_by_workspace_id_not_org,
        test_degraded_repull_does_not_null_out_columns,
    ]
    for t in tests:
        print(f"\n{t.__name__}:")
        t()
    print(f"\n{'='*50}\n  {_PASS} passed, {_FAIL} failed")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
