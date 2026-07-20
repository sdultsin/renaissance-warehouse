-- 1147: CM self-serve labeled-opportunity views [2026-07-19]
-- Why: CMs must judge campaigns on OUR labeled real opportunities, not only
-- Instantly-native opps — but campaign attribution is NULL on ~96% of label rows
-- (the all-time backfill wrote label events without campaign_id, and
-- message_ref_id='alltime_thread' joins to no reply surface). These views ship
-- the attribution workaround once, so a CM's query is a one-liner.
--
--   SELECT * FROM dash.v_cm_campaign_labels WHERE workspace = 'Funding 4 (Sam)';
--
-- Attribution rule (validated 2026-07-19 on F2/F4: 96-98% of labeled leads
-- attributed; Eyver 86%, R1 61% — R1 replies are under-covered in the reply
-- surfaces): each labeled lead (labeler 1.1.x, current label) maps to the
-- non-auto reply nearest its label message_ts in the same workspace, preferring
-- a variant-bearing row within 24h (pipeline-source reply rows never carry
-- variant; the instantly-source copy of the same reply does).

CREATE OR REPLACE VIEW dash.lead_label_attributed AS
WITH cur AS (
  SELECT l.workspace_slug, l.lead_email,
         l.current_reply_label AS label,
         l.current_label_message_ts AS label_ts,
         l.meeting_booked
  FROM core.v_reply_label_current l
  WHERE l.labeler_version LIKE '1.1%'
    AND l.current_reply_label IN ('opportunity','engagement','confused','not_interested')
),
j AS (
  SELECT c.workspace_slug, c.lead_email, c.label, c.label_ts, c.meeting_booked,
         w.name AS workspace_name,
         r.campaign_id, r.campaign_name, r.step, r.variant, r.reply_timestamp,
         row_number() OVER (
           PARTITION BY c.workspace_slug, c.lead_email
           ORDER BY (NOT (r.variant IS NOT NULL
                          AND abs(epoch(r.reply_timestamp - c.label_ts)) <= 86400)),
                    abs(epoch(r.reply_timestamp - c.label_ts))) AS rn
  FROM cur c
  LEFT JOIN core.workspace w ON w.slug = c.workspace_slug
  LEFT JOIN derived.v_reply_canonical r
         ON r.lead_email = c.lead_email
        AND NOT r.is_auto_reply
        AND r.workspace_name = w.name
)
SELECT workspace_slug, workspace_name, lead_email, label, label_ts, meeting_booked,
       campaign_id, campaign_name, step, variant, reply_timestamp
FROM j
WHERE rn = 1;

-- Campaign-grain rollup: lifetime scoreboard + both opportunity definitions.
-- For a date-windowed cut, aggregate dash.lead_label_attributed on label_ts
-- directly (this view is lifetime, matching v_campaign_scoreboard's grain).
CREATE OR REPLACE VIEW dash.v_cm_campaign_labels AS
SELECT sb.workspace, sb.name AS campaign_name, sb.cm_name, sb.status,
       sb.sent, sb.human_replies,
       sb.opps AS native_opps,
       COUNT(a.lead_email) FILTER (WHERE a.label = 'opportunity') AS real_opps,
       COUNT(a.lead_email) FILTER (WHERE a.label = 'engagement')  AS engaged,
       COUNT(a.lead_email) FILTER (WHERE a.label = 'confused')    AS confused,
       COUNT(a.lead_email) FILTER (WHERE a.label = 'not_interested') AS not_interested,
       COUNT(a.lead_email) FILTER (WHERE a.label = 'opportunity' AND a.meeting_booked) AS real_opps_booked,
       ROUND(sb.sent * 1.0 / NULLIF(sb.opps, 0)) AS eop_native,
       ROUND(sb.sent * 1.0 / NULLIF(COUNT(a.lead_email) FILTER (WHERE a.label = 'opportunity'), 0)) AS eop_real,
       sb.meetings, sb.kpi, sb.opp_to_meeting_pct,
       sb.first_send_date, sb.last_send_date, sb.campaign_id
FROM main.v_campaign_scoreboard sb
LEFT JOIN dash.lead_label_attributed a ON a.campaign_id = sb.campaign_id
GROUP BY ALL;
