-- KPI opp swap [2026-07-17, Sam directive]: the booking-form KPIs tab's Opportunities move
-- from Instantly interested-flags (~69% precision) to OUR labeled counts for completed days,
-- with Instantly's auto-count as the PROVISIONAL fallback for any day not yet labeled
-- (i.e. today; also any workspace outside labeler coverage, e.g. Tariffs history).
-- kpi_labels_daily is fed daily by renaissance-warehouse scripts/conversion_booking_feed.py
-- (same day-readiness gate + sanity assertions as the conversion feed).
-- ROLLBACK: apply kpi_compute_v1_rollback.sql (drop first), drop table if desired.

create table if not exists public.kpi_labels_daily (
  date          date   not null,
  workspace     text   not null,   -- kpi_workspaces.name
  sent          bigint,            -- native warehouse facts (denominator for Human RR)
  human_replies bigint,
  positive      bigint,            -- opportunity + engagement labels
  opps_labeled  bigint not null default 0,
  updated_at    timestamptz not null default now(),
  primary key (date, workspace)
);
alter table public.kpi_labels_daily enable row level security;

drop function if exists public.kpi_compute(date, date, jsonb);
create function public.kpi_compute(p_start date, p_end date, p_bookings jsonb)
 returns table(ws text, booked bigint, sent bigint, sent_per_booking numeric,
               opps bigint, opp_per_booking numeric,
               labeled_sent bigint, human_replies bigint, positive bigint,
               opps_provisional bigint)
 language sql stable security definer
as $function$
  with bk as (
    select case when jsonb_array_length(x) >= 3 then nullif(trim(x->>1),'') else null end as ws_hint,
           (x->>(jsonb_array_length(x)-1))::int as n
    from jsonb_array_elements(p_bookings) x),
  bk_ws as (
    select coalesce(kd.name, ka.name, '(Unattributed)') ws, sum(bk.n)::bigint booked
    from bk
    left join kpi_workspaces kd on lower(kd.name) = lower(bk.ws_hint)
    left join kpi_ws_alias a    on lower(a.label) = lower(bk.ws_hint)
    left join kpi_workspaces ka on ka.name = a.ws
    group by 1),
  agg_ws as (
    -- Per workspace-day: labeled counts when a kpi_labels_daily row exists (completed,
    -- gate-passed day), else Instantly's auto-count as the provisional fallback.
    select w.workspace ws,
           sum(w.sent)::bigint sent,
           sum(coalesce(l.opps_labeled, w.opps))::bigint opps,
           sum(case when l.date is null then w.opps else 0 end)::bigint opps_provisional,
           sum(l.sent)::bigint labeled_sent,
           sum(l.human_replies)::bigint human_replies,
           sum(l.positive)::bigint positive
    from kpi_workspace_daily w
    left join kpi_labels_daily l on l.date = w.date and l.workspace = w.workspace
    where w.date between p_start and p_end
    group by 1)
  select t.ws, t.booked, t.sent,
         case when t.booked>0 then round(t.sent::numeric/t.booked) else null end,
         t.opps,
         case when t.booked>0 then round(t.opps::numeric/t.booked, 1) else null end,
         t.labeled_sent, t.human_replies, t.positive, t.opps_provisional
  from (
    -- Workspaces flagged exclude_from_kpi (e.g. Warm leads) are dropped entirely: their sends,
    -- opportunities AND bookings never enter the KPI or its totals.
    select k.name ws, coalesce(b.booked,0) booked, coalesce(s.sent,0) sent, coalesce(s.opps,0) opps,
           s.labeled_sent, s.human_replies, s.positive,
           coalesce(s.opps_provisional,0) opps_provisional, k.sort_order so
    from kpi_workspaces k
    left join agg_ws s on s.ws=k.name
    left join bk_ws b on b.ws=k.name
    where not coalesce(k.exclude_from_kpi,false)
    union all
    select '(Unattributed)', booked, 0, 0, null, null, null, 0, 999 from bk_ws where ws='(Unattributed)'
  ) t
  order by t.so, t.ws;
$function$;

grant execute on function public.kpi_compute(date, date, jsonb) to service_role, authenticated, anon;
notify pgrst, 'reload schema';
