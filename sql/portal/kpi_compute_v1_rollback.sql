CREATE OR REPLACE FUNCTION public.kpi_compute(p_start date, p_end date, p_bookings jsonb)
 RETURNS TABLE(ws text, booked bigint, sent bigint, sent_per_booking numeric, opps bigint, opp_per_booking numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
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
    select workspace ws, sum(sent)::bigint sent, sum(opps)::bigint opps
    from kpi_workspace_daily where date between p_start and p_end group by 1)
  select t.ws, t.booked, t.sent,
         case when t.booked>0 then round(t.sent::numeric/t.booked) else null end,
         t.opps,
         case when t.booked>0 then round(t.opps::numeric/t.booked, 1) else null end
  from (
    -- Workspaces flagged exclude_from_kpi (e.g. Warm leads) are dropped entirely: their sends,
    -- opportunities AND bookings never enter the KPI or its totals.
    select k.name ws, coalesce(b.booked,0) booked, coalesce(s.sent,0) sent, coalesce(s.opps,0) opps, k.sort_order so
    from kpi_workspaces k
    left join agg_ws s on s.ws=k.name
    left join bk_ws b on b.ws=k.name
    where not coalesce(k.exclude_from_kpi,false)
    union all
    select '(Unattributed)', booked, 0, 0, 999 from bk_ws where ws='(Unattributed)'
  ) t
  order by t.so, t.ws;
$function$
