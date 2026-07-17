-- [2026-07-17] Sam override: the labels chain-swap (kpi_labels_daily + kpi_compute v2) is
-- REVERTED — the merged KPIs tab now blends the conversion feed client-side. kpi_compute v3
-- = v1 semantics (Instantly-native sends/opps from kpi_workspace_daily) with ONE addition:
-- excluded workspaces (Warm leads) are EMITTED with is_excluded=true instead of dropped,
-- enabling the client-side "Include Warm Leads" toggle (the frontend recomputes all totals
-- from rows and ignores the edge fn's row sums).
drop table if exists public.kpi_labels_daily;
drop function if exists public.kpi_compute(date, date, jsonb);
create function public.kpi_compute(p_start date, p_end date, p_bookings jsonb)
 returns table(ws text, booked bigint, sent bigint, sent_per_booking numeric,
               opps bigint, opp_per_booking numeric, is_excluded boolean)
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
    select workspace ws, sum(sent)::bigint sent, sum(opps)::bigint opps
    from kpi_workspace_daily where date between p_start and p_end group by 1)
  select t.ws, t.booked, t.sent,
         case when t.booked>0 then round(t.sent::numeric/t.booked) else null end,
         t.opps,
         case when t.booked>0 then round(t.opps::numeric/t.booked, 1) else null end,
         t.excl
  from (
    -- v3: excluded workspaces (Warm leads) are emitted flagged, NOT dropped — the client
    -- decides (default excluded; "Include Warm Leads" toggle includes them everywhere).
    select k.name ws, coalesce(b.booked,0) booked, coalesce(s.sent,0) sent, coalesce(s.opps,0) opps,
           coalesce(k.exclude_from_kpi,false) excl, k.sort_order so
    from kpi_workspaces k
    left join agg_ws s on s.ws=k.name
    left join bk_ws b on b.ws=k.name
    union all
    select '(Unattributed)', booked, 0, 0, false, 999 from bk_ws where ws='(Unattributed)'
  ) t
  order by t.so, t.ws;
$function$;
grant execute on function public.kpi_compute(date, date, jsonb) to service_role, authenticated, anon;
notify pgrst, 'reload schema';
