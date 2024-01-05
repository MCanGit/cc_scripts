select 
    mmt.srt_id,
    mmt.enterprise_id,
    mmt.role,
    mmt.employee_status,
    mmt.skills,
    mmt.cluster,
    mmt.workflow,
    mmt.current_workflow,
    mmt.team_lead,
    mmt.ops_lead_eid,
    mmt.roster_date,
    mmt.week_end_date_text,
    mmt.report_month,
    mmt.billable,
    iw.highlevel_status as iw_highlevel_status,
    iw.updated_status as iw_updated_status,
    iw.attendance_status as iw_attendance_status,
    iw.srt_id as iw_srt_id,
    iw.enterprise_id as iw_enterprise_id,
    iw.shift_date as iw_shift_date,
    iw.shift_status as iw_shift_status,
    iw.system_status as iw_system_status,
    iw.date_txt as iw_date_txt,
    iw.week_end_date_txt as iw_week_end_date_txt, 
    iw.report_month as iw_report_month,
    ROUND(cast(iw.actual_time_min as real)/60, 2) as iw_actual_time,
    srtf."total hours" as srtf_total_hrs,
    srtf."completed time" as srtf_completed_time,
    srtf.hc,
    srtf."adjustment types" as srtf_adjustment_type,
    srtf."date",
    ts.ts_total_hours,
    ts.ts_completed_time

FROM opsnav_client000_test.client115_co_mmt_roster_daily as mmt
LEFT JOIN opsnav_client000_test.client115_co_iw_schedule_attendance as iw
ON mmt.enterprise_id = iw.enterprise_id and mmt.roster_date = iw.shift_date
LEFT JOIN "opsnav_client115_test"."lx_srtf" as srtf
ON  mmt.enterprise_id = srtf."enterprise id" and TO_CHAR(mmt.roster_date, 'mm-dd-yyyy') = srtf.date
LEFT JOIN (
        with ts_cte as (
            SELECT distinct 
                reporting_ds,
                work_city,
                actor_id,
                status,  
                time_in_status_mins,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'available') as available,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'coaching') as coaching,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'unavailable') as unavailable,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'break') as break,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'meal') as meal,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'fb_training') as fb_training,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'team_meeting') as team_meeting,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'non-fb-training') as non_fb_training,
                max(CAST(time_in_status_mins as double)) filter (where "status" = 'wellness_support') as well_being

            FROM "opsnav_client000_test"."client115_raw_meta_di_ts_current"

            where 
                work_city = 'Lisbon, Portugal'
    
            group by 1, 2, 3, 4, 5)

            select 
                reporting_ds,
                actor_id,
                sum(available) as available,
                sum(coaching) as coaching,
                sum(unavailable) as unavailable,
                sum(break) as break,
                sum(meal) as meal,
                sum(fb_training) as fb_training,
                sum(team_meeting) as team_meeting,
                sum(non_fb_training) as non_fb_training,
                sum(well_being) as well_being,
                ROUND((sum(COALESCE(available, 0.0)) 
                    + sum(COALESCE(coaching, 0.0)) 
                    + sum(COALESCE(fb_training, 0.0)) 
                    + sum(COALESCE(team_meeting, 0.0)) 
                    + sum(COALESCE(well_being, 0.0)))/60,2) as ts_total_hours,

                ROUND((sum(COALESCE(available, 0.0)) 
                    + sum(COALESCE(coaching, 0.0)) 
                    + sum(COALESCE(fb_training, 0.0)) 
                    + sum(COALESCE(team_meeting, 0.0)) 
                    + sum(COALESCE(well_being, 0.0)) 
                    + sum(COALESCE(non_fb_training, 0.0)) 
                    + sum(COALESCE(meal, 0.0)) 
                    + sum(COALESCE(break, 0.0)))/60,2) as ts_completed_time

            from ts_cte 
            group by 1, 2 ) as ts

ON mmt.srt_id = ts.actor_id and TO_CHAR(mmt.roster_date, 'yyyy-mm-dd') = ts.reporting_ds

where 
    mmt.city = 'Lisbon'
and
    mmt.week_end_date_text = '2023-12-29'

 /*
 
 mmt.week_end_date_text >= '2023-07-07'
 mmt.week_end_date_text >= '2023-01-06' and mmt.week_end_date_text < '2023-07-07'
 */