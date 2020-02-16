
---- Latest Utilization

WITH doc AS ( 
		SELECT d.doctor_profile_id, MAX(INITCAP(d.name)) AS doctor_name, MIN(DATE(c.completion_time)) AS doctor_joining_date
	    FROM pe_docstat_91streets_media_technologies."case" c 
	    LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON d.id=c.doctor_id
	    WHERE c.current_status_id=5
	    GROUP BY 1
), 
profile_blocked AS (
	SELECT sh1.*, MIN(created_at) AS profile_accepted_at
	FROM (
		  SELECT doctor_id, created_at AS profile_blocked_at
		  FROM pe_docstat_91streets_media_technologies.status_history
		  WHERE new_status_id!=4
		  GROUP BY 1,2
	) sh1
	LEFT JOIN pe_docstat_91streets_media_technologies.status_history sh2 ON sh1.doctor_id=sh2.doctor_id AND sh2.new_status_id=4 AND sh1.profile_blocked_at<=sh2.created_at
	GROUP BY 1,2
),
approved_online AS (  
	 SELECT dsc.doctor_id, dsc.online_start AS online_start_time, dsc.online_minutes*60 AS online_seconds, dsc.online_end AS online_end_time
	 FROM pe_docstat_91streets_media_technologies.doctor_status_change dsc
	 LEFT JOIN profile_blocked pb ON dsc.doctor_id=pb.doctor_id AND ((online_start>=pb.profile_blocked_at) AND (online_end<=pb.profile_accepted_at OR pb.profile_accepted_at IS NULL))--online_start>=p.profile_accepted_at AND (p.profile_blocked_at IS NULL OR dsc.online_end<=p.profile_blocked_at)
	 WHERE pb.doctor_id IS NULL
	 GROUP BY 1,2,3,4
)
SELECT finale.*,
  --finale.doctor_id, finale.online_start AS online_start_time, dsc.online_minutes*60 AS online_seconds, finale.online_end AS online_end_time
  CASE WHEN finale.last_task_detached_at>=finale.online_end_time 
    THEN finale.last_task_detached_at
    ELSE finale.online_end_time 
  END AS actual_online_end_time,
  CASE WHEN finale.last_task_detached_at>=finale.online_end_time
    THEN DATEDIFF(second,online_start_time,finale.last_task_detached_at)
    ELSE DATEDIFF(second,online_start_time,finale.online_end_time)
  END AS actual_seconds_online
--  SUM(idle_time_secs) AS total_idle_secs,
--  COUNT(CASE WHEN idle_time_secs!=0 THEN idle_time_secs END) AS total_idle_instances
FROM (
	SELECT t.doctor_profile_id,
		   doc.doctor_name, 
		   doc.doctor_joining_date,
		   CASE WHEN DATEDIFF(day,doc.doctor_joining_date,DATE(t.task_assigned_at))>=30 THEN 'Old' ELSE 'New' END AS doctor_type,
		   online_start_time, online_seconds, online_end_time,
		   COUNT(task_id) AS total_tasks,
		   COUNT(CASE WHEN task_action='Rx Prescribed' THEN task_id END) AS tasks_prescribed,
		   COUNT(CASE WHEN task_action='Hold' THEN task_id END) AS tasks_putonhold,
		   COUNT(CASE WHEN task_action='Rescheduled' THEN task_id END) AS tasks_rescheduled,
		   COUNT(CASE WHEN task_action='Reassigned To Admin' THEN task_id END) AS tasks_reassigned,
		   COUNT(CASE WHEN task_action='Doctor Rejected' THEN task_id END) AS tasks_rejected,
		   COUNT(CASE WHEN task_action='Hold' AND call_attempts=0 THEN task_id END) AS tasks_putonhold_withoutcalling,
		   COUNT(CASE WHEN task_type='New' THEN task_id END) AS total_new_tasks,
		   COUNT(CASE WHEN task_type='Hold' THEN task_id END) AS total_hold_tasks,
		--   COUNT(CASE WHEN task_type='Rescheduled' THEN task_id END) AS total_rescheduled_tasks,
		--   COUNT(CASE WHEN task_type='ScheduleNow' THEN task_id END) AS total_schedulenow_tasks,
		--   COUNT(CASE WHEN task_type='Slotted' THEN task_id END) AS total_slotted_tasks,
		   MIN(task_assigned_at) AS first_task_assigned_at,
		--   MAX(task_assigned_at) AS last_task_assigned_at,
		--   MAX(task_actioned_at) AS last_task_actioned_at, 
		   MAX(task_detached_at) AS last_task_detached_at, 
		--   SUM(call_attempts) AS total_call_attempts,
		--   SUM(calls_connected) AS total_calls_connected,
		--   SUM(calls_disconnected) AS total_calls_disconnected,
		   SUM(doctor_inactive_flag) AS tasks_inactive,
		   SUM(doctor_notified_count) AS total_notified_count,
		   SUM(issue_task_flag) AS total_issue_tasks,
		--   COUNT(CASE WHEN min_call_initiated_at IS NOT NULL THEN task_id END) AS tasks_called,
		   COUNT(CASE WHEN min_call_initiated_at<max_call_disconnected_at THEN task_id END) AS tasks_disconnected,
		   COUNT(CASE WHEN task_actioned_at IS NOT NULL THEN task_id END) AS tasks_actioned,
		   COUNT(CASE WHEN min_call_initiated_at<max_call_disconnected_at AND task_action='Rx Prescribed' THEN task_id END) AS tasks_rx_disconnected,
		--   COUNT(CASE WHEN task_action='Rx Prescribed' THEN task_id END) AS tasks_rx_actioned,
		   SUM(DATEDIFF(second,task_assigned_at,min_call_initiated_at)) AS time_to_call,
		   SUM(DATEDIFF(second,min_call_initiated_at, max_call_disconnected_at)) AS total_call_duration,
		   SUM(DATEDIFF(second,max_call_disconnected_at,task_actioned_at)) AS total_time_to_action,
		   SUM(DATEDIFF(second,task_assigned_at,task_actioned_at)) AS total_case_handling_time,
		   SUM(CASE WHEN task_action='Rx Prescribed' THEN DATEDIFF(second,min_call_initiated_at, max_call_disconnected_at) END) AS total_rx_call_duration,
		   SUM(CASE WHEN task_action='Rx Prescribed' THEN DATEDIFF(second,max_call_disconnected_at,task_actioned_at) END) AS total_rx_time_to_action,
		   SUM(CASE WHEN task_action='Rx Prescribed' THEN DATEDIFF(second,task_assigned_at,task_actioned_at) END) AS total_rx_case_handling_time,
		   SUM(DATEDIFF(second,task_assigned_at,task_detached_at)) AS total_case_attachment_time,
		   SUM(CASE WHEN doctor_inactive_flag=1 THEN DATEDIFF(second,task_assigned_at,task_detached_at) END) AS total_inactive_case_attachment_time
	FROM approved_online ao 
	LEFT JOIN data_model.f_docstat_task t ON ao.doctor_id=t.doctor_id AND t.task_assigned_at BETWEEN ao.online_start_time AND ao.online_end_time
	LEFT JOIN doc ON t.doctor_profile_id=doc.doctor_profile_id
	WHERE DATE(ao.online_start_time)>='2019-11-01' AND
			DATE(ao.online_start_time) >= (CASE WHEN (EXTRACT(MONTH FROM CURRENT_DATE)-6)<=0 
									 THEN ((EXTRACT(YEAR FROM CURRENT_DATE)-1)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)+6)||'-'||'01')::DATE 
									 ELSE (EXTRACT(YEAR FROM CURRENT_DATE)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)-6)||'-'||'01')::DATE
								END)
			AND (task_action IS NOT NULL OR doctor_inactive_flag=1)
	GROUP BY 1,2,3,4,5,6,7
) finale
--LEFT JOIN pe_docstat_91streets_media_technologies.doctor_idle_time dit ON finale.doctor_id=dit.doctor_id AND dit.start_time BETWEEN finale.online_start_time AND finale.online_end_time
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35;
		       


--- Old Efficiency

CREATE TEMPORARY TABLE doctor_task AS (
WITH action_benchmarks AS (
	SELECT 'Rx Prescribed' AS task_action, 180 AS benchmark
	UNION
	SELECT 'Hold' AS task_action, 60 AS benchmark
	UNION
	SELECT 'Rescheduled' AS task_action, 120 AS benchmark
	UNION
	SELECT 'Doctor Rejected' AS task_action, 75 AS benchmark
	UNION
	SELECT 'Reassigned To Admin' AS task_action, 0 AS benchmark
	UNION
	SELECT 'Doctor Inactive' AS task_action, 0 AS benchmark
),
doc AS (
	SELECT d1.registration_id AS doctor_registration_id, name as doctor_name, first_case_completed_at AS doc_joining_date, 
			ROW_NUMBER() OVER (PARTITION BY d1.registration_id ORDER BY LENGTH(name) DESC) AS ranking
	FROM pe_docstat_91streets_media_technologies.doctor d1
	LEFT JOIN ( SELECT registration_id, MIN(DATE(c.completion_time)) AS first_case_completed_at
				FROM pe_docstat_91streets_media_technologies."case" c 
				LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON d.id=c.doctor_id
				WHERE c.current_status_id=5
				GROUP BY 1
				) d2 ON d1.registration_id=d2.registration_id
),
task_level AS (
SELECT cl.task_id, c.order_id, case_id, MAX(cl.doctor_id) AS doctor_id, 
		--MIN(cl.created_at) AS task_created_at,
		MIN(CASE WHEN case_log_status_id=2 THEN cl.created_at END) AS task_assigned_at,
		MIN(CASE WHEN case_log_status_id=3 THEN cl.created_at END) AS min_call_initiated_at,
		MAX(CASE WHEN case_log_status_id=3 THEN cl.created_at END) AS max_call_initiated_at,
		MIN(CASE WHEN case_log_status_id IN (4,5) THEN cl.created_at END) AS min_call_disconnected_at,
		MAX(CASE WHEN case_log_status_id IN (4,5) THEN cl.created_at END) AS max_call_disconnected_at,
		MAX(CASE WHEN case_log_status_id IN (6,8,10,11,14,15) THEN cl.created_at END) AS task_actioned_at,
		MAX(CASE WHEN case_log_status_id IN (1,6,8,10,11,14,15) THEN cl.created_at END) AS task_detached_at,
		MAX(CASE WHEN case_log_status_id=6 THEN 'Rx Prescribed'
				 WHEN case_log_status_id=8 THEN 'Docstat Rejected'
				 WHEN case_log_status_id=10 THEN 'Cancelled'
				 WHEN case_log_status_id=11 THEN 'Hold'
				 WHEN case_log_status_id=14 THEN 'Rescheduled'
				 WHEN case_log_status_id=15 THEN CASE WHEN clr.reason_id=99 THEN 'Reassigned To Admin' ELSE 'Doctor Rejected' END
				 END
			) AS task_action,
		COUNT(DISTINCT CASE WHEN case_log_status_id=3 THEN cl.created_at END) AS call_attempts,
		COUNT(DISTINCT CASE WHEN case_log_status_id=4 THEN cl.created_at END) AS calls_connected,
		COUNT(DISTINCT CASE WHEN case_log_status_id=5 THEN cl.created_at END) AS calls_disconnected,
		COUNT(DISTINCT CASE WHEN case_log_status_id=7 THEN cl.created_at END) AS doctor_notified_count,
		MAX(CASE WHEN case_log_status_id=1 THEN 1 ELSE 0 END) AS doctor_inactive_flag,
		MAX(CASE WHEN fdpo.rx_first_prescribed_at<=cl.created_at THEN 1 ELSE 0 END) AS issue_task_flag,
		MAX(ctm.name) AS task_type,
		MAX(rm.reason) AS reject_reason
FROM pe_docstat_91streets_media_technologies.case_log cl
INNER JOIN pe_docstat_91streets_media_technologies."case" c ON cl.case_id=c.id --AND DATE(c.created_at)=(CURRENT_DATE-1)
LEFT JOIN (SELECT task_id, MAX(id) AS case_log_id FROM pe_docstat_91streets_media_technologies.case_log GROUP BY 1) cl2 ON cl.id=cl2.case_log_id
LEFT JOIN pe_docstat_91streets_media_technologies.case_log_statuses_master clsm ON cl.case_log_status_id=clsm.id AND cl2.case_log_id IS NOT NULL
LEFT JOIN pe_docstat_91streets_media_technologies.case_types_master ctm ON cl.case_type_id=ctm.id
LEFT JOIN data_model.f_doctor_program_order fdpo ON c.order_id=fdpo.order_id::VARCHAR
LEFT JOIN pe_docstat_91streets_media_technologies.case_log_reason clr ON cl.id=clr.case_log_id
LEFT JOIN pe_docstat_91streets_media_technologies.reasons_master rm ON clr.reason_id=rm.id
WHERE cl.task_id IS NOT NULL AND DATE(c.created_at)>='2019-10-15'
GROUP BY 1,2,3
ORDER BY 3, 5
)
SELECT xyz.*, (xyz.task_count*ab.benchmark) AS total_expected_handling_time
FROM (
	SELECT doc.doctor_name, 
			DATE(task_assigned_at) AS task_assigned_date,
			EXTRACT(HOUR FROM task_assigned_at) AS task_assigned_hour,
			task_type,
			CASE WHEN tl.task_action IS NULL AND tl.doctor_inactive_flag=1 THEN 'Doctor Inactive' 
				 --WHEN doctor_inactive_flag=0 AND task_action IS NULL THEN 'Abrupt Offline/Internet'
				 ELSE tl.task_action
			END AS task_action,
			COUNT(task_id) AS task_count,
			SUM(issue_task_flag) AS total_issue_tasks,
			SUM(call_attempts) AS total_call_attempts,
			SUM(calls_connected) AS total_calls_connected,
			SUM(calls_disconnected) AS total_calls_disconnected,
			SUM(doctor_notified_count) AS total_doctor_notified_instances,
			COUNT(CASE WHEN min_call_initiated_at IS NOT NULL THEN task_id END) AS tasks_called,
			COUNT(CASE WHEN min_call_disconnected_at IS NOT NULL THEN task_id END) AS tasks_disconnected,
			COUNT(CASE WHEN task_actioned_at IS NOT NULL THEN task_id END) AS tasks_actioned,
			SUM(DATEDIFF(second,task_assigned_at,min_call_initiated_at)) AS time_to_call,
			SUM(DATEDIFF(second,min_call_initiated_at, min_call_disconnected_at)) AS first_call_duration,
			SUM(DATEDIFF(second,max_call_initiated_at, max_call_disconnected_at)) AS last_call_duration,
			SUM(DATEDIFF(second,min_call_initiated_at, max_call_disconnected_at)) AS total_call_duration,
			SUM(DATEDIFF(second,max_call_disconnected_at,task_actioned_at)) AS time_to_action,
			SUM(DATEDIFF(second,task_assigned_at,task_actioned_at)) AS total_actual_handling_time,
			SUM(DATEDIFF(second,task_assigned_at,task_detached_at)) AS total_detachment_time
	FROM task_level tl
	LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON tl.doctor_id=d.id
	LEFT JOIN doc ON d.registration_id=doc.doctor_registration_id AND doc.ranking=1
	WHERE DATE(task_assigned_at) BETWEEN '2019-11-01' AND (CURRENT_DATE-1) --AND (task_action IS NOT NULL OR doctor_inactive_flag=1)
	GROUP BY 1,2,3,4,5
	) xyz
LEFT JOIN action_benchmarks ab ON xyz.task_action=ab.task_action
)
;


--- Latest Efficiency

WITH doc AS ( 
		SELECT d.doctor_profile_id, MAX(INITCAP(d.name)) AS doctor_name, MIN(DATE(c.completion_time)) AS doctor_joining_date
	    FROM pe_docstat_91streets_media_technologies."case" c 
	    LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON d.id=c.doctor_id
	    WHERE c.current_status_id=5
	    GROUP BY 1
)
SELECT fdt1.doctor_profile_id,
		doc.doctor_name, 
		doc.doctor_joining_date,
		CASE WHEN DATEDIFF(day,doc.doctor_joining_date,DATE(fdt1.task_assigned_at))>=30 THEN 'Old' ELSE 'New' END AS doctor_type,
	    DATE(fdt1.task_assigned_at) AS task_assigned_date,
	    EXTRACT(HOUR FROM fdt1.task_assigned_at) AS task_assigned_hour,
	    fdt1.task_type,
	    CASE WHEN fdt1.task_action IS NULL AND fdt1.doctor_inactive_flag=1 THEN 'Doctor Inactive' 
	  --WHEN doctor_inactive_flag=0 AND task_action IS NULL THEN 'Abrupt Offline/Internet'
	      ELSE fdt1.task_action
	    END AS task_action,
	    CASE WHEN fdt1.next_task_id IS NULL THEN 'last_task' ELSE 'intermediate_task' END AS task_stage,
	    COUNT(fdt1.task_id) AS task_count,
	    SUM(prescription_count) AS total_rx_written,
	    SUM(fdt1.issue_task_flag) AS total_issue_tasks,
	    SUM(fdt1.call_attempts) AS total_call_attempts,
	    SUM(fdt1.calls_connected) AS total_calls_connected,
	    SUM(fdt1.calls_disconnected) AS total_calls_disconnected,
	    SUM(fdt1.doctor_notified_count) AS total_doctor_notified_instances,
	    COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' AND fdt2.issue_task_flag=1 THEN fdt1.task_id END) AS total_issues,
	    COUNT(CASE WHEN fdt1.min_call_initiated_at IS NOT NULL THEN fdt1.task_id END) AS tasks_called,
	    COUNT(CASE WHEN fdt1.min_call_disconnected_at IS NOT NULL THEN fdt1.task_id END) AS tasks_disconnected,
	    COUNT(CASE WHEN fdt1.task_actioned_at IS NOT NULL THEN fdt1.task_id END) AS tasks_actioned,
	    SUM(DATEDIFF(second,fdt1.task_assigned_at, fdt1.min_call_initiated_at)) AS time_to_call,
	    SUM(DATEDIFF(second,fdt1.min_call_initiated_at, fdt1.min_call_disconnected_at)) AS first_call_duration,
	    SUM(DATEDIFF(second,fdt1.max_call_initiated_at, fdt1.max_call_disconnected_at)) AS last_call_duration,
	    SUM(DATEDIFF(second,fdt1.min_call_initiated_at, fdt1.max_call_disconnected_at)) AS total_call_duration,
	    SUM(DATEDIFF(second,fdt1.max_call_disconnected_at, fdt1.task_actioned_at)) AS time_to_action,
	    SUM(DATEDIFF(second,fdt1.task_assigned_at, fdt1.task_actioned_at)) AS total_actual_handling_time,
	    SUM(DATEDIFF(second,fdt1.task_assigned_at,fdt1.task_detached_at)) AS total_detachment_time,
	    SUM(fdt1.expected_task_handling_seconds) AS total_expected_handling_time
FROM data_model.f_docstat_task fdt1
--LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON fdt1.doctor_id=d.id
LEFT JOIN doc ON fdt1.doctor_profile_id=doc.doctor_profile_id
LEFT JOIN data_model.f_docstat_task fdt2 ON fdt1.next_task_id=fdt2.task_id
LEFT JOIN (
	  SELECT case_id, COUNT(id) AS prescription_count
	  FROM pe_docstat_91streets_media_technologies.prescription
	  WHERE deleted_at IS NULL --AND DATE(created_at)>='2019-11-01'
	  GROUP BY 1
	  ) p ON fdt1.case_id=p.case_id AND fdt1.next_task_id IS NULL --AND fdt1.task_action='Rx Prescribed'
WHERE (fdt1.task_action IN ('Rx Prescribed','Hold','Rescheduled','Doctor Rejected','Reassigned To Admin') OR fdt1.doctor_inactive_flag=1)
		AND
		DATE(fdt1.task_assigned_at) >= (CASE WHEN (EXTRACT(MONTH FROM CURRENT_DATE)-6)<=0 
									 THEN ((EXTRACT(YEAR FROM CURRENT_DATE)-1)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)+6)||'-'||'01')::DATE 
									 ELSE (EXTRACT(YEAR FROM CURRENT_DATE)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)-6)||'-'||'01')::DATE
								END)
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 5,6,10 DESC;




---- Grading Prefinal

WITH aggregated AS (
 SELECT fdt1.doctor_name, 
   CASE WHEN EXTRACT(MONTH FROM fdt1.task_assigned_at) IN (1,2,3,4,5,6,7,8,9) 
     THEN (EXTRACT(YEAR FROM fdt1.task_assigned_at) || '-0' || EXTRACT(MONTH FROM fdt1.task_assigned_at))
     ELSE (EXTRACT(YEAR FROM fdt1.task_assigned_at) || '-' || EXTRACT(MONTH FROM fdt1.task_assigned_at)) 
   END AS year_month_value,
   (EXTRACT(YEAR FROM fdt1.task_assigned_at)*12+EXTRACT(MONTH FROM fdt1.task_assigned_at)) AS month_value,
   CASE WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 0 THEN 'M-0'
     WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 1 THEN 'M-1'
     WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 2 THEN 'M-2'
     WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 3 THEN 'M-3'
     WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 4 THEN 'M-4'
     WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 5 THEN 'M-5'
     WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 6 THEN 'M-6'
   END AS month_diff_type,
   --MAX(CASE WHEN fdt1.task_action='Rx Prescribed' THEN 1 ELSE 0 END) AS online_flag,
   COUNT(DISTINCT CASE WHEN fdt1.task_action='Rx Prescribed' THEN DATE(fdt1.task_assigned_at) END) AS total_online_days,
   COUNT(fdt1.task_id) AS total_tasks,
   COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' THEN fdt1.task_id END) AS total_prescribed_instances,
   COUNT(CASE WHEN fdt1.task_action='Doctor Rejected' THEN fdt1.task_id END) AS total_rejected_instances,
   COUNT(CASE WHEN fdt1.task_action='Reassigned To Admin' THEN fdt1.task_id END) AS total_reassigned_instances,
   COUNT(CASE WHEN fdt1.task_action='Hold' THEN fdt1.task_id END) AS total_hold_instances,
   COUNT(CASE WHEN fdt1.task_action='Rescheduled' THEN fdt1.task_id END) AS total_rescheduled_instances,
   COUNT(CASE WHEN fdt1.doctor_inactive_flag=1 THEN fdt1.task_id END) AS total_inactive_instances,
   COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' AND fdt2.issue_task_flag=1 THEN fdt1.task_id END) AS total_issues,
   SUM(fdt1.doctor_notified_count) AS total_doctor_notified_instances,
   COUNT(CASE WHEN fdt1.doctor_inactive_flag=1 THEN fdt1.task_id END)::FLOAT/COUNT(fdt1.task_id) AS total_inactive_fraction,
   COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' AND fdt2.issue_task_flag=1 THEN fdt1.task_id END)::FLOAT/COUNT(fdt1.task_id) AS total_issue_fraction,
   SUM(fdt1.doctor_notified_count)::FLOAT/COUNT(fdt1.task_id) AS total_notified_fraction,
   SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN DATEDIFF(second,fdt1.task_assigned_at,fdt1.task_actioned_at) END) AS total_actual_handling_time,
   SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN fdt1.expected_task_handling_seconds END) AS total_expected_handling_time,
   SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN fdt1.expected_task_handling_seconds END)::FLOAT/SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN DATEDIFF(second,fdt1.task_assigned_at,fdt1.task_actioned_at) END) AS total_efficiency_fraction
 FROM data_model.f_docstat_task fdt1
 LEFT JOIN data_model.f_docstat_task fdt2 ON fdt1.next_task_id=fdt2.task_id
 WHERE (fdt1.task_action IN ('Rx Prescribed','Hold','Rescheduled','Doctor Rejected','Reassigned To Admin') OR fdt1.doctor_inactive_flag=1)
   AND 
   DATE(fdt1.task_assigned_at)>=(CASE WHEN (EXTRACT(MONTH FROM CURRENT_DATE)-6)<=0 
          THEN ((EXTRACT(YEAR FROM CURRENT_DATE)-1)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)+6)||'-'||'01')::DATE 
          ELSE (EXTRACT(YEAR FROM CURRENT_DATE)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)-6)||'-'||'01')::DATE
        END)
 GROUP BY 1,2,3,4
),
percentile_rank AS (
 SELECT *, 
   PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_online_days, total_prescribed_instances) AS online_percentile,
   PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_prescribed_instances, total_online_days) AS consult_percentile,
   PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_issue_fraction DESC, total_prescribed_instances, total_online_days) AS issue_percentile,
   PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_inactive_fraction DESC, total_prescribed_instances, total_online_days) AS inactive_percentile,
   PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_notified_fraction DESC, total_prescribed_instances, total_online_days) AS notified_percentile,
   PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_efficiency_fraction, total_prescribed_instances, total_online_days) AS efficiency_percentile
 FROM aggregated
),
final_calc AS (
 SELECT *,
   CASE WHEN total_online_days=0 THEN NULL
     ELSE
     CASE WHEN final_score_percentile>=0.8 THEN 1
       WHEN final_score_percentile>=0.5 THEN 2
       WHEN final_score_percentile>=0.2 THEN 3
       ELSE 4
     END
   END AS grade
 FROM (
  SELECT *, 
    (online_percentile*0.2 + consult_percentile*0.5 + efficiency_percentile*0.2 + issue_percentile*0.0334 + inactive_percentile*0.0334 + notified_percentile*0.0334) AS final_score,
    PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY (online_percentile*0.2 + consult_percentile*0.5 + efficiency_percentile*0.2 + inactive_percentile*0.05 + notified_percentile*0.05)) AS final_score_percentile
  FROM percentile_rank
 )
)
SELECT fc1.*,
  fc2.year_month_value AS prev_year_month,
  fc2.grade AS prev_grade,
  fc3.year_month_value AS prev_prev_year_month,
  fc3.grade AS prev_prev_grade
FROM final_calc fc1
LEFT JOIN final_calc fc2 ON fc1.doctor_name=fc2.doctor_name AND fc1.month_value=fc2.month_value+1
LEFT JOIN final_calc fc3 ON fc1.doctor_name=fc3.doctor_name AND fc1.month_value=fc3.month_value+2
ORDER BY 2 DESC, 28 DESC;



--- Final Grading

WITH doc AS ( 
		SELECT d.doctor_profile_id, MAX(INITCAP(d.name)) AS doctor_name, MIN(DATE(c.completion_time)) AS doctor_joining_date
	    FROM pe_docstat_91streets_media_technologies."case" c 
	    LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON d.id=c.doctor_id
	    WHERE c.current_status_id=5
	    GROUP BY 1
), 
aggregated AS (
	SELECT doc.doctor_name, 
			CASE WHEN EXTRACT(MONTH FROM fdt1.task_assigned_at) IN (1,2,3,4,5,6,7,8,9) 
				 THEN (EXTRACT(YEAR FROM fdt1.task_assigned_at) || '-0' || EXTRACT(MONTH FROM fdt1.task_assigned_at))
				 ELSE (EXTRACT(YEAR FROM fdt1.task_assigned_at) || '-' || EXTRACT(MONTH FROM fdt1.task_assigned_at)) 
			END AS year_month_value,
			(EXTRACT(YEAR FROM fdt1.task_assigned_at)*12+EXTRACT(MONTH FROM fdt1.task_assigned_at)) AS month_value,
			CASE WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 0 THEN 'M-0'
				 WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 1 THEN 'M-1'
				 WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 2 THEN 'M-2'
				 WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 3 THEN 'M-3'
				 WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 4 THEN 'M-4'
				 WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 5 THEN 'M-5'
				 WHEN DATEDIFF(month,DATE(fdt1.task_assigned_at), CURRENT_DATE) = 6 THEN 'M-6'
			END AS month_diff_type,
			--MAX(CASE WHEN fdt1.task_action='Rx Prescribed' THEN 1 ELSE 0 END) AS online_flag,
			COUNT(DISTINCT CASE WHEN fdt1.task_action='Rx Prescribed' THEN DATE(fdt1.task_assigned_at) END) AS total_online_days,
			COUNT(fdt1.task_id) AS total_tasks,
			COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' THEN fdt1.task_id END) AS total_prescribed_instances,
			COUNT(CASE WHEN fdt1.task_action='Doctor Rejected' THEN fdt1.task_id END) AS total_rejected_instances,
			COUNT(CASE WHEN fdt1.task_action='Reassigned To Admin' THEN fdt1.task_id END) AS total_reassigned_instances,
			COUNT(CASE WHEN fdt1.task_action='Hold' THEN fdt1.task_id END) AS total_hold_instances,
			COUNT(CASE WHEN fdt1.task_action='Rescheduled' THEN fdt1.task_id END) AS total_rescheduled_instances,
			COUNT(CASE WHEN fdt1.doctor_inactive_flag=1 THEN fdt1.task_id END) AS total_inactive_instances,
			COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' AND fdt2.issue_task_flag=1 THEN fdt1.task_id END) AS total_issues,
			SUM(fdt1.doctor_notified_count) AS total_doctor_notified_instances,
			COUNT(CASE WHEN fdt1.doctor_inactive_flag=1 THEN fdt1.task_id END)::FLOAT/COUNT(fdt1.task_id) AS total_inactive_fraction,
			COUNT(CASE WHEN fdt1.task_action='Rx Prescribed' AND fdt2.issue_task_flag=1 THEN fdt1.task_id END)::FLOAT/COUNT(fdt1.task_id) AS total_issue_fraction,
			SUM(fdt1.doctor_notified_count)::FLOAT/COUNT(fdt1.task_id) AS total_notified_fraction,
			SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN DATEDIFF(second,fdt1.task_assigned_at,fdt1.task_actioned_at) END) AS total_actual_handling_time,
			SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN fdt1.expected_task_handling_seconds END) AS total_expected_handling_time,
			SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN fdt1.expected_task_handling_seconds END)::FLOAT/SUM(CASE WHEN fdt1.task_action IN ('Rescheduled','Hold','Rx Prescribed','Reassigned To Admin','Doctor Rejected') THEN DATEDIFF(second,fdt1.task_assigned_at,fdt1.task_actioned_at) END) AS total_efficiency_fraction
	FROM data_model.f_docstat_task fdt1
	LEFT JOIN doc ON fdt1.doctor_profile_id=doc.doctor_profile_id
	LEFT JOIN data_model.f_docstat_task fdt2 ON fdt1.next_task_id=fdt2.task_id
	WHERE (fdt1.task_action IN ('Rx Prescribed','Hold','Rescheduled','Doctor Rejected','Reassigned To Admin') OR fdt1.doctor_inactive_flag=1)
			AND 
			DATE(fdt1.task_assigned_at)>=(CASE WHEN (EXTRACT(MONTH FROM CURRENT_DATE)-6)<=0 
									 THEN ((EXTRACT(YEAR FROM CURRENT_DATE)-1)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)+6)||'-'||'01')::DATE 
									 ELSE (EXTRACT(YEAR FROM CURRENT_DATE)||'-'||(EXTRACT(MONTH FROM CURRENT_DATE)-6)||'-'||'01')::DATE
								END)
	GROUP BY 1,2,3,4
),
percentile_rank AS (
	SELECT *, 
			PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_online_days, total_prescribed_instances) AS online_percentile,
			PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_prescribed_instances, total_online_days) AS consult_percentile,
			PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_issue_fraction DESC, total_prescribed_instances, total_online_days) AS issue_percentile,
			PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_inactive_fraction DESC, total_prescribed_instances, total_online_days) AS inactive_percentile,
			PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_notified_fraction DESC, total_prescribed_instances, total_online_days) AS notified_percentile,
			PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY total_efficiency_fraction, total_prescribed_instances, total_online_days) AS efficiency_percentile
	FROM aggregated
),
final_calc AS (
	SELECT *,
			CASE WHEN total_online_days=0 THEN NULL
				 ELSE
					CASE WHEN final_score_percentile>=0.9 THEN 'A'
						 WHEN final_score_percentile>=0.7 THEN 'B'
						 WHEN final_score_percentile>=0.4 THEN 'C'
						 ELSE 'D'
					END
			END AS grade
	FROM (
		SELECT *, 
				(online_percentile*0.2 + consult_percentile*0.5 + efficiency_percentile*0.2 + issue_percentile*0.0334 + inactive_percentile*0.0334 + notified_percentile*0.0334) AS final_score,
				PERCENT_RANK() OVER (PARTITION BY year_month_value ORDER BY (online_percentile*0.2 + consult_percentile*0.5 + efficiency_percentile*0.2 + inactive_percentile*0.05 + notified_percentile*0.05)) AS final_score_percentile
		FROM percentile_rank
	)
)
SELECT --fc1.*,
		fc1.doctor_name, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.year_month_value END) AS zero_year_month, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.total_online_days END) AS zero_month_online_days, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.total_prescribed_instances END) AS zero_month_prescribed_instances, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.total_efficiency_fraction END) AS zero_month_efficiency_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.total_inactive_fraction END) AS zero_month_inactive_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.total_notified_fraction END) AS zero_month_notified_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.total_issue_fraction END) AS zero_month_issue_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.final_score_percentile END) AS zero_month_final_score_percentile, 
		MIN(CASE WHEN fc1.month_diff_type='M-0' THEN fc1.grade END) AS zero_month_grade, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.year_month_value END) AS first_year_month, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.total_online_days END) AS first_month_online_days, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.total_prescribed_instances END) AS first_month_prescribed_instances, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.total_efficiency_fraction END) AS first_month_efficiency_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.total_inactive_fraction END) AS first_month_inactive_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.total_notified_fraction END) AS first_month_notified_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.total_issue_fraction END) AS first_month_issue_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-1' THEN fc1.grade END) AS first_month_grade, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.year_month_value END) AS second_year_month, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.total_online_days END) AS second_month_online_days, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.total_prescribed_instances END) AS second_month_prescribed_instances, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.total_efficiency_fraction END) AS second_month_efficiency_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.total_inactive_fraction END) AS second_month_inactive_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.total_notified_fraction END) AS second_month_notified_fraction, 
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.total_issue_fraction END) AS second_month_issue_fraction,  
		MIN(CASE WHEN fc1.month_diff_type='M-2' THEN fc1.grade END) AS second_month_grade,
		MIN(CASE WHEN fc1.month_diff_type='M-3' THEN fc1.grade END) AS third_month_grade,
		MIN(CASE WHEN fc1.month_diff_type='M-4' THEN fc1.grade END) AS fourth_month_grade,
		MIN(CASE WHEN fc1.month_diff_type='M-5' THEN fc1.grade END) AS fifth_month_grade,
		MIN(CASE WHEN fc1.month_diff_type='M-6' THEN fc1.grade END) AS sixth_month_grade
FROM final_calc fc1
--LEFT JOIN final_calc fc2 ON fc1.doctor_name=fc2.doctor_name AND fc1.month_value=fc2.month_value+1
--LEFT JOIN final_calc fc3 ON fc1.doctor_name=fc3.doctor_name AND fc1.month_value=fc3.month_value+2
GROUP BY 1
ORDER BY 9 DESC NULLS LAST;
