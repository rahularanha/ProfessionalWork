
--DELETE FROM data_model.f_docstat_task; --DROP TABLE data_model.f_docstat_task; 

CREATE TABLE data_model.f_docstat_task
(
doctor_profile_id INT8,
task_id	VARCHAR(50),
order_id VARCHAR(50),
case_id INT8,
doctor_id INT8,
task_type VARCHAR(50),
task_assigned_at TIMESTAMP,
min_call_initiated_at TIMESTAMP,
max_call_initiated_at TIMESTAMP,
min_call_disconnected_at TIMESTAMP,
max_call_disconnected_at TIMESTAMP,
task_actioned_at TIMESTAMP,
task_detached_at TIMESTAMP,
task_action VARCHAR(50),
reject_reason VARCHAR(100),
call_attempts INT8,
calls_connected INT8,
calls_disconnected INT8,
doctor_notified_count INT8,
doctor_inactive_flag INT8,
issue_task_flag INT8,
expected_task_handling_seconds INT8,
previous_task_id VARCHAR(50),
next_task_id VARCHAR(50),
)
distkey(task_id)
compound sortkey(task_assigned_at,case_id, doctor_profile_id);



CREATE TEMPORARY TABLE updating_tasks AS
SELECT task_id
FROM pe_docstat_91streets_media_technologies.case_log 
GROUP BY 1
HAVING MIN(DATE(created_at))>='2019-11-01' --BETWEEN CURRENT_DATE-2 AND CURRENT_DATE-1
;


---- Doctor Task Data Model

INSERT INTO data_model.f_docstat_task (
WITH action_benchmarks AS (
	SELECT 'Rx Prescribed' AS task_action, 150 AS benchmark
	UNION
	SELECT 'Hold' AS task_action, 75 AS benchmark
	UNION
	SELECT 'Rescheduled' AS task_action, 120 AS benchmark
	UNION
	SELECT 'Doctor Rejected' AS task_action, 75 AS benchmark
	UNION
	SELECT 'Reassigned To Admin' AS task_action, 80 AS benchmark
	UNION
	SELECT 'Doctor Inactive' AS task_action, 0 AS benchmark
),
task_level AS (
	SELECT cl.task_id, c.order_id, case_id, MAX(cl.doctor_id) AS doctor_id, 
			--MIN(cl.created_at) AS task_created_at,
			MAX(ctm.name) AS task_type,
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
					 WHEN case_log_status_id=15 THEN CASE WHEN clr.reason_id=5 THEN 'Reassigned To Admin' ELSE 'Doctor Rejected' END
					 END
				) AS task_action,
			MAX(rm.reason) AS reject_reason,
			COUNT(DISTINCT CASE WHEN case_log_status_id=3 THEN cl.created_at END) AS call_attempts,
			COUNT(DISTINCT CASE WHEN case_log_status_id=4 THEN cl.created_at END) AS calls_connected,
			COUNT(DISTINCT CASE WHEN case_log_status_id=5 THEN cl.created_at END) AS calls_disconnected,
			COUNT(DISTINCT CASE WHEN case_log_status_id=7 THEN cl.created_at END) AS doctor_notified_count,
			MAX(CASE WHEN case_log_status_id=1 THEN 1 ELSE 0 END) AS doctor_inactive_flag,
			MAX(CASE WHEN fdpo.rx_first_prescribed_at<=cl.created_at THEN 1 ELSE 0 END) AS issue_task_flag
	FROM pe_docstat_91streets_media_technologies.case_log cl
	INNER JOIN pe_docstat_91streets_media_technologies."case" c ON cl.case_id=c.id --AND DATE(c.created_at)=(CURRENT_DATE-1)
	LEFT JOIN (SELECT task_id, MAX(id) AS case_log_id FROM pe_docstat_91streets_media_technologies.case_log GROUP BY 1) cl2 ON cl.id=cl2.case_log_id
	LEFT JOIN pe_docstat_91streets_media_technologies.case_log_statuses_master clsm ON cl.case_log_status_id=clsm.id AND cl2.case_log_id IS NOT NULL
	LEFT JOIN pe_docstat_91streets_media_technologies.case_types_master ctm ON cl.case_type_id=ctm.id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON c.order_id=fdpo.order_id::VARCHAR
	LEFT JOIN pe_docstat_91streets_media_technologies.case_log_reason clr ON cl.id=clr.case_log_id
	LEFT JOIN pe_docstat_91streets_media_technologies.reasons_master rm ON clr.reason_id=rm.id
	WHERE c.order_id!='Test01' AND cl.task_id IS NOT NULL
	GROUP BY 1,2,3
	ORDER BY 6 --3, 5
)
SELECT d.doctor_profile_id,
		tl.*, 
		ab.benchmark AS expected_task_handling_seconds,
		LAG(task_id,1) OVER (PARTITION BY case_id ORDER BY task_assigned_at) AS previous_task_id,
		LEAD(task_id,1) OVER (PARTITION BY case_id ORDER BY task_assigned_at) AS next_task_id
FROM task_level tl
LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON tl.doctor_id=d.id
LEFT JOIN action_benchmarks ab ON tl.task_action=ab.task_action
WHERE DATE(task_assigned_at) BETWEEN '2019-11-01' AND (CURRENT_DATE-1)
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
ORDER BY 7
);



--doc AS ( 
--	SELECT d.doctor_profile_id, MAX(INITCAP(d.name)) AS doctor_name, MIN(DATE(c.completion_time)) AS first_case_completed_at
--	FROM pe_docstat_91streets_media_technologies."case" c 
--	LEFT JOIN pe_docstat_91streets_media_technologies.doctor d ON c.doctor_id=d.id
----	LEFT JOIN pe_docstat_91streets_media_technologies.doctor_profile dp ON d.doctor_profile_id=dp.id
--	WHERE c.current_status_id=5
--	GROUP BY 1
--),
SELECT COUNT(task_id), COUNT(DISTINCT task_id) FROM data_model.f_docstat_task LIMIT 10;
