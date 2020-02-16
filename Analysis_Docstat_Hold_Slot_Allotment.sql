--- prefinal

SELECT c.order_id, xyz.case_log_id, case_id, d.name AS doc_name, d.phone AS doc_mobile_number,
		ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id) AS response_number,
		CASE WHEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id)=1 AND ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id DESC)=1 THEN 'FirstLast_response'
			 WHEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id)=1 THEN 'First_response'
			 WHEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id DESC)=1 THEN 'Last_response'
			 ELSE 'Intermediate_response'
		END AS response_type,
		CASE WHEN case_status=5 THEN 'Rx_prescribed' 
			 WHEN xyz.note LIKE '%Reassign%' THEN 'Reassigned'
			 WHEN xyz.note LIKE '%Rejected%' THEN CASE WHEN oh.next_status=8 THEN 'CustomerRejected' ELSE 'DoctorRejected' END
			 ELSE LEFT(xyz.note, POSITION(':' IN xyz.note)) 
		END AS doc_action, 
		case_status, xyz.note,--c.created_at AS moved_to_dp_at,
		cl.call_time, call_initiated_at, call_disconnected_at, doc_action_at, 
		DATEDIFF(SECOND,call_initiated_at,call_disconnected_at) AS second_dial_duration,
		CASE WHEN (ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id))=1 THEN DATEDIFF(MINUTE,c.created_at,call_initiated_at) END AS minute_first_attempt_since_movetodp, 
		json_extract_path_text(cl.response,'TimeToAnswer') AS time_to_answer,
		json_extract_path_text(cl.response,'Duration') AS call_duration,
		json_extract_path_text(cl.response,'HangupBy') AS hangup_by,
		json_extract_path_text(cl.response,'Status') AS call_status,
		json_extract_path_text(cl.response,'DialStatus') AS dial_status,
		json_extract_path_text(cl.response,'AgentStatus') AS agent_status,
		json_extract_path_text(cl.response,'CustomerStatus') AS customer_status --cl.response,
		-- call_status_id --cl.call_duration,
FROM (
	SELECT id AS case_log_id, case_id, doctor_id,
			new_status AS case_status,
			--CASE new_status WHEN 1 THEN 'Hold' WHEN 5 THEN 'Rx_Prescribed' END AS doc_action,
			LAG(created_at,2) OVER (PARTITION BY case_id ORDER BY id) AS call_initiated_at,
			LAG(created_at,1) OVER (PARTITION BY case_id ORDER BY id) AS call_disconnected_at, 
			created_at AS doc_action_at, 
			LAG(new_status,2) OVER (PARTITION BY case_id ORDER BY id) AS call_initiated_status,
			LAG(new_status,1) OVER (PARTITION BY case_id ORDER BY id) AS call_disconnected_status, 
			new_status AS doc_put_on_hold_status, note,
			LAG(id,1) OVER (PARTITION BY case_id ORDER BY id) AS prev_cl_id
	FROM docstat.case_log
) xyz
INNER JOIN docstat."case" c ON xyz.case_id=c.id AND DATE(c.created_at) BETWEEN '2019-01-01' AND '2019-04-09' --AND c.reschedule_counter>0 AND c.current_status_id=5
INNER JOIN (
			SELECT id, order_id, order_status, "timestamp" AS in_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS ranking,
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at
			FROM pe2.order_history oh
		) oh ON c.order_id=oh.order_id AND ranking=1 AND xyz.doc_action_at BETWEEN in_dp_at AND out_dp_at
LEFT JOIN docstat.call_log cl ON xyz.prev_cl_id=cl.case_log_id
LEFT JOIN docstat.doctor d ON c.doctor_id=d.id
WHERE (xyz.note LIKE 'Hold%' OR xyz.note LIKE 'Rx_prescribed%' OR xyz.note LIKE 'Rescheduled%' OR xyz.note LIKE 'Rejected%' OR xyz.note LIKE '%Reassign%') --AND case_id=752230
ORDER BY 3,2
;



---- Final query

SELECT final_result.*, 
		CASE WHEN (LAG(hold_number,1) OVER (PARTITION BY case_id ORDER BY case_log_id))=1 THEN 1 END AS response_after_hold,
		CASE WHEN (LAG(hold_number,1) OVER (PARTITION BY case_id ORDER BY case_log_id)) IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY case_log_id) END AS responses_after_hold
FROM (
SELECT c.order_id, case_id, xyz.case_log_id, d.name AS doc_name, d.phone AS doc_mobile_number,
		ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id) AS response_number,
		CASE WHEN doc_action2='Hold:' THEN (ROW_NUMBER() OVER (PARTITION BY case_id,doc_action2='Hold:' ORDER BY xyz.case_log_id)) END AS hold_number,
		CASE WHEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id)=1 AND ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id DESC)=1 THEN 'FirstLast_response'
			 WHEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id)=1 THEN 'First_response'
			 WHEN ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id DESC)=1 THEN 'Last_response'
			 ELSE 'Intermediate_response'
		END AS response_type,
		CASE WHEN case_status=5 THEN 'Rx_prescribed' 
			 WHEN xyz.note LIKE '%Reassign%' THEN 'Reassigned'
			 WHEN xyz.note LIKE '%Rejected%' THEN CASE WHEN oh.next_status=8 THEN 'CustomerRejected' ELSE 'DoctorRejected' END
			 ELSE LEFT(xyz.note, POSITION(':' IN xyz.note)) 
		END AS doc_action1, 
		case_status, xyz.note,--c.created_at AS moved_to_dp_at,
		cl.call_time, call_initiated_at, call_disconnected_at, doc_action_at, 
		DATEDIFF(SECOND,call_initiated_at,call_disconnected_at) AS second_dial_duration,
		CASE WHEN (ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY xyz.case_log_id))=1 THEN DATEDIFF(MINUTE,c.created_at,call_initiated_at) END AS minute_first_attempt_since_movetodp, 
		json_extract_path_text(cl.response,'TimeToAnswer') AS time_to_answer,
		json_extract_path_text(cl.response,'Duration') AS call_duration,
		json_extract_path_text(cl.response,'HangupBy') AS hangup_by,
		json_extract_path_text(cl.response,'Status') AS call_status,
		json_extract_path_text(cl.response,'DialStatus') AS dial_status,
		json_extract_path_text(cl.response,'AgentStatus') AS agent_status,
		json_extract_path_text(cl.response,'CustomerStatus') AS customer_status --cl.response,
		-- call_status_id --cl.call_duration,
FROM (
	SELECT id AS case_log_id, case_id, doctor_id,
			new_status AS case_status,
			--CASE new_status WHEN 1 THEN 'Hold' WHEN 5 THEN 'Rx_Prescribed' END AS doc_action,
			LAG(created_at,2) OVER (PARTITION BY case_id ORDER BY id) AS call_initiated_at,
			LAG(created_at,1) OVER (PARTITION BY case_id ORDER BY id) AS call_disconnected_at, 
			created_at AS doc_action_at, 
			LAG(new_status,2) OVER (PARTITION BY case_id ORDER BY id) AS call_initiated_status,
			LAG(new_status,1) OVER (PARTITION BY case_id ORDER BY id) AS call_disconnected_status, 
			new_status AS doc_put_on_hold_status, note,
			CASE WHEN case_status=5 THEN 'Rx_prescribed' 
				 WHEN note LIKE '%Reassign%' THEN 'Reassigned'
				 WHEN note LIKE '%Rejected%' THEN 'Rejected' 
				 ELSE LEFT(note, POSITION(':' IN note)) 
			END AS doc_action2,
			LAG(id,1) OVER (PARTITION BY case_id ORDER BY id) AS prev_cl_id
	FROM docstat.case_log
) xyz
INNER JOIN docstat."case" c ON xyz.case_id=c.id AND DATE(c.created_at) BETWEEN '2019-03-01' AND '2019-04-09' --AND c.reschedule_counter>0 AND c.current_status_id=5
INNER JOIN (
			SELECT id, order_id, order_status, "timestamp" AS in_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS ranking,
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at
			FROM pe2.order_history oh
		) oh ON c.order_id=oh.order_id AND ranking=1 AND xyz.doc_action_at BETWEEN in_dp_at AND out_dp_at
LEFT JOIN docstat.call_log cl ON xyz.prev_cl_id=cl.case_log_id
LEFT JOIN docstat.doctor d ON c.doctor_id=d.id
WHERE (xyz.note LIKE 'Hold%' OR xyz.note LIKE 'Rx_prescribed%' OR xyz.note LIKE 'Rescheduled%' OR xyz.note LIKE 'Rejected%' OR xyz.note LIKE '%Reassign%') --AND case_id=752230
) final_result
ORDER BY 2,3
;
