------- Suggestion for number of slots to provide

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

	
	
	
	
------------ Impact analysis of hold orders on slots
	
-------Slotting Order Analysis

WITH attempt_data AS (
	SELECT order_id,id, DATEADD(MIN,330,created_at) AS attempted_at,
			CASE "action" WHEN 1 THEN 'Moved_to_DP'
						  WHEN 2 THEN 'Assigned'
						  WHEN 3 THEN 'Rejected'
						  WHEN 4 THEN 'Accepted'
						  WHEN 5 THEN 'On Hold'
						  WHEN 6 THEN 'Awaiting_Order_Creation'
						  WHEN 7 THEN 'Order_Created'
						  WHEN 8 THEN 'Doctor_Reassigned'
						  WHEN 9 THEN 'Queued'
						  WHEN 10 THEN 'Resheduled'
			 END AS dp_action,
			 CASE WHEN "action" IN (3,4,5,8,10)  
			      THEN ROW_NUMBER() OVER (PARTITION BY order_id,"action" IN (3,4,5,8,10) ORDER BY id) 
			 END AS attempt_number
	FROM pe_pe2_pe2.doctor_program_order_history dpoh
	--WHERE order_id=9279133
)
SELECT order_id, order_placed_at, first_moved_to_dp_at, first_response_at, first_response, issue_flag, doctor_category,
		case_id, booking_id, booking_created_at, booking_deleted_at, booking_updated_at,
		slot_id, capacity, booked_capacity, user_profile_id, slot_date, slot_start_time, slot_end_time, 
		slot_created_at, slot_deleted_at, slot_updated_at,
		MIN(CASE WHEN attempt_before_first=1 THEN ad_b_dp_action END) AS before_first_attempt,
		MIN(CASE WHEN attempt_before_first=1 THEN ad_b_attempted_at END) AS before_first_attempted_at,
		MIN(CASE WHEN attempt_before_last=1 THEN ad_b_dp_action END) AS before_last_attempt,
		MIN(CASE WHEN attempt_before_last=1 THEN ad_b_attempt_number END) AS before_last_attempt_number,
		MIN(CASE WHEN attempt_before_last=1 THEN ad_b_attempted_at END) AS before_last_attempt_at,
		MIN(CASE WHEN attempt_after_first=1 THEN ad_a_dp_action END) AS after_first_attempt,
		MIN(CASE WHEN attempt_after_first=1 THEN ad_a_attempt_number END) AS after_first_attempt_number,
		MIN(CASE WHEN attempt_after_first=1 THEN ad_a_attempted_at END) AS after_first_attempt_at,
		MIN(CASE WHEN attempt_after_last=1 THEN ad_a_dp_action END) AS after_last_attempt,
		MIN(CASE WHEN attempt_after_last=1 THEN ad_a_attempt_number END) AS after_last_attempt_number,
		MIN(CASE WHEN attempt_after_last=1 THEN ad_a_attempted_at END) AS after_last_attempt_at
FROM (
	SELECT fdpo.order_id, fdpo.order_placed_at, fdpo.doctor_category, fdpo.first_moved_to_dp_at, fdpo.first_response_at, fdpo.first_response, fdpo.issue_flag,
			RIGHT(b.booking_id,7) AS case_id, b.booking_id, DATEADD(MIN,330,b.created_at) AS booking_created_at, DATEADD(MIN,330,b.deleted_at) AS booking_deleted_at, DATEADD(MIN,330,b.updated_at) AS booking_updated_at,
			b.slot_id, s.capacity, s.booked_capacity, s.user_profile_id, s."date" AS slot_date, s.start_time AS slot_start_time, s.end_time AS slot_end_time, 
			DATEADD(MIN,330,s.created_at) AS slot_created_at, DATEADD(MIN,330,s.deleted_at) AS slot_deleted_at, DATEADD(MIN,330,s.updated_at) AS slot_updated_at,
			ad_b.attempted_at AS ad_b_attempted_at, ad_b.attempt_number AS ad_b_attempt_number, ad_b.dp_action AS ad_b_dp_action,
			ad_a.attempted_at AS ad_a_attempted_at, ad_a.attempt_number AS ad_a_attempt_number, ad_a.dp_action AS ad_a_dp_action,
			ROW_NUMBER() OVER (PARTITION BY fdpo.order_id ORDER BY ad_b.attempted_at DESC) attempt_before_last,
			ROW_NUMBER() OVER (PARTITION BY fdpo.order_id ORDER BY ad_b.attempted_at) attempt_before_first,
			ROW_NUMBER() OVER (PARTITION BY fdpo.order_id ORDER BY ad_a.attempted_at DESC) attempt_after_last,
			ROW_NUMBER() OVER (PARTITION BY fdpo.order_id ORDER BY ad_a.attempted_at) attempt_after_first
	FROM data_model.f_doctor_program_order fdpo
	LEFT JOIN pe_docstat_91streets_media_technologies."case" c ON fdpo.order_id=c.order_id
	LEFT JOIN pe_hive_slotting_hive.bookings b ON c.id=RIGHT(b.booking_id,7)
	LEFT JOIN pe_hive_slotting_hive.slots s ON b.slot_id=s.id AND s.user_profile_id=41 
	LEFT JOIN attempt_data ad_b ON fdpo.order_id=ad_b.order_id AND ad_b.attempted_at<DATEADD(MIN,330,b.created_at) AND ad_b.attempt_number IS NOT NULL
	LEFT JOIN attempt_data ad_a ON fdpo.order_id=ad_a.order_id AND ad_a.attempted_at>DATEADD(MIN,330,b.created_at) AND ad_a.attempt_number IS NOT NULL
	WHERE fdpo.num_of_times_on_hold>0 AND DATE(fdpo.order_placed_at) BETWEEN '2019-06-01' AND '2019-06-30'
	ORDER BY fdpo.order_placed_at 
)
--WHERE attempt_before_last=1 OR attempt_before_first=1 OR attempt_after_last=1 OR attempt_after_first=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
ORDER BY 2
--LIMIT 100;



---Slotting Slot Analysis

SELECT s.id AS slot_id, s.capacity AS slot_capacity, s.booked_capacity, 
		s."date" AS slot_date, s.start_time AS slot_start_time, s.end_time AS slot_end_time, 
		(s."date"||' '||s.start_time)::TIMESTAMP AS slot_start_at, DATEADD(MIN,330,s.created_at) AS slot_created_at, 
		DATEADD(MIN,330,s.deleted_at) AS slot_deleted_at, DATEADD(MIN,330,s.updated_at) AS slot_updated_at, 
		COUNT(CASE WHEN b.deleted_at IS NULL THEN b.id END) AS total_bookings,
		MAX(CASE WHEN b.deleted_at IS NULL THEN DATEADD(MIN,330,b.created_at) END) AS last_booking_at
FROM pe_hive_slotting_hive.slots s
LEFT JOIN pe_hive_slotting_hive.bookings b ON s.id=b.slot_id
WHERE s.user_profile_id=41 AND s."date" BETWEEN '2019-06-01' AND '2019-06-30'
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY slot_start_at ;
