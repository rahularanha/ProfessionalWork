--Order Level analysis

WITH finale AS (
SELECT order_id, order_placed_at, delivery_pincode, order_source, is_add_to_cart,
		is_courier, supplier_city_name, user_type_monthly, chronic_flag_old, customer_type,
		dp_order_source, dp_order_source_id, successful_consultation_type, doctor_category, first_moved_to_dp_at, 
		instances_moved_to_dp, num_of_doc_attempts, num_of_times_on_hold,
		first_response_at, first_response_fdpo, first_response_fdpo_id, 
		first_in_dp_last_response_at, first_in_dp_last_response, first_in_dp_last_response_id,
		MIN(case_id) AS case_id, 
		MIN(task_assigned_at) AS first_task_assigned_at, 
		MIN(CASE WHEN ranking=1 THEN task_action END) AS first_response, 
		COUNT(task_id) AS total_attempts,
		COUNT(CASE WHEN task_action IN ('Hold','Rescheduled')  THEN task_id END) AS total_hold_attempts,
		MAX(task_assigned_at) AS last_task_assigned_at, 
		MAX(CASE WHEN ranking_last=1 THEN task_action END) AS last_response
FROM (
	SELECT fo.order_id, fo.order_placed_at, fo.delivery_pincode, foc.order_source, fo.is_add_to_cart,
			fo.is_courier, fo.supplier_city_name, foc.user_type_monthly, foc.chronic_flag_old, foc.customer_type, fdpo.doctor_category,
			fdt.case_id, fdpo.successful_consultation_type, fdpo.first_moved_to_dp_at, fdpo.instances_moved_to_dp, fdpo.num_of_doc_attempts, fdpo.num_of_times_on_hold,
			CASE dpo."source" WHEN 3 THEN 'CnR-Mediassist'					
						  WHEN 4 THEN 'CC-Valid-M2DP'        
						  WHEN 6 THEN 'Customer-App/Web'  
						  WHEN 7 THEN 'CC-New-InvalidRx' 				
						  WHEN 8 THEN 'RefillOrder-CC-M2DP'			
  						  WHEN 9 THEN 'OrderOnCall'			
						  WHEN 10 THEN 'CC-Reject'	
						  ELSE 'NotSure'
			END AS dp_order_source, 
			CASE WHEN dpo."source" IN (3,4,6,7,8,9,10) THEN dpo."source" ELSE 0 END AS dp_order_source_id,
			fdt.task_id, fdt.task_action, fdt.task_assigned_at, 
			fdpo.first_response_at, 
			CASE fdpo.first_response WHEN 1 THEN 'Moved_to_DP'
											  WHEN 2 THEN 'Assigned'
											  WHEN 3 THEN 'Rejected'
											  WHEN 4 THEN 'Accepted'
											  WHEN 5 THEN 'On Hold'
											  WHEN 6 THEN 'Awaiting_Order_Creation'
											  WHEN 7 THEN 'Order_Created'
											  WHEN 8 THEN 'Doctor_Reassigned'
											  WHEN 9 THEN 'Queued'
											  WHEN 10 THEN 'Resheduled'
			END AS first_response_fdpo, first_response AS first_response_fdpo_id, 
			fdpo.first_in_dp_last_response_at, 
			CASE fdpo.first_in_dp_last_response WHEN 1 THEN 'Moved_to_DP'
											  WHEN 2 THEN 'Assigned'
											  WHEN 3 THEN 'Rejected'
											  WHEN 4 THEN 'Accepted'
											  WHEN 5 THEN 'On Hold'
											  WHEN 6 THEN 'Awaiting_Order_Creation'
											  WHEN 7 THEN 'Order_Created'
											  WHEN 8 THEN 'Doctor_Reassigned'
											  WHEN 9 THEN 'Queued'
											  WHEN 10 THEN 'Resheduled'
			END AS first_in_dp_last_response, fdpo.first_in_dp_last_response AS first_in_dp_last_response_id,
			ROW_NUMBER() OVER (PARTITION BY fdt.case_id ORDER BY fdt.task_assigned_at) AS ranking,
			ROW_NUMBER() OVER (PARTITION BY fdt.case_id ORDER BY fdt.task_assigned_at DESC) AS ranking_last
	FROM data_model.f_doctor_program_order fdpo
	INNER JOIN data_model.f_order fo ON fdpo.order_id=fo.order_id
	INNER JOIN data_model.f_order_consumer foc ON fo.order_id=foc.order_id
	LEFT JOIN data_model.f_docstat_task fdt ON fdpo.order_id=fdt.order_id AND LENGTH(fdt.order_id)=8 
												AND fdt.task_assigned_at BETWEEN fdpo.first_moved_to_dp_at AND fdpo.first_in_dp_last_response_at
												AND fdt.task_action IS NOT NULL AND fdt.min_call_initiated_at IS NOT NULL
	LEFT JOIN pe_pe2_pe2.doctor_program_order dpo ON fdpo.order_id=dpo.order_id
	WHERE DATE(fdpo.order_placed_at) BETWEEN '2019-11-01' AND '2020-01-31'
)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) 
SELECT order_id, order_placed_at, delivery_pincode, order_source, is_add_to_cart,
		is_courier, supplier_city_name, user_type_monthly, chronic_flag_old, customer_type, doctor_category,
		case_id, first_moved_to_dp_at, dp_order_source, dp_order_source_id, successful_consultation_type,
		instances_moved_to_dp, num_of_doc_attempts, num_of_times_on_hold,
		first_response_at, first_response_fdpo, first_response_fdpo_id, 
		first_in_dp_last_response_at, first_in_dp_last_response, first_in_dp_last_response_id,
		first_task_assigned_at, 
		first_response, 
		total_attempts,
		total_hold_attempts,
		last_task_assigned_at, 
		last_response,
		COUNT(task_id) AS before_edd_total_attempts,
		COUNT(CASE WHEN task_action IN ('Hold','Rescheduled') THEN task_id END) AS before_edd_total_hold_attempts,
		MAX(task_assigned_at) AS before_edd_last_task_assigned_at, 
		MAX(CASE WHEN ranking_last=1 THEN task_action END) AS before_edd_last_response
FROM (
	SELECT finale.*,
			fdt.task_id, fdt.task_action, fdt.task_assigned_at, 
			ROW_NUMBER() OVER (PARTITION BY fdt.case_id ORDER BY fdt.task_assigned_at DESC) AS ranking_last
	FROM finale
	LEFT JOIN data_model.f_docstat_task fdt 
	ON finale.case_id=fdt.case_id 
		AND	(
		(RIGHT(finale.order_placed_at,8)<'12:00:00' AND fdt.task_assigned_at<(DATE(finale.order_placed_at)||' 14:00:00')::TIMESTAMP) 
			OR 
		(RIGHT(finale.order_placed_at,8)>='12:00:00' AND fdt.task_assigned_at<((DATE(finale.order_placed_at)+1)||' 14:00:00')::TIMESTAMP)
		)
		AND fdt.task_action IS NOT NULL AND fdt.min_call_initiated_at IS NOT NULL
)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
--HAVING (MIN(task_assigned_at) BETWEEN '2019-11-01' AND '2019-12-31')
;


----- Attempt Level Analysis

SELECT *,
		CASE WHEN ((task_action IN ('Hold', 'Rescheduled')) OR (task_action='Doctor Rejected' AND reject_reason='Other')) THEN 0
			 WHEN ((task_action IN ('Rx Prescribed', 'Cancelled', 'Docstat Rejected')) OR (task_action='Doctor Rejected' AND reject_reason NOT IN ('Other'))) THEN 1
		END AS success_flag,
		ROW_NUMBER() OVER(PARTITION BY case_id ORDER BY task_assigned_at) AS attempt_number,
		ROW_NUMBER() OVER(PARTITION BY case_id ORDER BY task_assigned_at DESC) AS attempt_number_last
FROM (
	SELECT fdt.order_id, fdt.case_id, 
			fo.order_placed_at, fo.delivery_pincode, COALESCE(foc.order_source,'Refill') AS order_source, fo.is_add_to_cart,
			fo.is_courier, fo.supplier_city_name, foc.user_type_monthly, foc.chronic_flag_old, foc.customer_type,
			fdpo.successful_consultation_type, fdpo.first_moved_to_dp_at, 
			fdpo.instances_moved_to_dp, fdpo.num_of_doc_attempts, fdpo.num_of_times_on_hold,
			CASE dpo."source" WHEN 3 THEN 'CnR-Mediassist'					
						  WHEN 4 THEN 'CC-Valid-M2DP'        
						  WHEN 6 THEN 'Customer-App/Web'  
						  WHEN 7 THEN 'CC-New-InvalidRx' 				
						  WHEN 8 THEN 'RefillOrder-CC-M2DP'			
						  WHEN 9 THEN 'OrderOnCall'			
						  WHEN 10 THEN 'CC-Reject'	
						  ELSE 'NotSure'
			END AS dp_order_source,
			fdt.task_id, fdt.doctor_name, fdt.doctor_inactive_flag, fdt.doctor_notified_count, 
			fdt.task_assigned_at, fdt.task_actioned_at, fdt.task_action, fdt.reject_reason,	
			LEAD(task_assigned_at,1) OVER (PARTITION BY case_id ORDER BY task_assigned_at) AS next_task_assigned_at,
			call_initiated_flag, call_answered_flag
	--		ROW_NUMBER() OVER(PARTITION BY case_id ORDER BY task_assigned_at) AS attempt_number,
	--		ROW_NUMBER() OVER(PARTITION BY case_id ORDER BY task_assigned_at DESC) AS attempt_number_last
	FROM data_model.f_docstat_task fdt
	INNER JOIN data_model.f_order fo ON fdt.order_id=fo.order_id AND LENGTH(fdt.order_id)=8 AND fdt.task_assigned_at BETWEEN fo.cc_min_in_dp_at AND fo.cc_min_out_dp_at
	LEFT JOIN data_model.f_order_consumer foc ON fo.order_id=foc.order_id
	LEFT JOIN pe_pe2_pe2.doctor_program_order dpo ON foc.order_id=dpo.order_id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON dpo.order_id=fdpo.order_id
	LEFT JOIN (
				SELECT task_id, 
						MAX(CASE WHEN case_log_status_id=3 THEN 1 ELSE 0 END) AS call_initiated_flag,
						MAX(CASE WHEN case_log_status_id=4 THEN 1 ELSE 0 END) AS call_answered_flag
				FROM pe_docstat_91streets_media_technologies.case_log
				GROUP BY 1
	) sf ON fdt.task_id=sf.task_id
	WHERE DATE(fo.order_placed_at) BETWEEN '2020-01-01' AND '2020-01-31' 
)
WHERE call_initiated_flag=1 AND task_action IS NOT NULL; 



------Testing Hypothesis - acquiring queue snapshot - all orders on Hold at a particular time - 11 am on 10th Jan


SELECT oh.order_id, fdt.case_id, 
			fo.order_placed_at, fo.delivery_pincode, COALESCE(foc.order_source,'Refill') AS order_source, fo.is_add_to_cart,
			fo.is_courier, fo.supplier_city_name, foc.user_type_monthly, foc.chronic_flag_old, foc.customer_type,
			fdpo.successful_consultation_type, fdpo.first_moved_to_dp_at, 
			fdpo.instances_moved_to_dp, fdpo.num_of_doc_attempts, fdpo.num_of_times_on_hold,
			CASE dpo."source" WHEN 3 THEN 'CnR-Mediassist'					
						  WHEN 4 THEN 'CC-Valid-M2DP'        
						  WHEN 6 THEN 'Customer-App/Web'  
						  WHEN 7 THEN 'CC-New-InvalidRx' 				
						  WHEN 8 THEN 'RefillOrder-CC-M2DP'			
						  WHEN 9 THEN 'OrderOnCall'			
						  WHEN 10 THEN 'CC-Reject'	
						  ELSE 'NotSure'
			END AS dp_order_source,
			fdpo.issue_flag,
			in_dp_at, 
			out_dp_at,
			COUNT(CASE WHEN call_initiated_flag=1 AND task_action IS NOT NULL THEN fdt.task_id END) AS total_attempts,
			COUNT(CASE WHEN call_initiated_flag=1 AND ((task_action IN ('Hold', 'Rescheduled')) OR (task_action='Doctor Rejected' AND reject_reason='Other')) THEN fdt.task_id END) AS unsuccessful_attempts,
			COUNT(CASE WHEN call_initiated_flag=1 AND ((task_action IN ('Rx Prescribed', 'Cancelled', 'Docstat Rejected')) OR (task_action='Doctor Rejected' AND reject_reason NOT IN ('Other'))) THEN fdt.task_id END) AS successful_attempts
--			fdt.task_id, fdt.doctor_name, fdt.doctor_inactive_flag, fdt.doctor_notified_count, 
--			fdt.task_assigned_at, fdt.task_actioned_at, fdt.task_action, fdt.reject_reason,	
--			LEAD(task_assigned_at,1) OVER (PARTITION BY case_id ORDER BY task_assigned_at) AS next_task_assigned_at,
--			call_initiated_flag, call_answered_flag
	--		ROW_NUMBER() OVER(PARTITION BY case_id ORDER BY task_assigned_at) AS attempt_number,
	--		ROW_NUMBER() OVER(PARTITION BY case_id ORDER BY task_assigned_at DESC) AS attempt_number_last
	FROM (
			SELECT order_id, MIN(status_at) AS in_dp_at, MAX(next_status_at) AS out_dp_at
			FROM (
				SELECT order_id, order_status, DATEADD(min,330,"timestamp") AS status_at, 
						LEAD(DATEADD(min,330,"timestamp"),1) OVER (PARTITION BY order_id ORDER BY id) AS next_status_at
				FROM pe_pe2_pe2.order_history oh
				WHERE "timestamp">'2020-01-01'
			) 
			WHERE order_status=49 AND ('2020-01-10 11:00:00')::TIMESTAMP BETWEEN status_at AND next_status_at
			GROUP BY 1
	) oh 
	LEFT JOIN data_model.f_docstat_task fdt ON oh.order_id=fdt.order_id AND LENGTH(fdt.order_id)=8 AND fdt.task_assigned_at<'2020-01-10 11:00:00'::TIMESTAMP
	LEFT JOIN data_model.f_order fo ON oh.order_id=fo.order_id --AND LENGTH(fdt.order_id)=8 AND fdt.task_assigned_at BETWEEN fo.cc_min_in_dp_at AND fo.cc_min_out_dp_at
	LEFT JOIN data_model.f_order_consumer foc ON fo.order_id=foc.order_id
	LEFT JOIN pe_pe2_pe2.doctor_program_order dpo ON foc.order_id=dpo.order_id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON dpo.order_id=fdpo.order_id
	LEFT JOIN (
				SELECT task_id, 
						MAX(CASE WHEN case_log_status_id=3 THEN 1 ELSE 0 END) AS call_initiated_flag,
						MAX(CASE WHEN case_log_status_id=4 THEN 1 ELSE 0 END) AS call_answered_flag
				FROM pe_docstat_91streets_media_technologies.case_log
				GROUP BY 1
	) sf ON fdt.task_id=sf.task_id
	WHERE DATE(fo.order_placed_at) BETWEEN '2020-01-01' AND '2020-01-31'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
; 


SELECT order_id 
FROM (
	SELECT order_id, order_status, DATEADD(min,330,"timestamp") AS status_at, 
			LEAD(DATEADD(min,330,"timestamp"),1) OVER (PARTITION BY order_id ORDER BY id) AS next_status_at
	FROM pe_pe2_pe2.order_history oh
	WHERE "timestamp">'2020-01-01'
) 
WHERE order_status=49 AND ('2020-01-14 11:00:00')::TIMESTAMP BETWEEN status_at AND next_status_at
GROUP BY 1;
