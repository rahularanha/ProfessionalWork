--- Data to be updated

CREATE TEMPORARY TABLE updated_orders AS
SELECT dpo.order_id
FROM pe_pe2_pe2.doctor_program_order dpo
LEFT JOIN data_model.f_doctor_program_order fdpo ON dpo.order_id=fdpo.order_id
WHERE DATE(dpo.updated_at) >= DATE(DATEADD(days,-3,current_date)) OR fdpo.order_id IS NULL
GROUP BY 1;


--- Consolidated

CREATE TEMPORARY TABLE f_dp_order_final AS
WITH dp_timings AS (
	SELECT RIGHT(opening_time,8) AS opening_time, 
			RIGHT(closing_time,8) AS closing_time, 
			(DATEDIFF(MIN, opening_time, closing_time)) AS mins_open
	FROM (
		SELECT '2000-01-01 08:30:00'::TIMESTAMP AS opening_time, '2000-01-01 23:00:00'::TIMESTAMP AS closing_time
		)
),
my_dpo AS (
	SELECT order_id, DATEADD(MIN,330,created_at) AS created_at, doctor_program_id, "action", doctor_order_type, "source", consultation_date
	FROM pe_pe2_pe2.doctor_program_order
),
my_dpoh AS (
	SELECT id, order_id, DATEADD(MIN,330,created_at) AS created_at, "action"
	FROM pe_pe2_pe2.doctor_program_order_history
),
my_order AS (
	SELECT id AS order_id, customer_id, address_id, DATEADD(MIN,330,time_stamp) AS o_placed_at, order_value, city_id, platform AS order_source, status 
	FROM pe_pe2_pe2."order"
	WHERE retailer_id NOT IN (12,80,82,115,85,69,63) OR retailer_id IS NULL
),
my_oh AS (
	SELECT id, order_id, order_status, DATEADD(MIN,330,"timestamp") AS "timestamp"
	FROM pe_pe2_pe2.order_history
),
dp_superset AS (
	SELECT my_dpo.order_id, o.customer_id, o.o_placed_at,
			CASE WHEN my_dpo.order_id=csc.order_id THEN 'First Successful Consultation'
				 WHEN my_dpo.order_id>csc.order_id THEN 'Repeat'
				 ELSE 'Zero Successful Consultations'
		    END AS customer_consultation_number,
			dp.name AS doctor_name,
			CASE WHEN my_dpo.doctor_program_id IN (2,7,9,25,47,62,66,67,68,158,159,8,11,43,49,55,70,75,172,1) THEN 'Third_Party'
				 WHEN my_dpo.doctor_program_id IS NULL THEN 'Not Applicable'
				 ELSE 'Docstat'
			END AS doctor_category, 
			my_dpo.consultation_date AS consultation_requested_at,
			my_dpo."action" AS latest_case_status,
			a.moved_to_dp_at AS first_moved_to_dp_at,
			dp_instances.last_moved_to_dp_at,
			a.first_assigned_to_doc_at,
			CASE WHEN canned_in_dp.order_id IS NULL THEN a.first_response_at END AS first_response_at,
			a.rx_first_prescribed_at,
			a.last_assigned_to_doc_at,
			a.rx_last_prescribed_at,
			a.rejected_at AS case_rejected_at,
			first_last_response.first_response,first_last_response.last_response AS first_in_dp_last_response,first_last_response.last_response_at AS first_in_dp_last_response_at,
			a.last_response_at AS last_in_dp_last_response_at,					 
			dp_instances.no_of_times_in_dp AS instances_moved_to_dp,
			a.num_of_times_on_hold,
			a.num_of_doc_attempts,
			CASE WHEN canned_in_dp.order_id IS NOT NULL THEN 1 END AS cancelled_before_frt,
			CASE my_dpo.doctor_order_type WHEN 2 THEN 1 ELSE 0 END AS issue_flag,
			CASE my_dpo."source" WHEN 1 THEN 'Offline Panel'
							  WHEN 2 THEN 'Order Without Prescription' 
							  WHEN 3 THEN 'Help Me'
							  WHEN 4 THEN 'Centralize Calling'    
							  WHEN 5 THEN 'Consumer App Doctor Push'
							  WHEN 6 THEN 'Order Without Prescription (Consumer App)'
							  WHEN 7 THEN 'Tech Bot' 
							  WHEN 8 THEN 'Subscription'
      						  WHEN 9 THEN 'Order On Call'
							  WHEN 10 THEN 'Order Rejected'
			END AS order_source, 
			dprr.refuse_reason AS customer_refuse_reason, dpcr.reject_reason AS dp_rejection_reason,
			CASE WHEN dpcr.reject_reason NOT IN ('Order Cancelled by user','Order Cancelled By Moderator','Customer not receiving call') THEN 'doctor'
				 WHEN dpcr.reject_reason IN ('Order Cancelled by user','Order Cancelled By Moderator','Customer not receiving call') THEN 'customer/other'
			END AS case_rejected_by,			
			(rx_last_prescribed_at-moved_to_dp_at) AS last_prescribed_tat24, (rx_first_prescribed_at-moved_to_dp_at) AS first_prescribed_tat24, 
			CASE WHEN canned_in_dp.order_id IS NULL THEN (first_response_at-moved_to_dp_at) END AS dp_frt_tat24, 
			CASE WHEN rx_last_prescribed_at IS NOT NULL THEN
				  CASE 
					 WHEN DATE(rx_last_prescribed_at)=DATE(moved_to_dp_at)
					 THEN CASE 
							 WHEN RIGHT(moved_to_dp_at, 8)<opening_time 
								THEN (rx_last_prescribed_at-((DATE(rx_last_prescribed_at)||' '||opening_time)::TIMESTAMP))
							 ELSE (rx_last_prescribed_at-moved_to_dp_at)
							 END
					 ELSE
					 	  CASE 
					 	  	 WHEN (RIGHT(moved_to_dp_at, 8)<=opening_time)
								THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-((DATE(moved_to_dp_at)||' '||opening_time)::TIMESTAMP))+
									 ( (DATEADD(MIN, ((DATE(rx_last_prescribed_at)-DATE(moved_to_dp_at)-1)*mins_open), rx_last_prescribed_at))-( (DATE(rx_last_prescribed_at)||' '||opening_time)::TIMESTAMP) )
							 WHEN ((RIGHT(moved_to_dp_at, 8)>opening_time) AND (RIGHT(moved_to_dp_at, 8)<closing_time))
								THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-moved_to_dp_at)+
									 ( (DATEADD(MIN, ((DATE(rx_last_prescribed_at)-DATE(moved_to_dp_at)-1)*mins_open), rx_last_prescribed_at))-( (DATE(rx_last_prescribed_at)||' '||opening_time)::TIMESTAMP) )
							 ELSE
							 		 ( (DATEADD(MIN, ((DATE(rx_last_prescribed_at)-DATE(moved_to_dp_at)-1)*mins_open), rx_last_prescribed_at))-( (DATE(rx_last_prescribed_at)||' '||opening_time)::TIMESTAMP) )
							 END
				  END
			END AS last_prescribed_tat14,
			CASE WHEN rx_first_prescribed_at IS NOT NULL THEN
				  CASE 
					 WHEN DATE(rx_first_prescribed_at)=DATE(moved_to_dp_at)
					 THEN CASE 
							 WHEN RIGHT(moved_to_dp_at, 8)<opening_time 
								THEN (rx_first_prescribed_at-((DATE(rx_first_prescribed_at)||' '||opening_time)::TIMESTAMP))
							 ELSE (rx_first_prescribed_at-moved_to_dp_at)
							 END
					 ELSE
					 	  CASE 
					 	  	 WHEN (RIGHT(moved_to_dp_at, 8)<=opening_time)
								THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-((DATE(moved_to_dp_at)||' '||opening_time)::TIMESTAMP))+
									 ( (DATEADD(MIN, ((DATE(rx_first_prescribed_at)-DATE(moved_to_dp_at)-1)*mins_open), rx_first_prescribed_at))-( (DATE(rx_first_prescribed_at)||' '||opening_time)::TIMESTAMP) )
							 WHEN ((RIGHT(moved_to_dp_at, 8)>opening_time) AND (RIGHT(moved_to_dp_at, 8)<closing_time))
								THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-moved_to_dp_at)+
									 ( (DATEADD(MIN, ((DATE(rx_first_prescribed_at)-DATE(moved_to_dp_at)-1)*mins_open), rx_first_prescribed_at))-( (DATE(rx_first_prescribed_at)||' '||opening_time)::TIMESTAMP) )
							 ELSE
							 		 ( (DATEADD(MIN, ((DATE(rx_first_prescribed_at)-DATE(moved_to_dp_at)-1)*mins_open), rx_first_prescribed_at))-( (DATE(rx_first_prescribed_at)||' '||opening_time)::TIMESTAMP) )
							 END
				  END
			END AS first_prescribed_tat14,
			CASE 
				 WHEN DATE(first_response_at)=DATE(moved_to_dp_at)
				 THEN CASE 
						 WHEN RIGHT(moved_to_dp_at, 8)<opening_time 
							THEN (first_response_at-((DATE(first_response_at)||' '||opening_time)::TIMESTAMP))
						 ELSE (first_response_at-moved_to_dp_at)
						 END
				 ELSE
				 	  CASE 
				 	  	 WHEN (RIGHT(moved_to_dp_at, 8)<=opening_time)
							THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-((DATE(moved_to_dp_at)||' '||opening_time)::TIMESTAMP))+
								 ( (DATEADD(MIN, ((DATE(first_response_at)-DATE(moved_to_dp_at)-1)*mins_open), first_response_at))-( (DATE(first_response_at)||' '||opening_time)::TIMESTAMP) )
						 WHEN ((RIGHT(moved_to_dp_at, 8)>opening_time) AND (RIGHT(moved_to_dp_at, 8)<closing_time))
							THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-moved_to_dp_at)+
								 ( (DATEADD(MIN, ((DATE(first_response_at)-DATE(moved_to_dp_at)-1)*mins_open), first_response_at))-( (DATE(first_response_at)||' '||opening_time)::TIMESTAMP) )
						 ELSE
						 		 ( (DATEADD(MIN, ((DATE(first_response_at)-DATE(moved_to_dp_at)-1)*mins_open), first_response_at))-( (DATE(first_response_at)||' '||opening_time)::TIMESTAMP) )
						 END
			END	AS dp_frt_tat14		
	FROM updated_orders
	INNER JOIN my_dpo ON updated_orders.order_id=my_dpo.order_id
	INNER JOIN dp_timings dp_time ON dp_time.opening_time IS NOT NULL
	INNER JOIN my_order o ON my_dpo.order_id=o.order_id
	INNER JOIN (
				SELECT order_id, MIN(CASE WHEN "action" IN (1,2) THEN created_at END) AS moved_to_dp_at,
								 MIN(CASE "action" WHEN 2 THEN created_at END) AS first_assigned_to_doc_at,
								 MIN(CASE WHEN "action" IN (3,4,5,8,10) THEN created_at END) AS first_response_at,
								 MIN(CASE "action" WHEN 4 THEN created_at END) AS rx_first_prescribed_at,
								 MAX(CASE "action" WHEN 2 THEN created_at END) AS last_assigned_to_doc_at,
								 MAX(CASE WHEN "action" IN (3,4,5,8,10) THEN created_at END) AS last_response_at,
								 MAX(CASE "action" WHEN 4 THEN created_at END) AS rx_last_prescribed_at,
								 MAX(CASE "action" WHEN 3 THEN created_at END) AS rejected_at,
								 COUNT(CASE "action" WHEN 5 THEN order_id END) AS num_of_times_on_hold,
								 COUNT(CASE WHEN "action" IN (3,4,5,8,10) THEN order_id END) AS num_of_doc_attempts
				FROM my_dpoh
				GROUP BY 1
			) a ON my_dpo.order_id=a.order_id	
	LEFT JOIN (
				SELECT my_order.customer_id, MIN(my_order.order_id) AS order_id
				FROM my_dpo
				INNER JOIN my_order ON my_dpo.order_id=my_order.order_id
				WHERE "action"=4 
				GROUP BY 1
	) csc ON o.customer_id=csc.customer_id
	LEFT JOIN (
				SELECT order_id, order_status, "timestamp" AS status_at, 
						LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
						LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS next_status_at,
						CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
				FROM my_oh
				--WHERE DATE("timestamp")>='2018-04-01'
			) canned_in_dp ON (a.order_id=canned_in_dp.order_id AND canned_in_dp.next_status=8 AND canned_in_dp.dp_num=1 AND (DATEDIFF(SECOND,a.first_response_at,canned_in_dp.next_status_at)<40 OR a.first_response_at IS NULL))
	LEFT JOIN (
				SELECT order_id, "timestamp" AS last_moved_to_dp_at,
						CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS no_of_times_in_dp,
						CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id DESC) END AS dp_index
				FROM my_oh 
			) dp_instances ON my_dpo.order_id=dp_instances.order_id AND dp_index=1
	LEFT JOIN (
				SELECT order_id,
						MIN(CASE WHEN asc_num=1 THEN doc_action END) AS first_response,
						MIN(CASE WHEN desc_num=1 THEN doc_action END) AS last_response,
						MIN(CASE WHEN desc_num=1 THEN doc_actioned_at END) AS last_response_at
				FROM (
					SELECT my_dpoh.order_id, 
							my_dpoh."action" AS doc_action, 
							my_dpoh.created_at AS doc_actioned_at, 
							ROW_NUMBER() OVER (PARTITION BY my_dpoh.order_id ORDER BY my_dpoh.id) as asc_num,
							ROW_NUMBER() OVER (PARTITION BY my_dpoh.order_id ORDER BY my_dpoh.id DESC) as desc_num
					FROM my_dpoh
					LEFT JOIN (SELECT order_id, order_status, "timestamp" AS in_dp_at, 
										LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
										LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at,
										CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
								FROM my_oh 
							) oh ON my_dpoh.order_id=oh.order_id AND oh.dp_num=1 AND my_dpoh.created_at BETWEEN in_dp_at AND out_dp_at
					WHERE my_dpoh."action" IN (3,4,5,8,10)
				)
				GROUP BY 1
			) first_last_response ON my_dpo.order_id=first_last_response.order_id
	LEFT JOIN pe_pe2_pe2.doctor_program dp ON my_dpo.doctor_program_id=dp.doctor_program_id
	LEFT JOIN (SELECT order_id, reject_reason_id, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC) AS ranking FROM pe_pe2_pe2.doctor_program_order_rejection) dpor ON my_dpo.order_id=dpor.order_id AND dpor.ranking=1
	LEFT JOIN pe_pe2_pe2.doctor_program_cancel_reason dpcr ON dpor.reject_reason_id=dpcr.id
	LEFT JOIN (SELECT order_id, refused_reason_id, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC) AS ranking FROM pe_pe2_pe2.doctor_program_order_refused) dporef ON my_dpo.order_id=dporef.order_id AND dporef.ranking=1 
	LEFT JOIN pe_pe2_pe2.doctor_program_refuse_reason dprr ON dporef.refused_reason_id=dprr.id
	--WHERE my_dpo.created_at>='2018-04-01'
)
SELECT order_id, customer_id, o_placed_at AS order_placed_at, doctor_name, doctor_category, order_source, consultation_requested_at, latest_case_status, 
		CASE WHEN customer_consultation_number='Repeat' THEN 'Repeat' ELSE 'First Time' END AS successful_consultation_type,
		cancelled_before_frt, instances_moved_to_dp, num_of_doc_attempts, num_of_times_on_hold, issue_flag, customer_refuse_reason, dp_rejection_reason, case_rejected_by,
		first_moved_to_dp_at, first_assigned_to_doc_at, first_response, first_response_at, first_in_dp_last_response, first_in_dp_last_response_at, 
		rx_first_prescribed_at, last_moved_to_dp_at, last_assigned_to_doc_at, rx_last_prescribed_at, last_in_dp_last_response_at, case_rejected_at, 
		CASE WHEN cancelled_before_frt=1 THEN NULL ELSE EXTRACT(epoch FROM dp_frt_tat24)/60 END AS dp_frt_tat_overall, 
		CASE WHEN cancelled_before_frt=1 THEN NULL ELSE EXTRACT(epoch FROM dp_frt_tat14)/60 END AS dp_frt_tat_office_hours,
		EXTRACT(epoch FROM first_prescribed_tat24)/60 AS first_prescribed_tat_overall, 
		EXTRACT(epoch FROM first_prescribed_tat14)/60 AS first_prescribed_tat_office_hours,
		EXTRACT(epoch FROM last_prescribed_tat24)/60 AS last_prescribed_tat_overall, 
		EXTRACT(epoch FROM last_prescribed_tat14)/60 AS last_prescribed_tat_office_hours, 
		CASE WHEN (EXTRACT(epoch FROM dp_frt_tat14)/60)>90 THEN 1 ELSE 0 END AS frt_sla_breached_internal,
		CASE WHEN (EXTRACT(epoch FROM dp_frt_tat14)/60)>120 THEN 1 ELSE 0 END AS frt_sla_breached_committed,
		CASE WHEN first_prescribed_tat14 IS NOT NULL 
			 THEN CASE WHEN (EXTRACT(epoch FROM first_prescribed_tat14)/60)>120 THEN 1 ELSE 0 END
		END AS overall_sla_breached_internal
FROM dp_superset
WHERE instances_moved_to_dp>=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38 
ORDER BY 1;


--- Deleting entries that are updated 
 	    
DELETE FROM data_model.f_doctor_program_order 
WHERE order_id IN (SELECT order_id FROM updated_orders group by 1);

					   
--- Inserting new/updated data

INSERT INTO data_model.f_doctor_program_order
SELECT * FROM f_dp_order_final;
