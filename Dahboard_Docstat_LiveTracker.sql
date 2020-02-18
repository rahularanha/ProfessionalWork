
--- Day Comparator query

SELECT EXTRACT(hour from dateadd(m,330,dpoh.created_at)) AS hour_of_the_day,
        CASE WHEN dpoh."action"=1 THEN 'Moved To DP'
 			 WHEN dpoh."action"=4 THEN 'Prescribed'
	    END AS category_filter,
	    CASE WHEN ordf.order_id IS NOT NULL THEN 1 ELSE 0 END AS courier_flag,
	    CASE WHEN o.api_source=5 THEN 1 ELSE 0 END AS is_mediassist_order,
	    CASE WHEN dpo.doctor_program_id IN (2,7,9,25,47,62,66,67,68,8,11,43,49,55,70,172,1) THEN 'Other_Third_Party'
				 --WHEN my_dpo.doctor_program_id=2 THEN 'DocsApp'
				 WHEN dpo.doctor_program_id IN (75,158,159) THEN 'LetsDoc'
				 WHEN dpo.doctor_program_id IS NULL THEN 'NotAssigned/ReassignedToAdmin'
				 ELSE 'Docstat'
			END AS doc_category,
	    CASE dpo."source" WHEN 3 THEN 'CnR-Mediassist'					
						  WHEN 4 THEN 'CC-Valid-M2DP'        
						  WHEN 6 THEN 'Customer-App/Web'  
						  WHEN 7 THEN 'CC-New-InvalidRx' 				
						  WHEN 8 THEN 'RefillOrder-CC-M2DP'			
  						  WHEN 9 THEN 'OrderOnCall'			
						  WHEN 10 THEN 'CC-Reject'	
						  ELSE 'NotSure'
		END AS dp_order_source,
	    COUNT(CASE WHEN DATE(DATEADD(MIN,330,dpoh.created_at))=DATE(DATEADD(MIN,330,GETDATE())) THEN dpoh.order_id END) AS todays,
	    --COUNT(DISTINCT CASE WHEN DATE(DATEADD(MIN,330,dpoh.created_at))=DATE(DATEADD(MIN,330,GETDATE())) THEN dpoh.order_id END) AS todays,
	    COUNT(CASE WHEN DATE(DATEADD(MIN,330,dpoh.created_at))=DATE(DATEADD(MIN,330,GETDATE()))-1 THEN dpoh.order_id END) AS yesterday,
	    COUNT(CASE WHEN DATE(DATEADD(MIN,330,dpoh.created_at))=DATE(DATEADD(MIN,330,GETDATE()))-7 THEN dpoh.order_id END) AS today_last_week,
	    COUNT(CASE WHEN DATE(DATEADD(MIN,330,dpoh.created_at))=DATE(DATEADD(MIN,330,GETDATE()))-14 THEN dpoh.order_id END) AS today_last_fortnight,
	    COUNT(CASE WHEN DATE(DATEADD(MIN,330,GETDATE()))-DATE(DATEADD(MIN,330,dpoh.created_at)) IN (7,14,21,28) THEN dpoh.order_id END) AS avg_last4_same_weekday
FROM pe_pe2_pe2."order" o
LEFT JOIN pe_pe2_pe2.doctor_program_order dpo ON o.id=dpo.order_id
LEFT JOIN pe_pe2_pe2.doctor_program_order_history dpoh ON dpo.order_id=dpoh.order_id
LEFT JOIN pe_pe2_pe2.city c1 ON o.city_id = c1.id
LEFT JOIN pe_pe2_pe2.city c2 ON c2.id = c1.supplier_city_id
LEFT JOIN (  SELECT o.customer_id,min(dateadd(m,330,o.time_stamp)) AS first_delivered_order_time
   				FROM pe_pe2_pe2."order" o
  				WHERE o.status in (9,10)
  				GROUP BY 1) fo ON fo.customer_id = o.customer_id
LEFT JOIN pe_pe2_pe2.order_flags ordf ON o.id=ordf.order_id AND ordf.flag_id=19
WHERE (o.retailer_id not in (12,80,82,115,85,69,63) OR o.retailer_id IS NULL) AND (dpoh."action"=1 OR dpoh."action"=4)
GROUP BY 1,2,3,4,5,6;


----- Skull final query for Microstategy

WITH my_dpo AS (
	SELECT order_id, DATEADD(MIN,330,created_at) AS created_at, doctor_program_id, doctor_order_type, "source", 
			DATEADD(MIN,330,consultation_date) AS consultation_date,
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
			END AS "action"
	FROM pe_pe2_pe2.doctor_program_order
),
my_dpoh AS (
	SELECT id, order_id, DATEADD(MIN,330,created_at) AS created_at, "action", doctor_program_id,
			ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY id DESC) AS ranking
	FROM pe_pe2_pe2.doctor_program_order_history
),
my_order AS (
	SELECT id AS order_id, customer_id, address_id, DATEADD(MIN,330,time_stamp) AS o_placed_at, order_value, city_id, 
			platform AS order_source, status , api_source
	FROM pe_pe2_pe2."order"
	WHERE retailer_id NOT IN (12,80,82,115,85,69,63) OR retailer_id IS NULL
),
my_of AS (
	SELECT order_id FROM pe_pe2_pe2.order_flags WHERE flag_id=19 GROUP BY 1
),
my_oedd AS (
	SELECT order_id, DATE(estd_delivery_date) AS estd_delivery_date,
			ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY id) AS ranking
	FROM pe_pe2_pe2.order_estd_delivery_history
),
dp_superset AS (
	SELECT o.*, my_oedd.estd_delivery_date AS original_estd_delivery_date, 
			CASE WHEN my_of.order_id IS NOT NULL THEN 1 ELSE 0 END AS courier_flag,
			CASE WHEN my_dpo.doctor_program_id IN (2,7,9,25,47,62,66,67,68,8,11,43,49,55,70,172,1) THEN 'Other_Third_Party'
				 --WHEN my_dpo.doctor_program_id=2 THEN 'DocsApp'
				 WHEN my_dpo.doctor_program_id IN (75,158,159) THEN 'LetsDoc'
				 WHEN my_dpo.doctor_program_id IS NULL THEN 
						CASE WHEN a.lastest_doctor_program_id IN (2,7,9,25,47,62,66,67,68,8,11,43,49,55,70,172,1) THEN 'Other_Third_Party'
							 --WHEN my_dpo.doctor_program_id=2 THEN 'DocsApp'
							 WHEN a.lastest_doctor_program_id IN (75,158,159) THEN 'LetsDoc'
							 WHEN a.lastest_doctor_program_id IS NULL THEN 'Not Applicable'
							 ELSE 'Docstat'
						END
				 ELSE 'Docstat'
			END AS doc_category, 
			my_dpo.consultation_date AS consultation_requested_at,
			my_dpo."action" AS latest_doc_action,
			a.moved_to_dp_at,
			a.assigned_to_doc_at,
			a.first_response_at,
			a.last_response_at,
			a.rx_first_prescribed_at,
			a.rx_last_prescribed_at,
			a.rejected_at,
			a.num_of_times_on_hold,
			a.num_of_doc_attempts,
			CASE my_dpo.doctor_order_type WHEN 2 THEN 1 ELSE 0 END AS dpo_issue_flag,
			CASE my_dpo."source"  WHEN 3 THEN 'CnR-Mediassist'					
								  WHEN 4 THEN 'CC-Valid-M2DP'        
								  WHEN 6 THEN 'Customer-App/Web'  
								  WHEN 7 THEN 'CC-New-InvalidRx' 				
								  WHEN 8 THEN 'RefillOrder-CC-M2DP'			
		  						  WHEN 9 THEN 'OrderOnCall'			
								  WHEN 10 THEN 'CC-Reject'	
								  ELSE 'NotSure'
			END AS dp_order_source
	FROM my_dpo
	INNER JOIN my_order o ON my_dpo.order_id=o.order_id AND o.status=49
	INNER JOIN (
				SELECT order_id, MIN(CASE WHEN "action" IN (1,2) THEN created_at END) AS moved_to_dp_at,
								 MIN(CASE "action" WHEN 2 THEN created_at END) AS assigned_to_doc_at,
								 MIN(CASE WHEN "action" IN (3,4,5,8,10) THEN created_at END) AS first_response_at,
								 MAX(CASE WHEN "action" IN (3,4,5,8,10) THEN created_at END) AS last_response_at,
								 MIN(CASE "action" WHEN 4 THEN created_at END) AS rx_first_prescribed_at,
								 MAX(CASE "action" WHEN 4 THEN created_at END) AS rx_last_prescribed_at,
								 MAX(CASE "action" WHEN 3 THEN created_at END) AS rejected_at,
								 COUNT(DISTINCT CASE "action" WHEN 5 THEN created_at END) AS num_of_times_on_hold,
								 COUNT(DISTINCT CASE WHEN "action" IN (3,4,5,8,10) THEN created_at END) AS num_of_doc_attempts,
								 MIN(CASE WHEN ranking=2 THEN doctor_program_id END) AS lastest_doctor_program_id
				FROM my_dpoh
				GROUP BY 1
			) a ON my_dpo.order_id=a.order_id
	LEFT JOIN my_of ON my_dpo.order_id=my_of.order_id
	LEFT JOIN my_oedd ON my_dpo.order_id=my_oedd.order_id AND ranking=1
)
SELECT order_id, customer_id, o_placed_at, order_source, 
		CASE WHEN api_source=5 THEN 1 ELSE 0 END AS mediassist_flag,
		original_estd_delivery_date, courier_flag, doc_category, latest_doc_action,
		moved_to_dp_at, assigned_to_doc_at, first_response_at, last_response_at, rx_first_prescribed_at, rx_last_prescribed_at, 
		rejected_at, num_of_times_on_hold, num_of_doc_attempts, dpo_issue_flag, dp_order_source 
FROM dp_superset
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 
ORDER BY 10;



---- Skull query for Spotfire dashboard

WITH dp_timings AS (
	SELECT RIGHT(opening_time,8) AS opening_time, 
			RIGHT(closing_time,8) AS closing_time, 
			(DATEDIFF(MIN, opening_time, closing_time)) AS mins_open
	FROM (
		SELECT '2000-01-01 08:30:00'::TIMESTAMP AS opening_time, '2000-01-01 23:00:00'::TIMESTAMP AS closing_time
		)
),
my_dpo AS (
	SELECT order_id, DATEADD(MIN,330,created_at) AS created_at, doctor_program_id, "action", doctor_order_type, "source", DATEADD(MIN,330,consultation_date) AS consultation_date
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
	SELECT o.*, 
			c1.name AS demand_city, c2.name AS supplier_city,
		   CASE WHEN o.status IN (9,10) THEN 'Fulfilled'
			 	WHEN o.status=2 THEN 'Rejected'
			 	WHEN o.status=8 THEN 'Cancelled'
			 	WHEN o.status=49 THEN 'In_DP'
				ELSE 'Under_Process'
		   END AS order_status,
		   	CASE WHEN my_dpo.order_id=csc.order_id THEN 'First Successful Consultation'
				 WHEN my_dpo.order_id>csc.order_id THEN 'Repeat'
				 ELSE 'Zero Successful Consultations'
		    END AS customer_consultation_number,
			dp.name AS doc_name,
			CASE WHEN my_dpo.doctor_program_id IN (2,7,9,25,47,62,66,67,68,158,159,8,11,43,49,55,70,75,172,1) THEN 'Third_Party'
				 WHEN my_dpo.doctor_program_id IS NULL THEN 'Not Applicable'
				 ELSE 'Docstat'
			END AS doc_category, 
			my_dpo.consultation_date AS consultation_requested_at,
			my_dpo."action" AS final_doc_action,
			a.moved_to_dp_at,
			a.assigned_to_doc_at,
			CASE WHEN canned_in_dp.order_id IS NULL THEN a.first_response_at END AS first_response_at,
			a.rx_first_prescribed_at,
			a.rx_last_prescribed_at,
			a.rejected_at,
			a.num_of_times_on_hold,
			CASE WHEN canned_in_dp.order_id IS NOT NULL THEN 1 END AS canned_in_dp_before_frt,
			CASE my_dpo.doctor_order_type WHEN 2 THEN 1 ELSE 0 END AS dpo_issue_flag,
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
			END AS dp_order_source,
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
			END	AS dp_frt_tat14,
			CASE 
				 WHEN DATE(GETDATE())=DATE(moved_to_dp_at)
				 THEN CASE 
						 WHEN RIGHT(moved_to_dp_at, 8)<opening_time 
							THEN (GETDATE()-((DATE(GETDATE())||' '||opening_time)::TIMESTAMP))
						 ELSE (GETDATE()-moved_to_dp_at)
						 END
				 ELSE
				 	  CASE 
				 	  	 WHEN (RIGHT(moved_to_dp_at, 8)<=opening_time)
							THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-((DATE(moved_to_dp_at)||' '||opening_time)::TIMESTAMP))+
								 ( (DATEADD(MIN, ((DATE(GETDATE())-DATE(moved_to_dp_at)-1)*mins_open), GETDATE()))-( (DATE(GETDATE())||' '||opening_time)::TIMESTAMP) )
						 WHEN ((RIGHT(moved_to_dp_at, 8)>opening_time) AND (RIGHT(moved_to_dp_at, 8)<closing_time))
							THEN (((DATE(moved_to_dp_at)||' '||closing_time)::TIMESTAMP)-moved_to_dp_at)+
								 ( (DATEADD(MIN, ((DATE(GETDATE())-DATE(moved_to_dp_at)-1)*mins_open), GETDATE()))-( (DATE(GETDATE())||' '||opening_time)::TIMESTAMP) )
						 ELSE
						 		 ( (DATEADD(MIN, ((DATE(GETDATE())-DATE(moved_to_dp_at)-1)*mins_open), GETDATE()))-( (DATE(GETDATE())||' '||opening_time)::TIMESTAMP) )
						 END
			END AS time_from_in_dp,
		   CASE WHEN atc.order_id IS NOT NULL THEN 1 ELSE 0 END AS atc_flag,
		   CASE WHEN courier.order_id IS NOT NULL THEN 1 ELSE 0 END AS courier_flag,
		   CASE WHEN chronic.order_id IS NOT NULL THEN 1 ELSE 0 END AS chronic_flag,
		   CASE WHEN new_customer.customer_id IS NULL THEN 1 
				ELSE CASE WHEN EXTRACT(MONTH FROM new_customer.first_fulfilled_order_time)=EXTRACT(MONTH FROM o.o_placed_at)
					   THEN 1
					   ELSE 0
					 END
		   END AS new_customer_flag
	FROM my_dpo
	INNER JOIN dp_timings dp_time ON dp_time.opening_time IS NOT NULL
	INNER JOIN my_order o ON my_dpo.order_id=o.order_id
	INNER JOIN (
				SELECT order_id, MIN(CASE WHEN "action" IN (1,2) THEN created_at END) AS moved_to_dp_at,
								 MIN(CASE "action" WHEN 2 THEN created_at END) AS assigned_to_doc_at,
								 MIN(CASE WHEN "action" IN (3,4,5,8,10) THEN created_at END) AS first_response_at,
								 MIN(CASE "action" WHEN 4 THEN created_at END) AS rx_first_prescribed_at,
								 MAX(CASE "action" WHEN 4 THEN created_at END) AS rx_last_prescribed_at,
								 MAX(CASE "action" WHEN 3 THEN created_at END) AS rejected_at,
								 COUNT(CASE "action" WHEN 5 THEN order_id END) AS num_of_times_on_hold
				FROM my_dpoh
				GROUP BY 1
			) a ON my_dpo.order_id=a.order_id	
	LEFT JOIN pe_pe2_pe2.city c1 ON o.city_id=c1.id
	LEFT JOIN pe_pe2_pe2.city c2 ON c1.supplier_city_id=c2.id
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
	LEFT JOIN pe_pe2_pe2.doctor_program dp ON my_dpo.doctor_program_id=dp.doctor_program_id
	LEFT JOIN (SELECT order_id FROM pe_pe2_pe2.order_flags WHERE flag_id IN (22,23) GROUP BY 1) atc ON my_dpo.order_id=atc.order_id
	LEFT JOIN (SELECT order_id FROM pe_pe2_pe2.order_flags WHERE flag_id=19 GROUP BY 1) courier ON my_dpo.order_id=courier.order_id
	LEFT JOIN (SELECT order_id 
				FROM pe_pe2_pe2.order_digitization_flag odf 
				INNER JOIN pe_pe2_pe2.medicine_notes mn ON odf.medicine_notes_id=mn.id AND (is_deleted=0 OR delete_reason NOT IN (3,5,13)) 
				WHERE odf.flag_id=31 GROUP BY 1) chronic ON my_dpo.order_id=chronic.order_id
	LEFT JOIN (SELECT customer_id, MIN(o_placed_at) AS first_fulfilled_order_time 
				FROM my_order o WHERE status IN (9,10) GROUP BY 1) new_customer ON o.customer_id=new_customer.customer_id
),
final_oh AS (
	SELECT id,order_id, CASE "action" WHEN 1 THEN 'Moved_to_DP'
									  WHEN 2 THEN 'Assigned'
									  WHEN 3 THEN 'Rejected'
									  WHEN 4 THEN 'Accepted'
									  WHEN 5 THEN 'On Hold'
									  WHEN 6 THEN 'Awaiting_Order_Creation'
									  WHEN 7 THEN 'Order_Created'
									  WHEN 8 THEN 'Doctor_Reassigned'
									  WHEN 9 THEN 'Queued'
									  WHEN 10 THEN 'Resheduled'
						 END AS dp_status, 
			DATEADD(MIN,0,created_at) AS status_changed_at, day_filter, hour24_filter, ranking
	FROM(
		SELECT *, LEAD(created_at,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status_at,
				RANK() OVER (PARTITION BY order_id ORDER BY id) AS ranking,
				CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE())) THEN 'today'
				ELSE CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE()))-1 THEN 'yesterday'
					 ELSE CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE()))-2 THEN 'day_before'
					 	  ELSE CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE()))-7 THEN 'today_last_week' 
						 	   ELSE CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE()))-8 THEN 'yesterday_last_week' 
						 	  	    ELSE CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE()))-14 THEN 'today_last_last_week' 
							 	   		 ELSE CASE WHEN DATE(DATEADD(MIN,0,created_at))=DATE(DATEADD(MIN,0,GETDATE()))-15 THEN 'yesterday_last_last_week' 
							 	  		 	  END
							 	  		 END
						 	  	    END
						 	   END
					 	 END
					 END
				END AS day_filter,
				CASE WHEN DATEDIFF(MIN, created_at,GETDATE())<=1440 THEN 1 
					 WHEN DATEDIFF(MIN, created_at,GETDATE())>1440 AND DATEDIFF(MIN, created_at,GETDATE())<=2880 THEN 2
					 WHEN DATEDIFF(MIN, created_at,GETDATE())>11520 AND DATEDIFF(MIN, created_at,GETDATE())<=12960 THEN 9
					 WHEN DATEDIFF(MIN, created_at,GETDATE())>21600 AND DATEDIFF(MIN, created_at,GETDATE())<=23040 THEN 16
				END AS hour24_filter
		FROM my_dpoh
	)
	WHERE (DATEDIFF(SECOND, created_at, next_status_at)>2 OR DATEDIFF(SECOND, created_at, next_status_at) IS NULL) AND (day_filter IS NOT NULL OR hour24_filter IS NOT NULL)
	GROUP BY 1,2,3,4,5,6,7
	ORDER BY order_id,id
)
SELECT dp_superset.order_id, customer_id, customer_consultation_number, o_placed_at, order_source, supplier_city, order_status, 
		new_customer_flag, atc_flag, chronic_flag, courier_flag, doc_name, doc_category, final_doc_action, canned_in_dp_before_frt,
		moved_to_dp_at, assigned_to_doc_at, first_response_at, rx_first_prescribed_at, rx_last_prescribed_at, 
		rejected_at, num_of_times_on_hold, dpo_issue_flag, dp_order_source, 
		EXTRACT(epoch FROM last_prescribed_tat24)/60 AS last_prescribed_tat_overall, EXTRACT(epoch FROM last_prescribed_tat14)/60 AS last_prescribed_tat_office_hour, 
		EXTRACT(epoch FROM first_prescribed_tat24)/60 AS first_prescribed_tat_overall, EXTRACT(epoch FROM first_prescribed_tat14)/60 AS first_prescribed_tat_office_hour,
		EXTRACT(epoch FROM dp_frt_tat24)/60 AS dp_frt_tat_overall, EXTRACT(epoch FROM dp_frt_tat14)/60 AS dp_frt_tat_office_hour,
		CASE WHEN (EXTRACT(epoch FROM (CASE WHEN dp_frt_tat14 IS NOT NULL THEN dp_frt_tat14 ELSE time_from_in_dp END))/60)>60 
			 THEN 1 ELSE 0
		END AS frt_sla_breached_internal,
		CASE WHEN (EXTRACT(epoch FROM (CASE WHEN dp_frt_tat14 IS NOT NULL THEN dp_frt_tat14 ELSE time_from_in_dp END))/60)>120 
			 THEN 1 ELSE 0
		END AS frt_sla_breached_committed,
		CASE WHEN first_prescribed_tat14 IS NOT NULL 
			 THEN CASE WHEN (EXTRACT(epoch FROM first_prescribed_tat14)/60)>120 THEN 1 ELSE 0 END
		END AS overall_sla_breached_internal,
		final_oh.id,final_oh.dp_status,final_oh.status_changed_at,final_oh.day_filter,final_oh.hour24_filter,final_oh.ranking
FROM dp_superset
INNER JOIN dp_timings dp_time ON dp_time.opening_time IS NOT NULL
LEFT JOIN final_oh ON dp_superset.order_id=final_oh.order_id
WHERE dp_superset.order_status='In_DP' OR final_oh.id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39;
