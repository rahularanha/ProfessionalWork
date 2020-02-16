SELECT fo.order_id, fo.customer_id, o.contact_number AS order_deliver_number, c.mobile_number AS customer_registered_number,
		
FROM pe2.f_order fo
LEFT JOIN pe2."order" o ON fo.order_id=o.id
LEFT JOIN pe2.customer c ON fo.customer_id=c.id


SELECT COUNT(DISTINCT call_id) FROM analytics.ozonetel_calls_dump


SELECT * FROM analytics.ozonetel_calls_dump LIMIT 100;


SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, 
		MAX(CASE WHEN o1.id IS NOT NULL OR o2.id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
		MAX(CASE WHEN (ocd.call_started_at BETWEEN o1.time_stamp AND o1.updated_at) OR (ocd.call_started_at BETWEEN o2.time_stamp AND o2.updated_at) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
		MAX(CASE WHEN fo.is_doctor_program=true OR fdpo.order_id IS NOT NULL THEN 1 ELSE 0 END) AS doctor_program_flag,
		MAX(CASE WHEN (ocd.call_started_at BETWEEN fo.cc_min_in_dp_at AND fo.cc_max_out_dp_at) OR (ocd.call_started_at BETWEEN fdpo.first_moved_to_dp_at AND fdpo.last_in_dp_last_response_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
		--CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END AS call_during_live_order_flag
FROM analytics.ozonetel_calls_dump ocd
LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number 
LEFT JOIN data_model.f_order fo ON o1.id=fo.order_id
LEFT JOIN pe2.customer c ON ocd.caller_no=c.mobile_number
LEFT JOIN pe2."order" o2 ON c.id=o2.customer_id
LEFT JOIN data_model.f_doctor_program_order fdpo ON o2.id=fdpo.order_id
GROUP BY 1,2,3
ORDER BY 3;



----PreFinal Query



SELECT call_id, caller_no, call_started_at,
		MAX(CASE WHEN customer_ordered_atleast_once_flag=1 AND order_id1 IS NOT NULL 
				 THEN order_id1 
				 ELSE CASE WHEN customer_ordered_atleast_once_flag=1 AND order_id2 IS NOT NULL 
				 		   THEN order_id2
				 	  END
			END) AS atleast_once_order_id,
		MAX(CASE WHEN customer_called_during_live_order_flag=1 AND order_id1 IS NOT NULL 
				 THEN order_id1 
				 ELSE CASE WHEN customer_called_during_live_order_flag=1 AND order_id2 IS NOT NULL 
				 		   THEN order_id2
				 	  END
			END) AS live_order_id,
		MAX(CASE WHEN live_doctor_program_flag=1 AND is_doctor_program=true 
				 THEN fo_order_id
				 ELSE CASE WHEN customer_called_during_live_order_flag=1 AND fdpo_order_id IS NOT NULL 
				 		   THEN fdpo_order_id
				 	  END
			END) AS dp_order_id,
		MAX(CASE WHEN customer_called_live_dp_flag=1 AND is_doctor_program=true 
				 THEN fo_order_id
				 ELSE CASE WHEN customer_called_during_live_order_flag=1 AND fdpo_order_id IS NOT NULL 
				 		   THEN fdpo_order_id
				 	  END
			END) AS live_dp_order_id
FROM (
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, o1.id AS order_id1, fo.order_id AS fo_order_id, o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN o1.id IS NOT NULL OR o2.id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN (ocd.call_started_at BETWEEN o1.time_stamp AND o1.updated_at) OR (ocd.call_started_at BETWEEN o2.time_stamp AND o2.updated_at) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN ((ocd.call_started_at BETWEEN o1.time_stamp AND o1.updated_at) AND fo.is_doctor_program=true) OR 
							((ocd.call_started_at BETWEEN o2.time_stamp AND o2.updated_at) AND fdpo.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo.cc_min_in_dp_at AND fo.cc_max_out_dp_at) OR (ocd.call_started_at BETWEEN fdpo.first_moved_to_dp_at AND fdpo.last_in_dp_last_response_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
			--CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END AS call_during_live_order_flag
	FROM analytics.ozonetel_calls_dump ocd
	LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number 
	LEFT JOIN data_model.f_order fo ON o1.id=fo.order_id
	LEFT JOIN pe2.customer c ON ocd.caller_no=c.mobile_number
	LEFT JOIN pe2."order" o2 ON c.id=o2.customer_id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON o2.id=fdpo.order_id
	--GROUP BY 1,2,3
	)
GROUP BY 1,2,3
ORDER BY 3;



------- Workaround



SELECT call_id, caller_no, call_started_at,
		MAX(customer_ordered_atleast_once_flag) AS customer_ordered_atleast_once_flag,
		MAX(customer_called_during_live_order_flag) AS customer_ordered_atleast_once_flag,
		MAX(live_doctor_program_flag) AS live_doctor_program_flag,
		MAX(customer_called_live_dp_flag) AS customer_called_live_dp_flag,
		MAX(CASE WHEN customer_ordered_atleast_once_flag=1 --AND order_id1 IS NOT NULL 
				 THEN order_id1 
				 --ELSE CASE WHEN customer_ordered_atleast_once_flag=1 AND order_id2 IS NOT NULL 
				 --		   THEN order_id2
				 --	  END
			END) AS atleast_once_order_id,
		MAX(CASE WHEN customer_called_during_live_order_flag=1 --AND order_id1 IS NOT NULL 
				 THEN order_id1 
				 --ELSE CASE WHEN customer_called_during_live_order_flag=1 AND order_id2 IS NOT NULL 
				 --		   THEN order_id2
				 --	  END
			END) AS live_order_id,
		MAX(CASE WHEN live_doctor_program_flag=1 --AND order_id1 IS NOT NULL 
				 THEN order_id1
				 --ELSE CASE WHEN customer_called_during_live_order_flag=1 AND order_id2 IS NOT NULL 
				 --		   THEN fdpo_order_id
				 --	  END
			END) AS dp_order_id,
		MAX(CASE WHEN customer_called_live_dp_flag=1 --AND is_doctor_program=true 
				 THEN order_id2
				 --ELSE CASE WHEN customer_called_during_live_order_flag=1 AND fdpo_order_id IS NOT NULL 
				 --		   THEN fdpo_order_id
				 --	  END
			END) AS live_dp_order_id
FROM (
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, o1.id AS order_id1, fdpo1.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN o1.id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN (ocd.call_started_at BETWEEN o1.time_stamp AND o1.updated_at) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN ((ocd.call_started_at BETWEEN o1.time_stamp AND o1.updated_at) AND fdpo2.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fdpo1.first_moved_to_dp_at AND fdpo1.last_in_dp_last_response_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
			--CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END AS call_during_live_order_flag
	FROM analytics.ozonetel_calls_dump ocd
	LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number 
	LEFT JOIN data_model.f_doctor_program_order fdpo1 ON o1.id=fdpo1.order_id
	--LEFT JOIN pe2.customer c ON ocd.caller_no=c.mobile_number
	--LEFT JOIN pe2."order" o2 ON c.id=o2.customer_id
	--LEFT JOIN data_model.f_doctor_program_order fdpo ON o2.id=fdpo.order_id
	--GROUP BY 1,2,3
	UNION
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, o2.id AS order_id1, fdpo2.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN o2.id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN (ocd.call_started_at BETWEEN o2.time_stamp AND o2.updated_at) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN ((ocd.call_started_at BETWEEN o2.time_stamp AND o2.updated_at) AND fdpo2.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fdpo2.first_moved_to_dp_at AND fdpo2.last_in_dp_last_response_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
			--CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END AS call_during_live_order_flag
	FROM analytics.ozonetel_calls_dump ocd
	--LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number 
	--LEFT JOIN data_model.f_order fo ON o1.id=fo.order_id
	LEFT JOIN pe2.customer c ON ocd.caller_no=c.mobile_number
	LEFT JOIN pe2."order" o2 ON c.id=o2.customer_id
	LEFT JOIN data_model.f_doctor_program_order fdpo2 ON o2.id=fdpo2.order_id
	)
GROUP BY 1,2,3
ORDER BY 3;


--- Final Query


SELECT call_id, caller_no, call_started_at,
		MAX(customer_ordered_atleast_once_flag) AS customer_ordered_atleast_once_flag,
		MAX(customer_called_during_live_order_flag) AS customer_ordered_atleast_once_flag,
		MAX(live_doctor_program_flag) AS live_doctor_program_flag,
		MAX(customer_called_live_dp_flag) AS customer_called_live_dp_flag,
		--MAX(CASE WHEN customer_ordered_atleast_once_flag=1 THEN order_id1 END) AS atleast_once_order_id,
		MAX(CASE WHEN customer_called_during_live_order_flag=1 THEN order_id1 END) AS live_order_id,
		MAX(CASE WHEN live_doctor_program_flag=1 THEN order_id1 END) AS dp_order_id,
		MAX(CASE WHEN customer_called_live_dp_flag=1 THEN order_id1	END) AS live_dp_order_id
FROM (
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, fdpo1.order_id AS order_id1, --fdpo1.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN fdpo1.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.delivered_at) OR 
						(ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.delivered_at) OR 
						(ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.cnr_timestamp)) AND fo1.is_doctor_program=true) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo1.cc_min_in_dp_at AND fo1.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number
	LEFT JOIN data_model.f_order fo1 ON o1.id=fo1.order_id 
	LEFT JOIN data_model.f_order_consumer fdpo1 ON fdpo1.order_id=fo1.order_id
	UNION ALL
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, o2.order_id AS order_id1, --fdpo2.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN o2.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN o2.order_placed_at AND o2.delivered_at) OR 
						(ocd.call_started_at BETWEEN o2.order_placed_at AND o2.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN o2.order_placed_at AND o2.delivered_at) OR 
						(ocd.call_started_at BETWEEN o2.order_placed_at AND o2.cnr_timestamp)) AND fdpo2.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fdpo2.cc_min_in_dp_at AND fdpo2.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN data_model.f_order_consumer o2 ON ocd.caller_no=o2.customer_registered_contact_number -- o2.id=fo2.order_id 
	LEFT JOIN data_model.f_order fdpo2 ON o2.order_id=fdpo2.order_id AND fdpo2.is_doctor_program=true
	)
GROUP BY 1,2,3
ORDER BY 3;



-----

SELECT call_id, caller_no, call_started_at, 
		customer_ordered_atleast_once_flag,
		customer_live_order_flag,
		live_doctor_program_flag,
		customer_called_live_dp_flag,
		live_order_id,
		dp_order_id,
		live_dp_order_id,
		case_status_before_call
		--MAX(CASE WHEN action_number=1 THEN 1 ELSE 0 END) AS dp_order_on_hold,
		--MAX(CASE WHEN action_number=1 AND "action"=5 THEN 1 ELSE 0 END) AS dp_order_on_hold
FROM (		
SELECT abc.*,
		dpoh.id AS dpoh_id, dpoh."action" AS case_status_before_call,
		CASE WHEN dpoh.order_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY dpoh.order_id IS NOT NULL, dpoh.order_id ORDER BY id DESC) END AS action_number,
		CASE WHEN dpoh.order_id IS NULL THEN 1 END AS include_flag
FROM (
SELECT call_id, caller_no, call_started_at,
		MAX(customer_ordered_atleast_once_flag) AS customer_ordered_atleast_once_flag,
		MAX(customer_called_during_live_order_flag) AS customer_live_order_flag,
		MAX(live_doctor_program_flag) AS live_doctor_program_flag,
		MAX(customer_called_live_dp_flag) AS customer_called_live_dp_flag,
		--MAX(CASE WHEN customer_ordered_atleast_once_flag=1 THEN order_id1 END) AS atleast_once_order_id,
		MAX(CASE WHEN customer_called_during_live_order_flag=1 THEN order_id1 END) AS live_order_id,
		MAX(CASE WHEN live_doctor_program_flag=1 THEN order_id1 END) AS dp_order_id,
		MAX(CASE WHEN customer_called_live_dp_flag=1 THEN order_id1	END) AS live_dp_order_id
FROM (
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, fdpo1.order_id AS order_id1, --fdpo1.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN fdpo1.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.delivered_at) OR 
						(ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.delivered_at) OR 
						(ocd.call_started_at BETWEEN fdpo1.order_placed_at AND fdpo1.cnr_timestamp)) AND fo1.is_doctor_program=true) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo1.cc_min_in_dp_at AND fo1.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number
	LEFT JOIN data_model.f_order fo1 ON o1.id=fo1.order_id 
	LEFT JOIN data_model.f_order_consumer fdpo1 ON fdpo1.order_id=fo1.order_id
	UNION ALL
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, o2.order_id AS order_id1, --fdpo2.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN o2.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN o2.order_placed_at AND o2.delivered_at) OR 
						(ocd.call_started_at BETWEEN o2.order_placed_at AND o2.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN o2.order_placed_at AND o2.delivered_at) OR 
						(ocd.call_started_at BETWEEN o2.order_placed_at AND o2.cnr_timestamp)) AND fdpo2.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fdpo2.cc_min_in_dp_at AND fdpo2.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN data_model.f_order_consumer o2 ON ocd.caller_no=o2.customer_registered_contact_number -- o2.id=fo2.order_id 
	LEFT JOIN data_model.f_order fdpo2 ON o2.order_id=fdpo2.order_id AND fdpo2.is_doctor_program=true
	)
GROUP BY 1,2,3
) abc
LEFT JOIN pe2.doctor_program_order_history dpoh ON abc.live_dp_order_id=dpoh.order_id AND dpoh.created_at<abc.call_started_at
)
WHERE action_number=1 OR include_flag=1
ORDER BY 3;




SELECT * FROM pe2.doctor_program_order_history WHERE order_id=8413281 ORDER BY id;


----- Final with Live On DP Hold

SELECT call_id, caller_no, call_started_at,
		MAX(customer_ordered_atleast_once_flag) AS customer_ordered_atleast_once_flag,
		MAX(customer_called_during_live_order_flag) AS customer_live_order_flag,
		MAX(live_doctor_program_flag) AS live_doctor_program_flag,
		MAX(customer_called_live_dp_flag) AS customer_called_live_dp_flag,
		--MAX(CASE WHEN customer_ordered_atleast_once_flag=1 THEN order_id1 END) AS atleast_once_order_id,
		MAX(CASE WHEN customer_called_during_live_order_flag=1 THEN order_id1 END) AS live_order_id,
		MAX(CASE WHEN live_doctor_program_flag=1 THEN order_id1 END) AS dp_order_id,
		MAX(CASE WHEN customer_called_live_dp_flag=1 THEN order_id1	END) AS live_dp_order_id,
		MAX(CASE WHEN action_number=1 THEN 1 ELSE 0 END) AS live_dp_order_flag,
		MAX(CASE WHEN action_number=1 AND case_status_before_call=5 THEN 1 ELSE 0 END) AS dp_order_on_hold
FROM (
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, foc1.order_id AS order_id1, --foc1.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN foc1.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.cnr_timestamp)) AND fo1.is_doctor_program=true) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo1.cc_min_in_dp_at AND fo1.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag,
			dpoh.id AS dpoh_id, dpoh."action" AS case_status_before_call,
			RANK() OVER (PARTITION BY dpoh.order_id ORDER BY dpoh.id DESC) AS action_number
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number
	LEFT JOIN data_model.f_order fo1 ON o1.id=fo1.order_id 
	LEFT JOIN data_model.f_order_consumer foc1 ON foc1.order_id=fo1.order_id
	LEFT JOIN pe2.doctor_program_order_history dpoh ON fo1.order_id=dpoh.order_id AND dpoh.created_at<ocd.call_started_at
	UNION ALL
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, foc2.order_id AS order_id1, --fo2.order_id AS order_id2, --foc2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN foc2.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.cnr_timestamp)) AND fo2.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo2.cc_min_in_dp_at AND fo2.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag,
			dpoh.id AS dpoh_id, dpoh."action" AS case_status_before_call,
			RANK() OVER (PARTITION BY dpoh.order_id ORDER BY dpoh.id DESC) AS action_number
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN data_model.f_order_consumer foc2 ON ocd.caller_no=foc2.customer_registered_contact_number -- foc2.id=fo2.order_id 
	LEFT JOIN data_model.f_order fo2 ON foc2.order_id=fo2.order_id AND fo2.is_doctor_program=true
	LEFT JOIN pe2.doctor_program_order_history dpoh ON fo2.order_id=dpoh.order_id AND dpoh.created_at<ocd.call_started_at
	)
GROUP BY 1,2,3
ORDER BY 3;



----- Live On Hold Impact


SELECT abc.*, 
		foc.order_status_id, foc.order_cancelled_at_stage, 
		fdpo.latest_case_status, fdpo.rx_first_prescribed_at,fdpo.rx_last_prescribed_at,
		CASE WHEN foc.order_status_id=8 AND foc.order_cancelled_at_stage=49 THEN 1 ELSE 0 END AS canned_in_docstat_flag,
		CASE WHEN abc.dp_order_on_hold=1 
			 THEN CASE WHEN abc.call_started_at>fdpo.rx_first_prescribed_at 
			 		   THEN DATEDIFF(MIN, abc.call_started_at, fdpo.rx_last_prescribed_at)
			 		   ELSE DATEDIFF(MIN, abc.call_started_at, fdpo.rx_last_prescribed_at)
			 		   END
		END AS minutes_wasted,
		CASE WHEN foc.order_status_id IN (2,8) THEN DATEDIFF(MIN, abc.call_started_at, foc.cnr_timestamp) END AS cnr_from_call,
		foc.cnr_timestamp AS cnr_at, foc.cnr_reason, fdpo.dp_rejection_reason, fdpo.customer_refuse_reason
FROM (
SELECT call_id, caller_no, call_started_at,
		MAX(customer_ordered_atleast_once_flag) AS customer_ordered_atleast_once_flag,
		MAX(customer_called_during_live_order_flag) AS customer_live_order_flag,
		MAX(live_doctor_program_flag) AS live_doctor_program_flag,
		MAX(customer_called_live_dp_flag) AS customer_called_live_dp_flag,
		--MAX(CASE WHEN customer_ordered_atleast_once_flag=1 THEN order_id1 END) AS atleast_once_order_id,
		MAX(CASE WHEN customer_called_during_live_order_flag=1 THEN order_id1 END) AS live_order_id,
		MAX(CASE WHEN live_doctor_program_flag=1 THEN order_id1 END) AS dp_order_id,
		MAX(CASE WHEN customer_called_live_dp_flag=1 THEN order_id1	END) AS live_dp_order_id,
		MAX(CASE WHEN action_number=1 THEN 1 ELSE 0 END) AS live_dp_order_flag,
		MAX(CASE WHEN action_number=1 AND case_status_before_call=5 THEN 1 ELSE 0 END) AS dp_order_on_hold
FROM (
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, foc1.order_id AS order_id1, --foc1.order_id AS order_id2, --o2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN foc1.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc1.order_placed_at AND foc1.cnr_timestamp)) AND fo1.is_doctor_program=true) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo1.cc_min_in_dp_at AND fo1.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag,
			dpoh.id AS dpoh_id, dpoh."action" AS case_status_before_call,
			RANK() OVER (PARTITION BY ocd.call_id,dpoh.order_id ORDER BY dpoh.id DESC) AS action_number
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN pe2."order" o1 ON ocd.caller_no=o1.contact_number
	LEFT JOIN data_model.f_order fo1 ON o1.id=fo1.order_id 
	LEFT JOIN data_model.f_order_consumer foc1 ON foc1.order_id=fo1.order_id
	LEFT JOIN pe2.doctor_program_order_history dpoh ON fo1.order_id=dpoh.order_id AND dpoh.created_at<ocd.call_started_at
	UNION ALL
	SELECT ocd.call_id, ocd.caller_no, ocd.call_started_at, foc2.order_id AS order_id1, --fo2.order_id AS order_id2, --foc2.id AS order_id2, fdpo.order_id AS fdpo_order_id, fo.is_doctor_program,
			(CASE WHEN foc2.order_id IS NOT NULL THEN 1 ELSE 0 END) AS customer_ordered_atleast_once_flag, 
			(CASE WHEN ((ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.cnr_timestamp)) THEN 1 ELSE 0 END) AS customer_called_during_live_order_flag,
			(CASE WHEN (((ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.delivered_at) OR 
						(ocd.call_started_at BETWEEN foc2.order_placed_at AND foc2.cnr_timestamp)) AND fo2.order_id IS NOT NULL) THEN 1 ELSE 0 END) AS live_doctor_program_flag,
			(CASE WHEN (ocd.call_started_at BETWEEN fo2.cc_min_in_dp_at AND fo2.cc_max_out_dp_at) THEN 1 ELSE 0 END) AS customer_called_live_dp_flag,
			dpoh.id AS dpoh_id, dpoh."action" AS case_status_before_call,
			RANK() OVER (PARTITION BY ocd.call_id,dpoh.order_id ORDER BY dpoh.id DESC) AS action_number
	FROM analytics.ozonetel_calls_dump_1 ocd
	LEFT JOIN data_model.f_order_consumer foc2 ON ocd.caller_no=foc2.customer_registered_contact_number -- foc2.id=fo2.order_id 
	LEFT JOIN data_model.f_order fo2 ON foc2.order_id=fo2.order_id AND fo2.is_doctor_program=true
	LEFT JOIN pe2.doctor_program_order_history dpoh ON fo2.order_id=dpoh.order_id AND dpoh.created_at<ocd.call_started_at
	)
GROUP BY 1,2,3
) abc
LEFT JOIN data_model.f_order_consumer foc ON abc.live_dp_order_id=foc.order_id
LEFT JOIN data_model.f_doctor_program_order fdpo ON abc.live_dp_order_id=fdpo.order_id --AND dp_order_on_hold=1
ORDER BY 3;
