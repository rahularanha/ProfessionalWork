WITH oh AS (
	SELECT order_id, status_bucket, (SUM(DATEDIFF(SECOND,status_at,next_status_at))::FLOAT)/60 AS time_spent_in_function
	FROM (
		SELECT order_id, order_status, "timestamp" AS status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS next_status_at,
				CASE order_status 
					WHEN 	1	 THEN 	'1) OC'
					WHEN 	2	 THEN 	'8) Final'
					WHEN 	3	 THEN 	'1) OC'
					WHEN 	4	 THEN 	'3) CS'
					WHEN 	5	 THEN 	'4) WMS'
					WHEN 	7	 THEN 	'7) Hub-to-Customer Logistics'
					WHEN 	8	 THEN 	'8) Final'
					WHEN 	9	 THEN 	'8) Final'
					WHEN 	10	 THEN 	'8) Final'
					WHEN 	12	 THEN 	'7) Hub-to-Customer Logistics'
					WHEN 	14	 THEN 	'6) Hub'
					WHEN 	15	 THEN 	'6) Hub'
					WHEN 	16	 THEN 	'1) OC'
					WHEN 	17	 THEN 	'1) OC'
					WHEN 	19	 THEN 	'7) Hub-to-Customer Logistics'
					WHEN 	20	 THEN 	'4) WMS'
					WHEN 	21	 THEN 	'4) WMS'
					WHEN 	47	 THEN 	'1) OC'
					WHEN 	49	 THEN 	'2) Docstat'
					WHEN 	50	 THEN 	'4) WMS'
					WHEN 	51	 THEN 	'4) WMS'
					WHEN 	52	 THEN 	'6) Hub'
					WHEN 	53	 THEN 	'5) WMS-to-Hub Logistics'
					WHEN 	54	 THEN 	'4) WMS'
					WHEN 	55	 THEN 	'8) VCM'
					WHEN 	56	 THEN 	'4) WMS'
					WHEN 	57	 THEN 	'4) WMS'
					WHEN 	58	 THEN 	'4) WMS'
					WHEN 	59	 THEN 	'4) WMS'
					WHEN 	60	 THEN 	'4) WMS'
					ELSE '10) Unexpected'
				END AS status_bucket
		FROM pe2.order_history
		WHERE "timestamp">='2018-11-01' --AND order_id=6949993
		ORDER BY order_id,id
	)
	GROUP BY 1,2
	ORDER BY 1
)
SELECT DATE(order_placed_at) AS order_placed_date, delivery_date, DATE(original_order_edd) AS original_order_edd, DATE(delivered_at) AS actual_delivery_date, delivery_type, order_status, cnr_reason, delivery_status, function_responsible_before_cnr, 
		order_source, supplier_city_name,--status_bucket,
		(CURRENT_DATE-60) AS lower_limit, (CURRENT_DATE-1) AS upper_limit,
		SUM(time_spent_in_oc) AS total_time_spent_in_oc,
		SUM(time_spent_in_dp) AS total_time_spent_in_dp,
		SUM(time_spent_in_cs) AS total_time_spent_in_cs,
		SUM(time_spent_in_wms) AS total_time_spent_in_wms,
		SUM(time_spent_in_log1) AS total_time_spent_in_log1,
		SUM(time_spent_in_hub) AS total_time_spent_in_hub,
		SUM(time_spent_in_log2) AS total_time_spent_in_log2,
		SUM(time_spent_in_final) AS total_time_spent_in_final,
		SUM(time_spent_in_vcm) AS total_time_spent_in_vcm,
		SUM(time_spent_in_rand) AS total_time_spent_in_rand,
		COUNT(time_spent_in_oc) AS total_in_oc,
		COUNT(time_spent_in_dp) AS total_in_dp,
		COUNT(time_spent_in_cs) AS total_in_cs,
		COUNT(time_spent_in_wms) AS total_in_wms,
		COUNT(time_spent_in_log1) AS total_in_log1,
		COUNT(time_spent_in_hub) AS total_in_hub,
		COUNT(time_spent_in_log2) AS total_in_log2,
		COUNT(time_spent_in_final) AS total_in_final,
		COUNT(time_spent_in_vcm) AS total_in_vcm,
		COUNT(time_spent_in_rand) AS total_in_rand,
		SUM(total_line_items) AS total_line_items_ordered,
		SUM(total_line_items_delivered) AS total_line_items_delivered,
		SUM(order_value) AS GMV, COUNT(order_id) AS order_count--,SUM(time_spent_in_function) AS time_spent_in_function, 
FROM(
SELECT fo.order_id, foc.order_status_id, foc.order_source, fo.supplier_city_name, fo.delivery_type, foc.total_line_items, foc.total_line_items_delivered,
		CASE  WHEN foc.order_status_id IN (2, 8) THEN '2) CnR' 
        	WHEN foc.order_status_id IN (9, 10) THEN '1) Delivered'
        	ElSE '3) UnderProcess'
		END AS order_status,
		foc.mrp AS order_value, fo.original_order_edd, fo.delivered_at, fo.order_placed_at,
		foc.cnr_timestamp AS cnr_at, 
		foc.cnr_reason_category, foc.cnr_reason,
		CASE WHEN DATE(fo.original_order_edd)=DATE(fo.delivered_at) THEN '2) On Committed'
			 WHEN DATE(fo.original_order_edd)>DATE(fo.delivered_at) THEN '1) Before Committed'
			 WHEN DATE(fo.original_order_edd)<DATE(fo.delivered_at) THEN '3) After Committed'
			 ELSE '4) NA'
		END AS delivery_status,
		DATE(CASE WHEN fo.delivered_at IS NOT NULL THEN fo.delivered_at
			 	  ELSE fo.original_order_edd
			 END) AS delivery_date,
		 foc.order_cancelled_at_stage,
		 CASE order_cancelled_at_stage 
			WHEN 	1	 THEN 	'1) OC'
			WHEN 	2	 THEN 	'8) Final'
			WHEN 	3	 THEN 	'1) OC'
			WHEN 	4	 THEN 	'3) CS'
			WHEN 	5	 THEN 	'4) WMS'
			WHEN 	7	 THEN 	'7) Hub-to-Customer Logistics'
			WHEN 	8	 THEN 	'8) Final'
			WHEN 	9	 THEN 	'8) Final'
			WHEN 	10	 THEN 	'8) Final'
			WHEN 	12	 THEN 	'7) Hub-to-Customer Logistics'
			WHEN 	14	 THEN 	'6) Hub'
			WHEN 	15	 THEN 	'6) Hub'
			WHEN 	16	 THEN 	'1) OC'
			WHEN 	17	 THEN 	'1) OC'
			WHEN 	19	 THEN 	'7) Hub-to-Customer Logistics'
			WHEN 	20	 THEN 	'4) WMS'
			WHEN 	21	 THEN 	'4) WMS'
			WHEN 	47	 THEN 	'1) OC'
			WHEN 	49	 THEN 	'2) Docstat'
			WHEN 	50	 THEN 	'4) WMS'
			WHEN 	51	 THEN 	'4) WMS'
			WHEN 	52	 THEN 	'6) Hub'
			WHEN 	53	 THEN 	'5) WMS-to-Hub Logistics'
			WHEN 	54	 THEN 	'4) WMS'
			WHEN 	55	 THEN 	'8) VCM'
			WHEN 	56	 THEN 	'4) WMS'
			WHEN 	57	 THEN 	'4) WMS'
			WHEN 	58	 THEN 	'4) WMS'
			WHEN 	59	 THEN 	'4) WMS'
			WHEN 	60	 THEN 	'4) WMS'
			ELSE NULL
		END AS function_responsible_before_cnr,
		oh.time_spent_in_oc,
		oh.time_spent_in_dp,
		oh.time_spent_in_cs,
		oh.time_spent_in_wms,
		oh.time_spent_in_log1,
		oh.time_spent_in_hub,
		oh.time_spent_in_log2,
		oh.time_spent_in_final,
		oh.time_spent_in_vcm,
		oh.time_spent_in_rand	
		--status_bucket, time_spent_in_function
FROM data_model.f_order fo
LEFT JOIN data_model.f_order_consumer foc ON fo.order_id=foc.order_id
LEFT JOIN ( SELECT order_id,
					SUM(CASE WHEN status_bucket='1) OC' THEN time_spent_in_function END) AS time_spent_in_oc,
					SUM(CASE WHEN status_bucket='2) Docstat' THEN time_spent_in_function END) AS time_spent_in_dp,
					SUM(CASE WHEN status_bucket='3) CS' THEN time_spent_in_function END) AS time_spent_in_cs,
					SUM(CASE WHEN status_bucket='4) WMS' THEN time_spent_in_function END) AS time_spent_in_wms,
					SUM(CASE WHEN status_bucket='5) WMS-to-Hub Logistics' THEN time_spent_in_function END) AS time_spent_in_log1,
					SUM(CASE WHEN status_bucket='6) Hub' THEN time_spent_in_function END) AS time_spent_in_hub,
					SUM(CASE WHEN status_bucket='7) Hub-to-Customer Logistics' THEN time_spent_in_function END) AS time_spent_in_log2,
					SUM(CASE WHEN status_bucket='8) Final' THEN time_spent_in_function END) AS time_spent_in_final,
					SUM(CASE WHEN status_bucket='9) VCM' THEN time_spent_in_function END) AS time_spent_in_vcm,
					SUM(CASE WHEN status_bucket='10) Unexpected' THEN time_spent_in_function END) AS time_spent_in_rand					
			FROM oh 
			GROUP BY 1
			) oh ON fo.order_id=oh.order_id
WHERE (DATE(fo.original_order_edd) BETWEEN (CURRENT_DATE-60) AND (CURRENT_DATE-1)) OR 
		(DATE(fo.delivered_at) BETWEEN (CURRENT_DATE-60) AND (CURRENT_DATE-1))
)
--WHERE delivery_date BETWEEN (CURRENT_DATE-60) AND (CURRENT_DATE-1)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 1
