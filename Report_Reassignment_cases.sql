-- First Draft
WITH daily_reassigned_cases AS (
SELECT order_id, function_responsible, status_before_valid, status_before_valid_at, instance_in_valid_at, reassigned_instance
FROM (
	SELECT *, CASE WHEN (order_status=17 AND (new_num!=1 OR new_num IS NULL) AND (dp_num!=1 OR dp_num IS NULL) AND (valid_num!=1 OR valid_num IS NULL)) 
				   THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=17,((new_num!=1 OR new_num IS NULL) AND (dp_num!=1 OR dp_num IS NULL) AND (valid_num!=1 OR valid_num IS NULL)) ORDER BY id) END AS reassigned_instance
	FROM (
		SELECT id,order_id, order_status, DATEADD(MIN,330,status_at) AS instance_in_valid_at, s.status_name AS status_before_valid, previous_status,	--row_num AS times_in_valid, 
			   DATEADD(MIN,330,previous_status_at) AS status_before_valid_at,
			   CASE previous_status WHEN 	1	 THEN 	'OC'
									WHEN 	2	 THEN 	'N.A.'
									WHEN 	3	 THEN 	'OC'
									WHEN 	4	 THEN 	'CS'
									WHEN 	5	 THEN 	'WMS'
									WHEN 	6	 THEN 	'DP'
									WHEN 	7	 THEN 	'Logistics'
									WHEN 	8	 THEN 	'N.A.'
									WHEN 	9	 THEN 	'N.A.'
									WHEN 	10	 THEN 	'N.A.'
									WHEN 	11	 THEN 	'N.A.'
									WHEN 	12	 THEN 	'Logistics'
									WHEN 	13	 THEN 	'Logistics'
									WHEN 	14	 THEN 	'WMS'
									WHEN 	15	 THEN 	'Logistics'
									WHEN 	16	 THEN 	'OC'
									WHEN 	17	 THEN 	'OC'
									WHEN 	18	 THEN 	'DP'
									WHEN 	19	 THEN 	'Logistics'
									WHEN 	20	 THEN 	'WMS'
									WHEN 	21	 THEN 	'WMS'
									WHEN 	22	 THEN 	'WMS'
									WHEN 	23	 THEN 	'WMS'
									WHEN 	24	 THEN 	'Logistics'
									WHEN 	25	 THEN 	'Logistics'
									WHEN 	26	 THEN 	'WMS'
									WHEN 	27	 THEN 	'N.A.'
									WHEN 	28	 THEN 	'N.A.'
									WHEN 	29	 THEN 	'N.A.'
									WHEN 	30	 THEN 	'Logistics'
									WHEN 	31	 THEN 	'Logistics'
									WHEN 	32	 THEN 	'Logistics'
									WHEN 	33	 THEN 	'Logistics'
									WHEN 	34	 THEN 	'Logistics'
									WHEN 	35	 THEN 	'Logistics'
									WHEN 	36	 THEN 	'Logistics'
									WHEN 	37	 THEN 	'Logistics'
									WHEN 	38	 THEN 	'Logistics'
									WHEN 	39	 THEN 	'Logistics'
									WHEN 	40	 THEN 	'Logistics'
									WHEN 	41	 THEN 	'Logistics'
									WHEN 	42	 THEN 	'Logistics'
									WHEN 	43	 THEN 	'WMS'
									WHEN 	44	 THEN 	'WMS'
									WHEN 	45	 THEN 	'Logistics'
									WHEN 	46	 THEN 	'OC'
									WHEN 	47	 THEN 	'OC'
									WHEN 	48	 THEN 	'WMS'
									WHEN 	49	 THEN 	'DP'
									WHEN 	50	 THEN 	'WMS'
									WHEN 	51	 THEN 	'WMS'
									WHEN 	52	 THEN 	'WMS'
									WHEN 	53	 THEN 	'WMS'
									WHEN 	54	 THEN 	'Logistics'
									WHEN 	55	 THEN 	'Custom Med Verification'
									WHEN 	56	 THEN 	'WMS'
									WHEN 	57	 THEN 	'WMS'
									WHEN 	58	 THEN 	'WMS'
				END AS function_responsible, CASE WHEN previous_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,previous_status=49 ORDER BY id) END AS dp_num,
				CASE WHEN previous_status=3 THEN ROW_NUMBER() OVER (PARTITION BY order_id,previous_status=3 ORDER BY id) END AS new_num,
				CASE WHEN order_status=17 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=17 ORDER BY id) END AS valid_num
			FROM (
				SELECT id,order_id, order_status, created_at AS status_at, 
						LAG(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS previous_status, 
						LAG(created_at,1) OVER (PARTITION BY order_id ORDER BY id) AS previous_status_at
				FROM public.order_history
				--WHERE order_id=4284225
			) abc
			LEFT JOIN prod.status s ON abc.previous_status=s.status_id))
--WHERE valid_num>1 AND DATE(instance_in_valid_at)=(CURRENT_DATE-1) AND order_status=17
--GROUP BY 1,2,3,4,5,6,7
--ORDER BY 2 DESC
--ORDER BY id
WHERE reassigned_instance>0  AND function_responsible IS NOT NULL --AND DATE(instance_in_valid_at)=(CURRENT_DATE-1)
)
SELECT * FROM daily_reassigned_cases
WHERE order_id IN (SELECT order_id FROM daily_reassigned_cases WHERE DATE(instance_in_valid_at)=(CURRENT_DATE-1)) --AND status_before_valid='Retailer Accept'
ORDER BY 6 DESC
;

												 
												 

												 
------ Final query
												 

WITH status_description AS (
		SELECT *, CASE status_id WHEN 	1	 THEN 	'OC'
									WHEN 	2	 THEN 	'N.A.'
									WHEN 	3	 THEN 	'OC'
									WHEN 	4	 THEN 	'CS'
									WHEN 	5	 THEN 	'WMS'
									WHEN 	6	 THEN 	'DP'
									WHEN 	7	 THEN 	'Logistics'
									WHEN 	8	 THEN 	'N.A.'
									WHEN 	9	 THEN 	'N.A.'
									WHEN 	10	 THEN 	'N.A.'
									WHEN 	11	 THEN 	'N.A.'
									WHEN 	12	 THEN 	'Logistics'
									WHEN 	13	 THEN 	'Logistics'
									WHEN 	14	 THEN 	'WMS'
									WHEN 	15	 THEN 	'Logistics'
									WHEN 	16	 THEN 	'OC'
									WHEN 	17	 THEN 	'OC'
									WHEN 	18	 THEN 	'DP'
									WHEN 	19	 THEN 	'Logistics'
									WHEN 	20	 THEN 	'WMS'
									WHEN 	21	 THEN 	'WMS'
									WHEN 	22	 THEN 	'WMS'
									WHEN 	23	 THEN 	'WMS'
									WHEN 	24	 THEN 	'Logistics'
									WHEN 	25	 THEN 	'Logistics'
									WHEN 	26	 THEN 	'WMS'
									WHEN 	27	 THEN 	'N.A.'
									WHEN 	28	 THEN 	'N.A.'
									WHEN 	29	 THEN 	'N.A.'
									WHEN 	30	 THEN 	'Logistics'
									WHEN 	31	 THEN 	'Logistics'
									WHEN 	32	 THEN 	'Logistics'
									WHEN 	33	 THEN 	'Logistics'
									WHEN 	34	 THEN 	'Logistics'
									WHEN 	35	 THEN 	'Logistics'
									WHEN 	36	 THEN 	'Logistics'
									WHEN 	37	 THEN 	'Logistics'
									WHEN 	38	 THEN 	'Logistics'
									WHEN 	39	 THEN 	'Logistics'
									WHEN 	40	 THEN 	'Logistics'
									WHEN 	41	 THEN 	'Logistics'
									WHEN 	42	 THEN 	'Logistics'
									WHEN 	43	 THEN 	'WMS'
									WHEN 	44	 THEN 	'WMS'
									WHEN 	45	 THEN 	'Logistics'
									WHEN 	46	 THEN 	'OC'
									WHEN 	47	 THEN 	'OC'
									WHEN 	48	 THEN 	'WMS'
									WHEN 	49	 THEN 	'DP'
									WHEN 	50	 THEN 	'WMS'
									WHEN 	51	 THEN 	'WMS'
									WHEN 	52	 THEN 	'WMS'
									WHEN 	53	 THEN 	'WMS'
									WHEN 	54	 THEN 	'Logistics'
									WHEN 	55	 THEN 	'Custom Med Verification'
									WHEN 	56	 THEN 	'WMS'
									WHEN 	57	 THEN 	'WMS'
									WHEN 	58	 THEN 	'WMS'
				END AS function_responsible 
		FROM prod.status
),
daily_reassigned_cases AS (
	SELECT order_id, function_responsible, status_before_valid, status_before_valid_at, instance_in_valid_at, reassigned_by, reassigned_instance
	FROM (
		SELECT *, CASE WHEN (order_status=17 AND (dp_num!=1 OR dp_num IS NULL) AND (valid_num!=1)) 
				   THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=17,((dp_num!=1 OR dp_num IS NULL) AND (valid_num!=1)) ORDER BY id) END AS reassigned_instance
		FROM (
			SELECT id,order_id, order_status, DATEADD(MIN,330,status_at) AS instance_in_valid_at, sd.status_name AS status_before_valid, 
			   		previous_status, DATEADD(MIN,330,previous_status_at) AS status_before_valid_at, valid_num, 
			   		CASE WHEN previous_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,previous_status=49 ORDER BY id) END AS dp_num,
			   		sd.function_responsible, abc.reassigned_by
			FROM (
				SELECT oh.id, oh.order_id, oh.order_status, oh.created_at AS status_at, u.username AS reassigned_by,
						LAG(oh.order_status,1) OVER (PARTITION BY oh.order_id ORDER BY oh.id) AS previous_status, 
						LAG(oh.created_at,1) OVER (PARTITION BY oh.order_id ORDER BY oh.id) AS previous_status_at,
						CASE WHEN oh.order_status=17 THEN ROW_NUMBER() OVER (PARTITION BY oh.order_id, oh.order_status=17 ORDER BY oh.id) END AS valid_num
				FROM (SELECT id,order_id, order_status, created_at, updated_by 
						FROM public.order_history WHERE DATE(DATEADD(MIN,330,created_at))<CURRENT_DATE GROUP BY 1,2,3,4,5) oh
				LEFT JOIN public."user" u ON u.id=oh.updated_by
				WHERE order_id IN 
						(SELECT order_id FROM public.order_history WHERE order_status=17 AND DATE(DATEADD(MIN,330,created_at))=(CURRENT_DATE-1))
				) abc
			LEFT JOIN status_description sd ON abc.previous_status=sd.status_id
			WHERE order_status=17
			)
		)
	WHERE reassigned_instance>0  
)
SELECT function_responsible, status_before_valid, 
		COUNT(DISTINCT CASE WHEN DATE(instance_in_valid_at)=(CURRENT_DATE-1) THEN order_id END) AS same_day_reassigned_orders,
		COUNT(CASE WHEN DATE(instance_in_valid_at)=(CURRENT_DATE-1) THEN order_id END) AS same_day_reassigned_instances,
		COUNT(order_id) AS total_reassigned_instances
FROM (
	SELECT * FROM daily_reassigned_cases
	GROUP BY 1,2,3,4,5,6,7
	)
GROUP BY 1,2
ORDER BY 5 DESC
;
