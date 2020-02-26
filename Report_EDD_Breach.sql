SELECT o.order_id, DATEADD(MIN,330,order_placed_at) AS order_placed_at, DATE(estd_delivery_date) AS edd, c2.city_name AS supplier_city,
		    CASE o.status WHEN  1  THEN  'ORDER PLACED'
						WHEN  3  THEN  'SENT TO CHEMIST'
						WHEN  4  THEN  'HELP ME'
						WHEN  16  THEN  'ORDER IS ON HOLD'
						WHEN  17  THEN  'ORDER HAS BEEN MARKED VALID BY RETAILER'
						WHEN  46  THEN  'ORDER WAS PUT IN HQ DIGITIZATION'
						WHEN  47  THEN  'AWATING PRESCRIPTION FOR OFFLINE ORDER'
						WHEN  49  THEN  'ASSIGNED TO DOCTOR PROGRAM'
						WHEN  55  THEN  'VERIFY CUSTOM MEDICINES'
	   		END AS current_status,
			CASE o.status WHEN 	1	 THEN 	'OC'
						WHEN 	3	 THEN 	'OC'
						WHEN 	4	 THEN 	'CS'		
						WHEN 	16	 THEN 	'OC'
						WHEN 	17	 THEN 	'OC'
						WHEN 	46	 THEN 	'OC'
						WHEN 	47	 THEN 	'OC'					
						WHEN 	49	 THEN 	'DP'
						WHEN 	55	 THEN 	'Custom Med Verification'
			END AS function_responsible,
			CASE WHEN o_corporate.order_id IS NOT NULL THEN 'corporate'
				 WHEN soi.subscription_id IS NOT NULL OR ranking > 1 THEN 'refill' 
				 WHEN o_courier.order_id IS NOT NULL THEN 'courier'
			END AS order_type,
			DATEDIFF(MINUTE, last_status_updated_at, DATEADD(MIN,-10,GETDATE())) AS minutes_since_last_updated,
			DATEDIFF(HOUR, last_status_updated_at, DATEADD(MIN,-10,GETDATE())) AS hours_since_last_updated
	FROM public."order" o
	LEFT JOIN public.city c1 ON o.city_id=c1.city_id
	LEFT JOIN public.city c2 ON c1.supplier_city_id=c2.city_id
	LEFT JOIN public.doctor_program_order dpo ON o.order_id=dpo.order_id AND dpo."source"=6 AND DATEADD(MIN,330,created_at) BETWEEN ((DATE(DATEADD(MIN,330,GETDATE()))||' 10:00:00')::TIMESTAMP) AND ((DATE(DATEADD(MIN,330,GETDATE()))||' 12:00:00')::TIMESTAMP)
	--INNER JOIN public.customer_address ca ON o.address_id=ca.address_id AND ca.pincode!=444444
	LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=45 GROUP BY 1) o_corporate ON o.order_id=o_corporate.order_id
	LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=19 GROUP BY 1) o_courier ON o.order_id=o_courier.order_id
    LEFT JOIN (SELECT subscription_id, order_id, RANK() OVER (PARTITION BY subscription_id ORDER BY order_id) AS ranking
                 FROM order_subscription_info) osi ON o.order_id = osi.order_id	
    LEFT JOIN subscription_origin_info soi ON osi.subscription_id=soi.subscription_id
    LEFT JOIN (SELECT order_id, MAX(created_at) AS last_status_updated_at FROM public.order_history GROUP BY 1) oh ON o.order_id=oh.order_id
	--WHERE (o.retailer_id NOT IN (12,80,82,115,85,69,63) OR o.retailer_id IS NULL)
	--		AND DATE(o.estd_delivery_date)=CURRENT_DATE	
	--		AND o.status IN (1,3,4,16,17,46,47,49,55)
			--AND osi.order_id IS NULL
	WHERE DATEADD(MIN,330,order_placed_at) BETWEEN ((DATE(DATEADD(MIN,-1110,GETDATE()))||' 12:00:00')::TIMESTAMP) AND ((DATE(DATEADD(MIN,330,GETDATE()))||' 12:00:00')::TIMESTAMP) 
			AND dpo.order_id IS NULL AND o.status IN (1,3,4,16,17,46,47,49) AND (o.retailer_id NOT IN (12,80,82,115,85,69,63) OR o.retailer_id IS NULL)	
    GROUP BY 1,2,3,4,5,6,7,8,9
	ORDER BY 8 DESC
