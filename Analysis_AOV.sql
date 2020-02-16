SELECT ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_placed_time) AS cust_ord_num,
		ROW_NUMBER() OVER (PARTITION BY order_delivery_number ORDER BY order_placed_time) AS cont_num_ord_num, *
FROM ( 
SELECT o.order_id, o.customer_id, o.contact_number AS order_delivery_number,
		DATEADD(MIN,330,o.order_placed_at) AS order_placed_time, o.order_value, c.city_name AS delivery_city, c1.city_name AS supplier_city,  
		CASE WHEN c.does_carrier_delivery THEN 1 ELSE 0 END AS courier_flag,
		o.platform, CASE WHEN o.platform IN ('cms','CMS') THEN 'CMS'
						 WHEN o.platform IN ('android','ios') THEN 'APP'
						 WHEN o.platform IN ('mweb','web') THEN 'WEBSITE'
						 WHEN o.platform IN ('order-on-call') THEN 'OOC'
						 WHEN o.platform IN ('api') THEN 'ThirdPartyAPI'
					END AS o_source,
		CASE WHEN atc.order_id IS NOT NULL THEN 1 ELSE 0 END AS atc_flag,
		CASE WHEN o_chronic.order_id IS NOT NULL THEN 1 ELSE 0 END AS o_chronic_flag,
		CASE WHEN c_chronic.customer_id IS NOT NULL THEN 1 ELSE 0 END AS c_chronic_flag,
		CASE WHEN atc_rx_notreq.order_id IS NOT NULL THEN 1 ELSE 0 END AS atc_rx_notreq_flag,
		CASE WHEN (cc.order_id IS NOT NULL) AND (atc.order_id IS NULL) AND (refill.order_id IS NULL) AND (o.platform IN ('android','ios','mweb','web')) THEN 1 ELSE 0 END AS APPorWEB_uploadRx_flag,
		CASE WHEN dp.order_id IS NOT NULL THEN 1 ELSE 0 END AS dp_flag,
		CASE WHEN ooc.order_id IS NOT NULL THEN 1 ELSE 0 END AS ooc_flag,
		CASE WHEN offline.order_id IS NOT NULL THEN 1 ELSE 0 END AS offline_flag,
		CASE WHEN refill.order_id IS NOT NULL THEN 1 ELSE 0 END AS refill_flag,
		CASE WHEN osi.order_id IS NOT NULL THEN 1 ELSE 0 END AS subscription_flag,
		CASE WHEN ftc.order_id IS NOT NULL THEN 1 ELSE 0 END AS ftc_flag,
		CASE WHEN fpc.order_id IS NOT NULL THEN 1 ELSE 0 END AS fpc_flag
FROM public."order" o
LEFT JOIN public.city c ON o.city_id=c.city_id
LEFT JOIN public.city c1 ON c.supplier_city_id=c1.city_id
LEFT JOIN (
	SELECT a.order_id
	FROM
	   (
		SELECT orf.order_id, CASE odf.flag_id WHEN 34 THEN 0 ELSE 1 END AS Rx_req_flag 
		FROM public.order_flags orf
		LEFT JOIN public.medicine_notes mn ON orf.order_id=mn.order_id
		LEFT JOIN public.order_digitization_flag odf ON (mn.id=odf.medicine_notes_id AND odf.flag_id=34)
		WHERE (orf.flag_id=23 OR orf.flag_id=22)
	) a
	GROUP BY 1
	HAVING SUM(a.Rx_req_flag)=0
	) atc_rx_notreq ON o.order_id=atc_rx_notreq.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id IN (22,23) GROUP BY 1) atc ON o.order_id=atc.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id IN (14,15,16,18) GROUP BY 1) offline ON o.order_id=offline.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=43 GROUP BY 1) ooc ON o.order_id=ooc.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=17 GROUP BY 1) dp ON o.order_id=dp.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=1 GROUP BY 1) ftc ON o.order_id=ftc.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=11 GROUP BY 1) fpc ON o.order_id=fpc.order_id
LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id=12 GROUP BY 1) o_chronic ON o.order_id=o_chronic.order_id
LEFT JOIN (SELECT customer_id FROM public.customer_flags WHERE flag_id=13 GROUP BY 1) c_chronic ON o.customer_id=c_chronic.customer_id
LEFT JOIN (SELECT osi.order_id FROM public.order_subscription_info osi 
			INNER JOIN public.subscription s ON (osi.subscription_id=s.id AND osi.order_id!=s.template_order_id) GROUP BY 1) refill ON o.order_id=refill.order_id
LEFT JOIN (SELECT order_id FROM public.order_subscription_info GROUP BY 1) osi ON o.order_id=osi.order_id
LEFT JOIN (SELECT qt.entity_id AS order_id FROM public.q_task qt WHERE qt."type" IN (1,2) GROUP BY 1) cc ON o.order_id=cc.order_id
WHERE (DATE(DATEADD(MIN,330,o.order_placed_at)) BETWEEN '2018-01-01' AND '2018-05-31') AND o.status IN (9,10)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
ORDER BY 6
