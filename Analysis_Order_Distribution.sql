SELECT EXTRACT(MONTH FROM order_placed_time) AS Month_number, 
		COUNT(CASE WHEN queue IS NULL AND current_status IN ('Fulfilled') THEN order_id END) AS blankqueue_fulfilled,
		COUNT(CASE WHEN queue IS NULL AND current_status IN ('Rejected') THEN order_id END) AS blankqueue_rejected,
		COUNT(CASE WHEN queue IN ('refill') AND current_status IN ('Fulfilled') THEN order_id END) AS refill_fulfilled,
		COUNT(CASE WHEN queue IN ('refill') AND current_status IN ('Rejected') THEN order_id END) AS refill_rejected,
		COUNT(CASE WHEN queue IN ('CC_nonATC') AND called_status IN ('Called') AND current_status IN ('Fulfilled') THEN order_id END) AS CC_nonATC_Called_fulfilled,
		COUNT(CASE WHEN queue IN ('CC_nonATC') AND called_status IN ('Called') AND current_status IN ('Rejected') THEN order_id END) AS CC_nonATC_Called_rejected,
		COUNT(CASE WHEN queue IN ('CC_nonATC') AND called_status IN ('Not_Called') AND current_status IN ('Fulfilled') THEN order_id END) AS CC_nonATC_NotCalled_fulfilled,
		COUNT(CASE WHEN queue IN ('CC_nonATC') AND called_status IN ('Not_Called') AND current_status IN ('Rejected') THEN order_id END) AS CC_nonATC_NotCalled_rejected,
		COUNT(CASE WHEN queue IN ('CC_ATC') AND called_status IN ('Called') AND current_status IN ('Fulfilled') THEN order_id END) AS CC_ATC_Called_fulfilled,
		COUNT(CASE WHEN queue IN ('CC_ATC') AND called_status IN ('Called') AND current_status IN ('Rejected') THEN order_id END) AS CC_ATC_Called_rejected,
		COUNT(CASE WHEN queue IN ('CC_ATC') AND called_status IN ('Not_Called') AND current_status IN ('Fulfilled') THEN order_id END) AS CC_ATC_NotCalled_fulfilled,
		COUNT(CASE WHEN queue IN ('CC_ATC') AND called_status IN ('Not_Called') AND current_status IN ('Rejected') THEN order_id END) AS CC_ATC_NotCalled_rejected,		
		COUNT(CASE WHEN queue IN ('ATC_RxNotReq') AND current_status IN ('Fulfilled') THEN order_id END) AS non_Rx_fulf,
		COUNT(CASE WHEN queue IN ('ATC_RxNotReq') AND current_status IN ('Rejected') THEN order_id END) AS non_Rx_rejected
FROM (
	SELECT CASE WHEN soi.subscription_id IS NOT NULL OR ranking > 1 THEN 'refill'		
				WHEN cc.order_id IS NOT NULL THEN CASE WHEN atc.order_id IS NOT NULL THEN 'CC_ATC' ELSE 'CC_nonATC' END
				WHEN atc_rx_not.order_id IS NOT NULL THEN 'ATC_RxNotReq'
			END AS queue, o.order_id, DATEADD(MIN,330,o.order_placed_at) AS order_placed_time, 
			CASE WHEN o.status IN (9,10) THEN 'Fulfilled'
				 WHEN o.status IN (2,8) THEN 'Rejected'
				 ELSE 'Under_Process'
			END AS current_status,
			CASE WHEN call_dump.order_id_calls IS NOT NULL THEN 'Called'
				 ELSE 'Not_Called'
			END AS called_status,
			CASE WHEN new_customer.customer_id IS NULL 
				 THEN 1 
				 ELSE CASE WHEN EXTRACT(MONTH FROM new_customer.first_fulfilled_order_time)=EXTRACT(MONTH FROM DATEADD(MIN,330,o.order_placed_at))
				 		   THEN 1
				 		   ELSE 0
				 	  END
			END AS new_customer_flag
	FROM public."order" o 
	LEFT JOIN ( SELECT order_id
				FROM
				(
					SELECT orf.order_id, CASE odf.flag_id WHEN 34 THEN 0 ELSE 1 END AS Rx_req_flag	 
					FROM public.order_flags orf 
					LEFT JOIN public.medicine_notes mn ON orf.order_id=mn.order_id
					LEFT JOIN public.order_digitization_flag odf ON (mn.id=odf.medicine_notes_id AND odf.flag_id=34)
					WHERE orf.flag_id IN (22,23)
				) a
				GROUP BY 1
				HAVING SUM(a.Rx_req_flag)=0
			) atc_rx_not ON o.order_id=atc_rx_not.order_id
	LEFT JOIN (SELECT order_id FROM public.order_flags WHERE flag_id IN (22,23) GROUP BY 1) atc ON o.order_id=atc.order_id
	LEFT JOIN (SELECT qt.entity_id AS order_id FROM public.q_task qt WHERE qt."type" IN (1,2) GROUP BY 1) cc ON o.order_id=cc.order_id
	LEFT JOIN (SELECT subscription_id, order_id, RANK() OVER (PARTITION BY subscription_id ORDER BY order_id) AS ranking
                 FROM order_subscription_info) osi ON o.order_id = osi.order_id	
    LEFT JOIN subscription_origin_info soi ON osi.subscription_id=soi.subscription_id
    LEFT JOIN (SELECT customer_id, MIN(DATEADD(MIN,330,order_placed_at)) AS first_fulfilled_order_time 
			FROM public."order" WHERE status IN (9,10) GROUP BY 1) new_customer ON o.customer_id=new_customer.customer_id
	LEFT JOIN ( SELECT x.order_id
				FROM (
					SELECT order_id, CASE WHEN updated_by=218 THEN 0 ELSE 1 END AS auto_flag
					FROM public.order_history WHERE order_status=5
				) x
				GROUP BY 1
				HAVING SUM(x.auto_flag)=0
			) auto_accept ON o.order_id=auto_accept.order_id
	LEFT JOIN 
			( SELECT DISTINCT(CASE WHEN (POSITION('|' IN uui)) THEN SUBSTRING(uui, 1, (POSITION('|' IN uui)-1)) ELSE NULL END)::INT AS order_id_calls
			  FROM public.ng_ozonetel_calls_dump
			  WHERE (call_type='Manual') AND (campaign='Order_Confirmation') AND LEFT(uui, 1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
			) call_dump ON o.order_id=call_dump.order_id_calls
	WHERE (DATE(DATEADD(MIN,330,o.order_placed_at)) BETWEEN '2018-08-01' AND '2018-08-31') AND (o.retailer_id not in (12,80,82,115,85,69,63) or o.retailer_id is null)
	)
	GROUP BY 1
	ORDER BY 1
