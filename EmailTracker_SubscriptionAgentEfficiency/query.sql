SELECT DATE(action_at) AS action_date, classification, 
		username AS actioned_by, action_task, action_point, 
		COUNT(performed_by) AS total_actions, SUM(order_delivered_flag) AS total_orders_delivered
FROM
(
(
SELECT fin.*,
		CASE 
			 WHEN (fin.classification='STC' AND action_task='accept') THEN 7
			 WHEN (fin.classification='STC' AND action_task='reject') THEN 3
			 WHEN (fin.classification='STC' AND action_task='clone') THEN 9
			 WHEN (fin.classification='STC' AND action_task='move_to_DP') THEN 8
			 WHEN (fin.classification='STC' AND action_task='STC_Hold') THEN 2
			 WHEN (fin.classification='STC' AND action_task='CDD_inRange(n+6)') THEN 7
			 WHEN (fin.classification='STC' AND action_task='CDD_other') THEN 3
			 WHEN (fin.classification='SOH' AND action_task='accept') THEN 7
			 WHEN (fin.classification='SOH' AND action_task='reject') THEN 3
			 WHEN (fin.classification='SOH' AND action_task='clone') THEN 9
			 WHEN (fin.classification='SOH' AND action_task='move_to_DP') THEN 8
			 WHEN (fin.classification='SOH' AND action_task='Hold_callBack') THEN 3
			 WHEN (fin.classification='SOH' AND action_task='Hold_other') THEN 1
			 WHEN (fin.classification='SOH' AND action_task='CDD_inRange(n+6)') THEN 7
			 WHEN (fin.classification='SOH' AND action_task='CDD_other') THEN 3
			 WHEN (fin.classification='GTE' AND action_task='reject') THEN 3
			 WHEN (fin.classification='GTE' AND action_task='clone') THEN 6
			 WHEN (fin.classification='GTE' AND action_task='move_to_DP') THEN 5
			 WHEN (fin.classification='GTE' AND action_task='Hold_callBack') THEN 2
			 WHEN (fin.classification='GTE' AND action_task='Hold_other') THEN 1
			 WHEN (fin.classification='Refill_Details' AND action_task='move_to_DP') THEN 5
			 WHEN (fin.classification='Refill_Details' AND action_task='clone') THEN 6
			 WHEN (fin.classification='Refill_Details' AND action_task='reject') THEN 1
			 WHEN (fin.classification='CnR' AND action_task='Hold_callBack') THEN 2
			 WHEN (fin.classification='CnR' AND action_task='Hold_other') THEN 1
			 END
			 AS action_point
FROM
(
	SELECT   
		CASE 
			 WHEN (qt."type"=4) THEN 'STC'
			 WHEN (qt."type"=5) THEN 'SOH'
			 WHEN (qt."type"=6) THEN 'CnR'
			 WHEN (qt."type"=7) THEN 'GTE'
			 WHEN (qal.task_id IS NULL) THEN 'Refill_Details'			 
			 WHEN (qt."type" IN (1,2,3)) THEN 'not_required'
			 ELSE 'task_id_notNull_task_type_Null'
		END AS classification, 
		CASE WHEN (qt."type"!=6 OR qal.task_id IS NULL) THEN qal.entity_id END AS subscription_id,
		CASE WHEN qt."type"=6 THEN qal.entity_id 
			 WHEN action_name IN ('accept','change-ndd-with-order') THEN qtso.subscription_order_id 
			 WHEN action_name IN ('clone','move-to-doctor-program') THEN s.template_order_id
			 END AS order_id, 
		qal.action_by_user_id AS performed_by, DATEADD(MIN,330,qal.actioned_at) AS action_at,		
		CASE 
			 WHEN (qal.action_name='hold' AND qt."type"=4) THEN 'STC_Hold'
			 WHEN (qal.action_name='hold' AND qthl.reason=2) THEN 'Hold_callBack'
			 WHEN (qal.action_name='hold') THEN 'Hold_other'
			 WHEN (qal.action_name='change-ndd-with-order') THEN 'CDD_inRange(n+6)'
			 WHEN (qal.action_name='change-ndd-without-order') THEN 'CDD_other'
			 WHEN (qal.action_name='move-to-doctor-program') THEN 'move_to_DP'
			 ELSE qal.action_name
		END AS action_task,
		CASE WHEN qal.action_name IN ('accept','change-ndd-with-order') AND qtso.subscription_order_id IS NOT NULL 
				THEN CASE WHEN o.status IN (9,10) THEN 1 ELSE 0 END
			 WHEN qal.action_name IN ('clone','move-to-doctor-program') 
			 	THEN CASE WHEN o1.status IN (9,10) THEN 1 ELSE 0 END
		END AS order_delivered_flag
	FROM pe_queue_queue.q_action_log qal
	LEFT JOIN pe_queue_queue.q_task qt ON qal.task_id=qt.id
	LEFT JOIN pe_queue_queue.q_task_hold_log qthl ON qal.task_id=qthl.task_id AND qal.action_name='hold' AND qal.action_by_user_id=qthl.action_by AND qal.actioned_at=qthl.action_at
	LEFT JOIN pe_queue_queue.q_task_subscription_order qtso ON qal.task_id=qtso.task_id AND (qal.action_name IN ('accept','change-ndd-with-order'))
	LEFT JOIN pe_pe2_pe2."order" o ON qtso.subscription_order_id=o.id
	LEFT JOIN pe_pe2_pe2.subscription_origin_info soi ON qal.entity_id=soi.parent_subscription_id AND (qal.action_name IN ('clone','move-to-doctor-program'))
	LEFT JOIN pe_pe2_pe2.subscription s ON soi.subscription_id=s.id
	LEFT JOIN pe_pe2_pe2."order" o1 ON s.template_order_id=o1.id
	WHERE (DATE(DATEADD(MIN,330,qal.actioned_at))>='2018-02-01') AND (qt."type" IN (4,5,6,7) OR qal.task_id IS NULL)
) fin
WHERE ((fin.classification!='Refill_Details') OR (action_task NOT IN ('CDD_inRange(n+6)','CDD_other')))
GROUP BY 1,2,3,4,5,6,7,8
)
UNION
(
SELECT 'CnR' AS classification, NULL::INT AS subscription_id, ocrh.order_id AS order_id, action_by AS performed_by, DATEADD(MIN,330,action_time) AS action_at,
							CASE "action" WHEN 1 THEN 'reassign'
											WHEN 2 THEN 'reject'
											WHEN 3 THEN 'move_to_DP'
											END AS action_task, 
							CASE WHEN (ocrh."action" IN (1,3)) AND (o.status IN (9,10)) THEN 1 ELSE 0 END AS order_delivered_flag,
							CASE "action" WHEN 1 THEN 5
											WHEN 2 THEN 5
											WHEN 3 THEN 6
											END AS action_point
FROM pe_pe2_pe2.order_cancel_reason_history ocrh
LEFT JOIN pe_pe2_pe2."order" o ON ocrh.order_id=o.id
WHERE ((DATE(DATEADD(MIN,330,action_time))>='2018-02-01') AND (action_by!=218) AND (action_by!=cancelled_by))
GROUP BY 1,2,3,4,5,6,7,8
)
UNION
(
SELECT 'Refill_Details' AS classification, s_cdd_log.subscription_id, osi.order_id, s_cdd_log.changed_by_user_id AS performed_by,
		DATEADD(MIN,330,s_cdd_log.created_at) AS action_at, 
		CASE WHEN osi.order_id IS NOT NULL 
			 THEN 'CDD_inRange(n+6)'
			 ELSE 'CDD_other'
		END AS action_task,
		CASE WHEN osi.order_id IS NOT NULL 
			 THEN CASE WHEN o.id IS NOT NULL 
			 		   THEN 1 ELSE 0 
			 	  END
		END AS order_delivered_flag,
		CASE WHEN osi.order_id IS NOT NULL 
			 THEN 7
			 ELSE 1
		END AS action_point
FROM pe_pe2_pe2.subscription_ndd_change_log s_cdd_log
LEFT JOIN pe_pe2_pe2.order_subscription_info osi ON (osi."source" IN (7,8)) AND s_cdd_log.subscription_id=osi.subscription_id AND ((osi.created_at-s_cdd_log.created_at) BETWEEN '00:00:00' AND '00:00:02')
LEFT JOIN pe_pe2_pe2."order" o ON osi.order_id=o.id AND o.status IN (9,10)
WHERE s_cdd_log."source" IN (9,10) AND DATEADD(MIN,330,s_cdd_log.created_at)>='2018-02-01'
GROUP BY 1,2,3,4,5,6,7,8
)
) combined
LEFT JOIN pe_pe2_pe2."user" u ON combined.performed_by=u.id
WHERE EXTRACT(YEAR FROM action_at)=EXTRACT(YEAR FROM (CURRENT_DATE-1)) 
		AND EXTRACT(MONTH FROM action_at)=EXTRACT(MONTH FROM (CURRENT_DATE-1))
		AND EXTRACT(DAY FROM action_at) BETWEEN 1 AND EXTRACT(DAY FROM (CURRENT_DATE-1))
GROUP BY 1,2,3,4,5
ORDER BY 1, 6 DESC
