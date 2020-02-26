select c.entity_id, c.task_id, c.s2c_at, c.s2c_date, c."action", c.next_task, c.last_hold_task,
		CASE WHEN c.action = 'hold' THEN CASE WHEN qal.action_name='hold' THEN hold_counts ELSE hold_counts-1 END END AS hold_counts,
      case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
      case when c.action = 'hold' then qal.action_name else c.action end as final_action
      from
              (select a.*,b.next_task,max(qal.id) as last_hold_task, COUNT(qal.id)+1 AS hold_counts
              from
                        (select qt.entity_id,qtl.task_id, min(dateadd(min,330,qt.created_at)) as s2c_at,min(date(dateadd(min,330,qt.created_at))) as s2c_date,
                        max(qal.action_name) as action
                        from q_task qt
                        inner join q_task_log qtl
                        on qt.id = qtl.task_id and qtl.action_at > '2018-08-01'
                        left join q_action_log qal
                        on qtl.task_id = qal.task_id and qtl.status = 2 
                        where qt.type = 4
                        group by 1,2)a
              left join (select task_id,next_task
                        from
                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
                                lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
                                from q_task qt
                                where qt.type in (4,5))
                        where is_soh = 5 and type = 4)b
              on a.task_id = b.task_id and a.action = 'hold'
              left join q_action_log qal
              on b.next_task = qal.task_id
              group by 1,2,3,4,5,6)c 
      left join q_action_log qal
      on c.last_hold_task = qal.id
      GROUP BY 1,2,3,4,5,6,7,8,9,10
      
      
------ Prefinal
      
      
WITH refnonref AS (
	SELECT a.*, CASE WHEN o1.order_id IS NOT NULL THEN 1 ELSE 0 END AS fulfilled_flag, cr.name AS ref_order_cancel_reason, 
			fo.order_id AS non_refil_fulf_order_id, fo.order_placed_at, CASE WHEN DATE(fo.order_placed_at)<ndd THEN 'Before NDD'
																			 WHEN DATE(fo.order_placed_at)>ndd THEN 'After NDD'
																			 WHEN DATE(fo.order_placed_at)=ndd THEN 'Same as NDD'
																		END AS time_comparison
	FROM
	(
		SELECT s1.customer_id, qt.entity_id AS subscription_id, DATE(qt.entity_timestamp) AS ndd, qal.action_name, 
				DATEADD(MIN,330,qal.actioned_at) AS actioned_at, soi.subscription_id AS new_subs_id,
				CASE WHEN qal.action_name IN ('accept','change-ndd-with-order') THEN qtso.subscription_order_id 
					 WHEN qal.action_name IN ('clone','move-to-doctor-program') THEN s2.template_order_id
				END AS ref_order_id
		FROM public.q_task qt
		INNER JOIN (SELECT entity_id, entity_timestamp FROM public.q_task qt 
					WHERE qt."type" IN (4,5) AND DATE(DATEADD(MIN,330,qt.created_at)) BETWEEN '2018-05-01' AND '2018-05-31' AND DATE(qt.entity_timestamp)>='2018-05-01') qr
					ON qt.entity_timestamp=qr.entity_timestamp AND qt.entity_id=qr.entity_id
		LEFT JOIN public.subscription s1 ON qt.entity_id=s1.id
		LEFT JOIN public.q_action_log qal ON (qt.id=qal.task_id AND qal.action_name NOT IN ('hold', 'change-ndd-without-order'))
		LEFT JOIN public.q_task_subscription_order qtso ON qal.task_id=qtso.task_id AND (qal.action_name IN ('accept','change-ndd-with-order'))
		LEFT JOIN public.subscription_origin_info soi ON qal.entity_id=soi.subscription_original_id AND (qal.action_name IN ('clone','move-to-doctor-program'))
		LEFT JOIN public.subscription s2 ON soi.subscription_id=s2.id
		WHERE qt."type" IN (4,5) 
		GROUP BY 1,2,3,4,5,6,7
		) a
	LEFT JOIN public."order" o1 ON a.ref_order_id=o1.order_id AND o1.status IN (9,10)
	LEFT JOIN public.order_cancel_reason ocr ON a.ref_order_id=ocr.order_id AND o1.order_id IS NULL
	LEFT JOIN public.cancel_reason cr ON ocr.cancel_reason_id=cr.id
	LEFT JOIN (
				SELECT o.order_id, customer_id, DATEADD(MIN,330,o.order_placed_at) AS order_placed_at
				FROM public."order" o
				LEFT JOIN public.order_subscription_info osi ON o.order_id=osi.order_id
				WHERE o.status IN (9,10) AND DATE(DATEADD(MIN,330,o.order_placed_at))>'2018-04-20' AND osi.order_id IS NULL
	) fo ON a.customer_id=fo.customer_id AND DATE(fo.order_placed_at) BETWEEN (a.ndd-10) AND (a.ndd+10)
	ORDER BY 3
)
SELECT COUNT(customer_id) AS total_distinct_customers, 
		COUNT(CASE WHEN fulf_ref_orders=0 THEN customer_id END) AS customers_with_unfulfilled_refil_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NULL THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_unfulfilled_normal_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NOT NULL THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_fulfilled_normal_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent>0 THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_atlrast_one_med_match_fulfilled_normal_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='76%-100%' THEN customer_id END) AS percent76_100,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='51%-75%' THEN customer_id END) AS percent51_75,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='26%-50%' THEN customer_id END) AS percent26_50,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='>0%-25%' THEN customer_id END) AS percent0_25,
		COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent=1 THEN customer_id END) AS percent100exact
FROM
(
	SELECT abc.customer_id, subscription_id, non_refil_fulf_order_id, time_comparison, xyz.fulf_ref_orders, final_match_percent,
			CASE WHEN final_match_percent>.75 THEN '76%-100%'
				 WHEN final_match_percent>.5 THEN '51%-75%'
				 WHEN final_match_percent>.25 THEN '26%-50%'
				 WHEN final_match_percent>0 THEN '>0%-25%'
			END AS match_percent_bucket
	FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY final_match_percent DESC NULLS LAST) AS row_num
			FROM (
				SELECT y.*, CASE WHEN matchpercent1 IS NOT NULL 
								 THEN CASE WHEN matchpercent2>=matchpercent1 
								 		   THEN matchpercent2
								 		   ELSE matchpercent1
								 	  END
								 ELSE matchpercent2
							END AS final_match_percent
					FROM (
						SELECT x.*, CASE WHEN x.non_refil_fulf_order_id IS NOT NULL
										 THEN (count(DISTINCT sd2.ucode)::float / NULLIF(count(distinct case when len(mn2.ucode) > 1 then mn2.ucode end ),0)::float)::float
									END	as matchpercent2
						FROM (
							SELECT refnonref.*, CASE WHEN refnonref.non_refil_fulf_order_id IS NOT NULL
													 THEN (count(DISTINCT mn1.ucode)::float / NULLIF(count(distinct case when len(sd1.ucode) > 1 then sd1.ucode end ),0)::float)::float 
												END	as matchpercent1 
							FROM refnonref
							LEFT JOIN public.subscription_digitization sd1 ON refnonref.subscription_id=sd1.subscription_id AND sd1.ucode IS NOT NULL
							LEFT JOIN public.medicine_notes mn1 ON refnonref.non_refil_fulf_order_id=mn1.order_id AND sd1.ucode=mn1.ucode
							GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
							) x
						LEFT JOIN public.medicine_notes mn2 ON x.non_refil_fulf_order_id=mn2.order_id AND mn2.ucode IS NOT NULL
						LEFT JOIN public.subscription_digitization sd2 ON x.subscription_id=sd2.subscription_id AND mn2.ucode=sd2.ucode
						GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
					) y
				)
			)abc
	LEFT JOIN (SELECT customer_id, SUM(fulfilled_flag) AS fulf_ref_orders FROM refnonref GROUP BY 1) xyz ON abc.customer_id=xyz.customer_id
	WHERE row_num=1
)
ORDER BY 1
;







------------Testing


WITH refnonref AS (
	SELECT a.*, CASE WHEN o1.order_id IS NOT NULL THEN 1 ELSE 0 END AS fulfilled_flag, cr.name AS ref_order_cancel_reason, 
			fo.order_id AS non_refil_fulf_order_id, fo.order_placed_at, CASE WHEN DATE(fo.order_placed_at)<ndd THEN 'Before NDD'
																			 WHEN DATE(fo.order_placed_at)>ndd THEN 'After NDD'
																			 WHEN DATE(fo.order_placed_at)=ndd THEN 'Same as NDD'
																		END AS time_comparison
	FROM
	(
		SELECT s1.customer_id, qt.entity_id AS subscription_id, DATE(qt.entity_timestamp) AS ndd, qt.final_action, s2c_at, --soi.subscription_id AS new_subs_id,
				CASE WHEN qt.final_action IN ('accept','change-ndd-with-order') THEN qtso.subscription_order_id 
					 WHEN qt.final_action IN ('clone','move-to-doctor-program') THEN s2.template_order_id
				END AS ref_order_id
		FROM (
				select c.entity_id, c.entity_timestamp, c.task_id, c.s2c_at, c.s2c_date, c."action", c.next_task, c.last_hold_task,
						CASE WHEN c.action = 'hold' THEN CASE WHEN qal.action_name='hold' THEN hold_counts ELSE hold_counts-1 END END AS hold_counts,
				      	case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
				      	case when c.action = 'hold' then qal.action_name else c.action end as final_action
				from
		              (select a.*,b.next_task,max(qal.id) as last_hold_task, COUNT(qal.id)+1 AS hold_counts
		              from
		                        (select qt.entity_id,qt.entity_timestamp,qtl.task_id, min(dateadd(min,330,qt.created_at)) as s2c_at,min(date(dateadd(min,330,qt.created_at))) as s2c_date,
		                        max(qal.action_name) as action
		                        from q_task qt
		                        inner join q_task_log qtl
		                        on qt.id = qtl.task_id 
		                        left join q_action_log qal
		                        on qtl.task_id = qal.task_id and qtl.status = 2 
		                        where qt.type = 4 and 
		                        		(EXTRACT(YEAR FROM DATE(DATEADD(MIN,330,qt.created_at)))=EXTRACT(YEAR FROM CURRENT_DATE)) AND 
		                        		(EXTRACT(MONTH FROM DATE(DATEADD(MIN,330,qt.created_at)))=(EXTRACT(MONTH FROM CURRENT_DATE)-3))
		                        group by 1,2,3)a
		              left join (select task_id,next_task
		                        from
		                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
		                                lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
		                                from q_task qt
		                                where qt.type in (4,5))
		                        where is_soh = 5 and type = 4)b
		              on a.task_id = b.task_id and a.action = 'hold'
		              left join q_action_log qal
		              on b.next_task = qal.task_id
		              group by 1,2,3,4,5,6,7)c 
		      	left join q_action_log qal
		      	on c.last_hold_task = qal.id
		     	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
		) qt
		LEFT JOIN public.subscription s1 ON qt.entity_id=s1.id
		LEFT JOIN public.q_task_subscription_order qtso ON qt.final_task=qtso.task_id AND (qt.final_action IN ('accept','change-ndd-with-order'))
		LEFT JOIN public.subscription_origin_info soi ON qt.entity_id=soi.subscription_original_id AND (qt.final_action IN ('clone','move-to-doctor-program'))
		LEFT JOIN public.subscription s2 ON soi.subscription_id=s2.id
		GROUP BY 1,2,3,4,5,6
		) a
	LEFT JOIN public."order" o1 ON a.ref_order_id=o1.order_id AND o1.status IN (9,10)
	LEFT JOIN public.order_cancel_reason ocr ON a.ref_order_id=ocr.order_id AND o1.order_id IS NULL
	LEFT JOIN public.cancel_reason cr ON ocr.cancel_reason_id=cr.id
	LEFT JOIN (
				SELECT o.order_id, customer_id, DATEADD(MIN,330,o.order_placed_at) AS order_placed_at
				FROM public."order" o
				LEFT JOIN public.order_subscription_info osi ON o.order_id=osi.order_id
				WHERE o.status IN (9,10) AND osi.order_id IS NULL --AND DATE(DATEADD(MIN,330,o.order_placed_at))>'2018-04-20'
	) fo ON a.customer_id=fo.customer_id AND DATE(fo.order_placed_at) BETWEEN (a.ndd-10) AND (a.ndd+10)
	ORDER BY 3
)
SELECT 'COUNTS' AS description,
		AVG(distinct_refills_on_stc) AS aa, 
		COUNT(customer_id) AS bb, 
		COUNT(CASE WHEN fulf_ref_orders=0 THEN customer_id END) AS cc,
		COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NULL THEN customer_id END) AS dd,
		COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NOT NULL THEN customer_id END) AS ee,
		COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent>0 THEN customer_id END) AS ff,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='76%-100%' THEN customer_id END) AS gg,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='51%-75%' THEN customer_id END) AS hh,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='26%-50%' THEN customer_id END) AS ii,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='>0%-25%' THEN customer_id END) AS jj,
		COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent=1 THEN customer_id END) AS kk
FROM
(
	SELECT abc.customer_id, subscription_id, non_refil_fulf_order_id, time_comparison, xyz.fulf_ref_orders, final_match_percent, distinct_refills_on_stc,
			CASE WHEN final_match_percent>.75 THEN '76%-100%'
				 WHEN final_match_percent>.5 THEN '51%-75%'
				 WHEN final_match_percent>.25 THEN '26%-50%'
				 WHEN final_match_percent>0 THEN '>0%-25%'
			END AS match_percent_bucket
	FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY final_match_percent DESC NULLS LAST) AS row_num
			FROM (
				SELECT y.*, CASE WHEN matchpercent1 IS NOT NULL 
								 THEN CASE WHEN matchpercent2>=matchpercent1 
								 		   THEN matchpercent2
								 		   ELSE matchpercent1
								 	  END
								 ELSE matchpercent2
							END AS final_match_percent
					FROM (
						SELECT x.*, CASE WHEN x.non_refil_fulf_order_id IS NOT NULL
										 THEN (count(DISTINCT sd2.ucode)::float / NULLIF(count(distinct case when len(mn2.ucode) > 1 then mn2.ucode end ),0)::float)::float
									END	as matchpercent2
						FROM (
							SELECT refnonref.*, CASE WHEN refnonref.non_refil_fulf_order_id IS NOT NULL
													 THEN (count(DISTINCT mn1.ucode)::float / NULLIF(count(distinct case when len(sd1.ucode) > 1 then sd1.ucode end ),0)::float)::float 
												END	as matchpercent1 
							FROM refnonref
							LEFT JOIN public.subscription_digitization sd1 ON refnonref.subscription_id=sd1.subscription_id AND sd1.ucode IS NOT NULL
							LEFT JOIN public.medicine_notes mn1 ON refnonref.non_refil_fulf_order_id=mn1.order_id AND sd1.ucode=mn1.ucode
							GROUP BY 1,2,3,4,5,6,7,8,9,10,11
							) x
						LEFT JOIN public.medicine_notes mn2 ON x.non_refil_fulf_order_id=mn2.order_id AND mn2.ucode IS NOT NULL
						LEFT JOIN public.subscription_digitization sd2 ON x.subscription_id=sd2.subscription_id AND mn2.ucode=sd2.ucode
						GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
					) y
				)
			)abc
	LEFT JOIN (SELECT customer_id, SUM(fulfilled_flag) AS fulf_ref_orders FROM refnonref GROUP BY 1) xyz ON abc.customer_id=xyz.customer_id
	LEFT JOIN (SELECT COUNT(DISTINCT subscription_id) AS distinct_refills_on_stc FROM refnonref) efg ON xyz.customer_id IS NOT NULL
	WHERE row_num=1
)
ORDER BY 1
;










------------final_report


WITH refnonref AS (
	SELECT a.*, CASE WHEN o1.order_id IS NOT NULL THEN 1 ELSE 0 END AS fulfilled_flag, cr.name AS ref_order_cancel_reason, 
			fo.order_id AS non_refil_fulf_order_id, fo.order_placed_at, CASE WHEN DATE(fo.order_placed_at)<ndd THEN 'Before NDD'
																			 WHEN DATE(fo.order_placed_at)>ndd THEN 'After NDD'
																			 WHEN DATE(fo.order_placed_at)=ndd THEN 'Same as NDD'
																		END AS time_comparison
	FROM
	(
		SELECT s1.customer_id, qt.entity_id AS subscription_id, DATE(qt.entity_timestamp) AS ndd, qt.final_action, s2c_at, --soi.subscription_id AS new_subs_id,
				CASE WHEN qt.final_action IN ('accept','change-ndd-with-order') THEN qtso.subscription_order_id 
					 WHEN qt.final_action IN ('clone','move-to-doctor-program') THEN s2.template_order_id
				END AS ref_order_id
		FROM (
				select c.entity_id, c.entity_timestamp, c.task_id, c.s2c_at, c.s2c_date, c."action", c.next_task, c.last_hold_task,
						CASE WHEN c.action = 'hold' THEN CASE WHEN qal.action_name='hold' THEN hold_counts ELSE hold_counts-1 END END AS hold_counts,
				      	case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
				      	case when c.action = 'hold' then qal.action_name else c.action end as final_action
				from
		              (select a.*,b.next_task,max(qal.id) as last_hold_task, COUNT(qal.id)+1 AS hold_counts
		              from
		                        (select qt.entity_id,qt.entity_timestamp,qtl.task_id, min(dateadd(min,330,qt.created_at)) as s2c_at,min(date(dateadd(min,330,qt.created_at))) as s2c_date,
		                        max(qal.action_name) as action
		                        from q_task qt
		                        inner join q_task_log qtl
		                        on qt.id = qtl.task_id 
		                        left join q_action_log qal
		                        on qtl.task_id = qal.task_id and qtl.status = 2 
		                        where qt.type = 4 and 
		                        		(EXTRACT(YEAR FROM DATE(DATEADD(MIN,330,qt.created_at)))=EXTRACT(YEAR FROM CURRENT_DATE)) AND 
		                        		(EXTRACT(MONTH FROM DATE(DATEADD(MIN,330,qt.created_at)))=(EXTRACT(MONTH FROM CURRENT_DATE)-1))				-- just change the '3' value to alter the month for analysis
		                        group by 1,2,3)a
		              left join (select task_id,next_task
		                        from
		                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
		                                lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
		                                from q_task qt
		                                where qt.type in (4,5))
		                        where is_soh = 5 and type = 4)b
		              on a.task_id = b.task_id and a.action = 'hold'
		              left join q_action_log qal
		              on b.next_task = qal.task_id
		              group by 1,2,3,4,5,6,7)c 
		      	left join q_action_log qal
		      	on c.last_hold_task = qal.id
		     	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
		) qt
		LEFT JOIN public.subscription s1 ON qt.entity_id=s1.id
		LEFT JOIN public.q_task_subscription_order qtso ON qt.final_task=qtso.task_id AND (qt.final_action IN ('accept','change-ndd-with-order'))
		LEFT JOIN public.subscription_origin_info soi ON qt.entity_id=soi.subscription_original_id AND (qt.final_action IN ('clone','move-to-doctor-program'))
		LEFT JOIN public.subscription s2 ON soi.subscription_id=s2.id
		GROUP BY 1,2,3,4,5,6
		) a
	LEFT JOIN public."order" o1 ON a.ref_order_id=o1.order_id AND o1.status IN (9,10)
	LEFT JOIN public.order_cancel_reason ocr ON a.ref_order_id=ocr.order_id AND o1.order_id IS NULL
	LEFT JOIN public.cancel_reason cr ON ocr.cancel_reason_id=cr.id
	LEFT JOIN (
				SELECT o.order_id, customer_id, DATEADD(MIN,330,o.order_placed_at) AS order_placed_at
				FROM public."order" o
				LEFT JOIN public.order_subscription_info osi ON o.order_id=osi.order_id
				WHERE o.status IN (9,10) AND osi.order_id IS NULL --AND DATE(DATEADD(MIN,330,o.order_placed_at))>'2018-04-20'
	) fo ON a.customer_id=fo.customer_id AND DATE(fo.order_placed_at) BETWEEN (a.ndd-10) AND (a.ndd+10)
	ORDER BY 3
),
pivoting AS (
	SELECT 	AVG(distinct_refills_on_stc) AS distinct_refills_on_stc, 
			COUNT(customer_id) AS total_distinct_customers, 
			COUNT(CASE WHEN fulf_ref_orders>0 THEN customer_id END) AS customers_with_fulfilled_refil_order,
			COUNT(CASE WHEN fulf_ref_orders=0 THEN customer_id END) AS customers_with_unfulfilled_refil_order,
			COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NULL THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_unfulfilled_normal_order,
			COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NOT NULL THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_fulfilled_normal_order,
			COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent>0 THEN customer_id END) AS customers_with_unfulfilled_refill_order_and_atleast_one_med_match_fulfilled_normal_order,
			COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='76%-100%' THEN customer_id END) AS percent76_100,
			COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='51%-75%' THEN customer_id END) AS percent51_75,
			COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='26%-50%' THEN customer_id END) AS percent26_50,
			COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='>0%-25%' THEN customer_id END) AS percent0_25,
			COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent=1 THEN customer_id END) AS percent100exact
	FROM
	(
		SELECT abc.customer_id, subscription_id, non_refil_fulf_order_id, time_comparison, xyz.fulf_ref_orders, final_match_percent, distinct_refills_on_stc,
				CASE WHEN final_match_percent>.75 THEN '76%-100%'
					 WHEN final_match_percent>.5 THEN '51%-75%'
					 WHEN final_match_percent>.25 THEN '26%-50%'
					 WHEN final_match_percent>0 THEN '>0%-25%'
				END AS match_percent_bucket
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY final_match_percent DESC NULLS LAST) AS row_num
				FROM (
					SELECT y.*, CASE WHEN matchpercent1 IS NOT NULL 
									 THEN CASE WHEN matchpercent2>=matchpercent1 
									 		   THEN matchpercent2
									 		   ELSE matchpercent1
									 	  END
									 ELSE matchpercent2
								END AS final_match_percent
						FROM (
							SELECT x.*, CASE WHEN x.non_refil_fulf_order_id IS NOT NULL
											 THEN (count(DISTINCT sd2.ucode)::float / NULLIF(count(distinct case when len(mn2.ucode) > 1 then mn2.ucode end ),0)::float)::float
										END	as matchpercent2
							FROM (
								SELECT refnonref.*, CASE WHEN refnonref.non_refil_fulf_order_id IS NOT NULL
														 THEN (count(DISTINCT mn1.ucode)::float / NULLIF(count(distinct case when len(sd1.ucode) > 1 then sd1.ucode end ),0)::float)::float 
													END	as matchpercent1 
								FROM refnonref
								LEFT JOIN public.subscription_digitization sd1 ON refnonref.subscription_id=sd1.subscription_id AND sd1.ucode IS NOT NULL
								LEFT JOIN public.medicine_notes mn1 ON refnonref.non_refil_fulf_order_id=mn1.order_id AND sd1.ucode=mn1.ucode
								GROUP BY 1,2,3,4,5,6,7,8,9,10,11
								) x
							LEFT JOIN public.medicine_notes mn2 ON x.non_refil_fulf_order_id=mn2.order_id AND mn2.ucode IS NOT NULL
							LEFT JOIN public.subscription_digitization sd2 ON x.subscription_id=sd2.subscription_id AND mn2.ucode=sd2.ucode
							GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
						) y
					)
				)abc
		LEFT JOIN (SELECT customer_id, SUM(fulfilled_flag) AS fulf_ref_orders FROM refnonref GROUP BY 1) xyz ON abc.customer_id=xyz.customer_id
		LEFT JOIN (SELECT COUNT(DISTINCT subscription_id) AS distinct_refills_on_stc FROM refnonref) efg ON xyz.customer_id IS NOT NULL
		WHERE row_num=1
	)
	ORDER BY 1
)
SELECT description, "count", "percent"
FROM
(
	SELECT 1 AS sr_no, 'Distinct refills on STC-SOH' AS description, distinct_refills_on_stc AS "count", NULL AS "percent" FROM pivoting
	UNION ALL
	SELECT 2 AS sr_no, 'Distinct customers on STC-SOH' AS description, total_distinct_customers AS "count", NULL AS "percent" FROM pivoting
	UNION ALL
	SELECT 2.5 AS sr_no, 'Customers with at least one fulfilled refill order' AS description, customers_with_fulfilled_refil_order AS "count", ROUND((customers_with_fulfilled_refil_order::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 3 AS sr_no, 'Customers with no fulfilled refill order' AS description, customers_with_unfulfilled_refil_order AS "count", ROUND((customers_with_unfulfilled_refil_order::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 4 AS sr_no, 'Customers with no fulfilled refill order and no fulfilled normal order within (NDD-10 to NDD+10)' AS description, customers_with_unfulfilled_refil_order_and_unfulfilled_normal_order AS "count", ROUND((customers_with_unfulfilled_refil_order_and_unfulfilled_normal_order::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 5 AS sr_no, 'Customers with no fulfilled refill order but fulfilled normal order within (NDD-10 to NDD+10)' AS description, customers_with_unfulfilled_refil_order_and_fulfilled_normal_order AS "count", ROUND((customers_with_unfulfilled_refil_order_and_fulfilled_normal_order::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 6 AS sr_no, NULL AS description, NULL AS "count", NULL AS "percent" FROM pivoting
	UNION ALL
	SELECT 7 AS sr_no, 'Customers with no fulfilled refill order but fulfilled normal order within (NDD-10 to NDD+10) with atleast one med match with refill' AS description, customers_with_unfulfilled_refill_order_and_atleast_one_med_match_fulfilled_normal_order AS "count", ROUND((customers_with_unfulfilled_refill_order_and_atleast_one_med_match_fulfilled_normal_order::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 8 AS sr_no, '>75% to 100% match percent' AS description, percent76_100 AS "count", ROUND((percent76_100::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 9 AS sr_no, '>50% to 75% match percent' AS description, percent51_75 AS "count", ROUND((percent51_75::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 10 AS sr_no, '>25% to 50% match percent' AS description, percent26_50 AS "count", ROUND((percent26_50::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 11 AS sr_no, '>0% to 25% match percent' AS description, percent0_25 AS "count", ROUND((percent0_25::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
	UNION ALL
	SELECT 12 AS sr_no, NULL AS description, NULL AS "count", NULL AS "percent" FROM pivoting
	UNION ALL
	SELECT 13 AS sr_no, '100% medicine match, subset of first bin' AS description, percent100exact AS "count", ROUND((percent100exact::FLOAT)/(total_distinct_customers::FLOAT)*100)::INT AS "percent" FROM pivoting
)
ORDER BY sr_no
;







------------final


WITH refnonref AS (
	SELECT a.*, CASE WHEN o1.order_id IS NOT NULL THEN 1 ELSE 0 END AS fulfilled_flag, cr.name AS ref_order_cancel_reason, 
			fo.order_id AS non_refil_fulf_order_id, fo.order_placed_at, CASE WHEN DATE(fo.order_placed_at)<ndd THEN 'Before NDD'
																			 WHEN DATE(fo.order_placed_at)>ndd THEN 'After NDD'
																			 WHEN DATE(fo.order_placed_at)=ndd THEN 'Same as NDD'
																		END AS time_comparison
	FROM
	(
		SELECT s1.customer_id, qt.entity_id AS subscription_id, DATE(qt.entity_timestamp) AS ndd, qt.final_action, s2c_at, --soi.subscription_id AS new_subs_id,
				CASE WHEN qt.final_action IN ('accept','change-ndd-with-order') THEN qtso.subscription_order_id 
					 WHEN qt.final_action IN ('clone','move-to-doctor-program') THEN s2.template_order_id
				END AS ref_order_id
		FROM (
				select c.entity_id, c.entity_timestamp, c.task_id, c.s2c_at, c.s2c_date, c."action", c.next_task, c.last_hold_task,
						CASE WHEN c.action = 'hold' THEN CASE WHEN qal.action_name='hold' THEN hold_counts ELSE hold_counts-1 END END AS hold_counts,
				      	case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
				      	case when c.action = 'hold' then qal.action_name else c.action end as final_action
				from
		              (select a.*,b.next_task,max(qal.id) as last_hold_task, COUNT(qal.id)+1 AS hold_counts
		              from
		                        (select qt.entity_id,qt.entity_timestamp,qtl.task_id, min(dateadd(min,330,qt.created_at)) as s2c_at,min(date(dateadd(min,330,qt.created_at))) as s2c_date,
		                        max(qal.action_name) as action
		                        from q_task qt
		                        inner join q_task_log qtl
		                        on qt.id = qtl.task_id 
		                        left join q_action_log qal
		                        on qtl.task_id = qal.task_id and qtl.status = 2 
		                        where qt.type = 4 and 
		                        		(EXTRACT(YEAR FROM DATE(DATEADD(MIN,330,qt.created_at)))=EXTRACT(YEAR FROM CURRENT_DATE)) AND 
		                        		(EXTRACT(MONTH FROM DATE(DATEADD(MIN,330,qt.created_at)))=(EXTRACT(MONTH FROM CURRENT_DATE)-3))
		                        group by 1,2,3)a
		              left join (select task_id,next_task
		                        from
		                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
		                                lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
		                                from q_task qt
		                                where qt.type in (4,5))
		                        where is_soh = 5 and type = 4)b
		              on a.task_id = b.task_id and a.action = 'hold'
		              left join q_action_log qal
		              on b.next_task = qal.task_id
		              group by 1,2,3,4,5,6,7)c 
		      	left join q_action_log qal
		      	on c.last_hold_task = qal.id
		     	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
		) qt
		LEFT JOIN public.subscription s1 ON qt.entity_id=s1.id
		LEFT JOIN public.q_task_subscription_order qtso ON qt.final_task=qtso.task_id AND (qt.final_action IN ('accept','change-ndd-with-order'))
		LEFT JOIN public.subscription_origin_info soi ON qt.entity_id=soi.subscription_original_id AND (qt.final_action IN ('clone','move-to-doctor-program'))
		LEFT JOIN public.subscription s2 ON soi.subscription_id=s2.id
		GROUP BY 1,2,3,4,5,6
		) a
	LEFT JOIN public."order" o1 ON a.ref_order_id=o1.order_id AND o1.status IN (9,10)
	LEFT JOIN public.order_cancel_reason ocr ON a.ref_order_id=ocr.order_id AND o1.order_id IS NULL
	LEFT JOIN public.cancel_reason cr ON ocr.cancel_reason_id=cr.id
	LEFT JOIN (
				SELECT o.order_id, customer_id, DATEADD(MIN,330,o.order_placed_at) AS order_placed_at
				FROM public."order" o
				LEFT JOIN public.order_subscription_info osi ON o.order_id=osi.order_id
				WHERE o.status IN (9,10) AND osi.order_id IS NULL --AND DATE(DATEADD(MIN,330,o.order_placed_at))>'2018-04-20'
	) fo ON a.customer_id=fo.customer_id AND DATE(fo.order_placed_at) BETWEEN (a.ndd-10) AND (a.ndd+10)
	ORDER BY 3
)
SELECT 'COUNTS' AS description,
		AVG(distinct_refills_on_stc) AS distinct_refills_on_stc, 
		COUNT(customer_id) AS total_distinct_customers, 
		COUNT(CASE WHEN fulf_ref_orders=0 THEN customer_id END) AS customers_with_unfulfilled_refil_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NULL THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_unfulfilled_normal_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND non_refil_fulf_order_id IS NOT NULL THEN customer_id END) AS customers_with_unfulfilled_refil_order_and_fulfilled_normal_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent>0 THEN customer_id END) AS customers_with_unfulfilled_refill_order_and_atleast_one_med_match_fulfilled_normal_order,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='76%-100%' THEN customer_id END) AS percent76_100,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='51%-75%' THEN customer_id END) AS percent51_75,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='26%-50%' THEN customer_id END) AS percent26_50,
		COUNT(CASE WHEN fulf_ref_orders=0 AND match_percent_bucket='>0%-25%' THEN customer_id END) AS percent0_25,
		COUNT(CASE WHEN fulf_ref_orders=0 AND final_match_percent=1 THEN customer_id END) AS percent100exact
FROM
(
	SELECT abc.customer_id, subscription_id, non_refil_fulf_order_id, time_comparison, xyz.fulf_ref_orders, final_match_percent, distinct_refills_on_stc,
			CASE WHEN final_match_percent>.75 THEN '76%-100%'
				 WHEN final_match_percent>.5 THEN '51%-75%'
				 WHEN final_match_percent>.25 THEN '26%-50%'
				 WHEN final_match_percent>0 THEN '>0%-25%'
			END AS match_percent_bucket
	FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY final_match_percent DESC NULLS LAST) AS row_num
			FROM (
				SELECT y.*, CASE WHEN matchpercent1 IS NOT NULL 
								 THEN CASE WHEN matchpercent2>=matchpercent1 
								 		   THEN matchpercent2
								 		   ELSE matchpercent1
								 	  END
								 ELSE matchpercent2
							END AS final_match_percent
					FROM (
						SELECT x.*, CASE WHEN x.non_refil_fulf_order_id IS NOT NULL
										 THEN (count(DISTINCT sd2.ucode)::float / NULLIF(count(distinct case when len(mn2.ucode) > 1 then mn2.ucode end ),0)::float)::float
									END	as matchpercent2
						FROM (
							SELECT refnonref.*, CASE WHEN refnonref.non_refil_fulf_order_id IS NOT NULL
													 THEN (count(DISTINCT mn1.ucode)::float / NULLIF(count(distinct case when len(sd1.ucode) > 1 then sd1.ucode end ),0)::float)::float 
												END	as matchpercent1 
							FROM refnonref
							LEFT JOIN public.subscription_digitization sd1 ON refnonref.subscription_id=sd1.subscription_id AND sd1.ucode IS NOT NULL
							LEFT JOIN public.medicine_notes mn1 ON refnonref.non_refil_fulf_order_id=mn1.order_id AND sd1.ucode=mn1.ucode
							GROUP BY 1,2,3,4,5,6,7,8,9,10,11
							) x
						LEFT JOIN public.medicine_notes mn2 ON x.non_refil_fulf_order_id=mn2.order_id AND mn2.ucode IS NOT NULL
						LEFT JOIN public.subscription_digitization sd2 ON x.subscription_id=sd2.subscription_id AND mn2.ucode=sd2.ucode
						GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
					) y
				)
			)abc
	LEFT JOIN (SELECT customer_id, SUM(fulfilled_flag) AS fulf_ref_orders FROM refnonref GROUP BY 1) xyz ON abc.customer_id=xyz.customer_id
	LEFT JOIN (SELECT COUNT(DISTINCT subscription_id) AS distinct_refills_on_stc FROM refnonref) efg ON xyz.customer_id IS NOT NULL
	WHERE row_num=1
)
ORDER BY 1
;
