---- detailed query for subscription on the queue from the S2C dashboard

select d.*,s.customer_id,s.start_date,
case when s.interval_value is null then 30 else s.interval_value end as cycle_length,
c.name as city, c1.name as supplier_city_name,c.does_carrier_delivery as courier_city,
case s.source
when 0 then case when soi.subscription_id is null then 'CMS - Create new' else 'clone' end
when 1 then 'APP'
when 3 then 'CMS caller'
when 4 then 'Order on call'
end as subscription_source,
case s.status
when 1 then 'Under review'
when 2 then 'Active'
when 3 then 'Cancelled'
when 4 then case when soi2.parent_subscription_id is not null then 'Completed with clone' else 'completed without clone' end
when 5 then 'Hold'
when 6 then 'Going to expire'
end as current_status,
qtso.subscription_order_id,
nvl(max(osi.rank),0) as rank
from (
	  select c.*, 
      case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
      case when c.action = 'hold' then qal.action_name else c.action end as final_action
      from
              (select a.*,b.next_task,max(qal.id) as last_hold_task
              from
                        (select qt.entity_id,qtl.task_id, min(dateadd(min,0,qt.created_at)) as s2c_at,min(date(dateadd(min,0,qt.created_at))) as s2c_date,max(qal.action_name) as action
                        from pe_queue_queue.q_task qt
                        inner join pe_queue_queue.q_task_log qtl on qt.id = qtl.task_id and qtl.action_at > '2018-01-18'
                        left join pe_queue_queue.q_action_log qal on qtl.task_id = qal.task_id and qtl.status = 2 
                        where qt.type = 4
                        group by 1,2)a
              left join (
              			select task_id,next_task
                        from
                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
                                		lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
                                from pe_queue_queue.q_task qt
                                where qt.type in (4,5)
                                )
                        where is_soh = 5 and type = 4
                        )b on a.task_id = b.task_id and a.action = 'hold'
              left join pe_queue_queue.q_action_log qal on b.next_task = qal.task_id
              group by 1,2,3,4,5,6)c 
      left join pe_queue_queue.q_action_log qal on c.last_hold_task = qal.id
      )d
left join pe2.subscription s on d.entity_id = s.id
left join pe2.city c on s.city_id = c.id
left join pe2.city c1 on c.supplier_city_id = c1.id
left join pe2.subscription_origin_info soi on s.id = soi.subscription_id
left join pe2.subscription_origin_info soi2 on s.id = soi2.parent_subscription_id
left join (
			SELECT subscription_id,order_id,order_placed_at, RANK() OVER (PARTITION BY root_subscription_id ORDER BY order_id)
			FROM
			(
				SELECT osi.subscription_id, osi.order_id, DATEADD(MIN,0,osi.created_at) AS order_placed_at,
						CASE WHEN soi.root_subscription_id IS NOT NULL THEN soi.root_subscription_id ELSE osi.subscription_id END AS root_subscription_id
				FROM pe2.order_subscription_info osi
				LEFT JOIN pe2.subscription_origin_info soi ON osi.subscription_id=soi.subscription_id
			)
		)osi on s.id = osi.subscription_id and d.s2c_at > osi.order_placed_at  
left join queue.q_task_subscription_order qtso on d.final_task = qtso.task_id
WHERE RIGHT(app_version,1) NOT IN ('t') OR app_version IS NULL
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;


----- query for a customer's subscription (root - highest parent) and conversion in subsequent cycles

SELECT s1.customer_id,
		s2.created_at AS root_subscription_start_date,
		s1.created_at AS current_subscription_start_date,
		case when s1.interval_value is null then 30 else s1.interval_value end as cycle_length,
		d.*
from (
	  select c.*, 
      case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
      case when c.action = 'hold' then qal.action_name else c.action end as final_action
      from
              (select a.*,b.next_task,max(qal.id) as last_hold_task
              from
                        (select qt.entity_id,qtl.task_id, min(dateadd(min,0,qt.created_at)) as s2c_at,min(date(dateadd(min,0,qt.created_at))) as s2c_date,max(qal.action_name) as action
                        from pe_queue_queue.q_task qt
                        inner join pe_queue_queue.q_task_log qtl on qt.id = qtl.task_id and qtl.action_at > '2018-01-18'
                        left join pe_queue_queue.q_action_log qal on qtl.task_id = qal.task_id and qtl.status = 2 
                        where qt.type = 4
                        group by 1,2)a
              left join (
              			select task_id,next_task
                        from
                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
                                		lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
                                from pe_queue_queue.q_task qt
                                where qt.type in (4,5)
                                )
                        where is_soh = 5 and type = 4
                        )b on a.task_id = b.task_id and a.action = 'hold'
              left join pe_queue_queue.q_action_log qal on b.next_task = qal.task_id
              group by 1,2,3,4,5,6)c 
      left join pe_queue_queue.q_action_log qal on c.last_hold_task = qal.id
      )d
left join pe_pe2_pe2.subscription s1 ON d.entity_id = s1.id
left join pe_pe2_pe2.subscription_origin_info soi ON s1.id=soi.subscription_id
left join pe_pe2_pe2.subscription s2 ON soi.root_subscription_id=s2.id
where s2.created_at>='2019-04-01'
ORDER BY s2c_at;


----- FINAL query for a customer's subscription (root - highest parent) and conversion in subsequent cycles

SELECT COUNT(DISTINCT root_subscription_id) AS unique_root_subscriptions,
		COUNT(CASE WHEN ranking=1 THEN root_subscription_id END) AS total_in_first_s2c,
		COUNT(CASE WHEN ranking=2 THEN root_subscription_id END) AS total_in_second_s2c,
		COUNT(CASE WHEN ranking=3 THEN root_subscription_id END) AS total_in_third_s2c,
		COUNT(CASE WHEN ranking=4 THEN root_subscription_id END) AS total_in_fourth_s2c,
		COUNT(CASE WHEN ranking=5 THEN root_subscription_id END) AS total_in_fifth_s2c,
		COUNT(CASE WHEN ranking=1 AND final_action IN ('accept', 'change-ndd-with-order', 'move-to-doctor-program', 'clone') THEN root_subscription_id END) AS conversions_in_first_s2c,
		COUNT(CASE WHEN ranking=2 AND final_action IN ('accept', 'change-ndd-with-order', 'move-to-doctor-program', 'clone') THEN root_subscription_id END) AS conversions_in_second_s2c,
		COUNT(CASE WHEN ranking=3 AND final_action IN ('accept', 'change-ndd-with-order', 'move-to-doctor-program', 'clone') THEN root_subscription_id END) AS conversions_in_third_s2c,
		COUNT(CASE WHEN ranking=4 AND final_action IN ('accept', 'change-ndd-with-order', 'move-to-doctor-program', 'clone') THEN root_subscription_id END) AS conversions_in_fourth_s2c,
		COUNT(CASE WHEN ranking=5 AND final_action IN ('accept', 'change-ndd-with-order', 'move-to-doctor-program', 'clone') THEN root_subscription_id END) AS conversions_in_fifth_s2c
FROM (
		SELECT s1.customer_id, s2.id AS root_subscription_id, s1.id AS subcription_id,
				s2.created_at AS root_subscription_start_date, s1.created_at AS current_subscription_start_date,
				case when s1.interval_value is null then 30 else s1.interval_value end as cycle_length,
				d.*, ROW_NUMBER() OVER (PARTITION BY s2.id ORDER BY s2c_at) AS ranking
		from (
			  select c.*, 
		      case when c.action = 'hold' then c.next_task else c.task_id end as final_task,
		      case when c.action = 'hold' then qal.action_name else c.action end as final_action
		      from
		              (select a.*,b.next_task,max(qal.id) as last_hold_task
		              from
		                        (select qt.entity_id,qtl.task_id, min(dateadd(min,0,qt.created_at)) as s2c_at,min(date(dateadd(min,0,qt.created_at))) as s2c_date,max(qal.action_name) as action
		                        from pe_queue_queue.q_task qt
		                        inner join pe_queue_queue.q_task_log qtl on qt.id = qtl.task_id and qtl.action_at > '2018-01-18'
		                        left join pe_queue_queue.q_action_log qal on qtl.task_id = qal.task_id and qtl.status = 2 
		                        where qt.type = 4
		                        group by 1,2)a
		              left join (
		              			select task_id,next_task
		                        from
		                                (select qt.entity_id,qt.id as task_id,qt.type, lead(qt.id,1) over (partition by qt.entity_id order by qt.id) as next_task, 
		                                		lead(qt.type,1) over (partition by qt.entity_id order by qt.id) as is_soh
		                                from pe_queue_queue.q_task qt
		                                where qt.type in (4,5)
		                                )
		                        where is_soh = 5 and type = 4
		                        )b on a.task_id = b.task_id and a.action = 'hold'
		              left join pe_queue_queue.q_action_log qal on b.next_task = qal.task_id
		              group by 1,2,3,4,5,6)c 
		      left join pe_queue_queue.q_action_log qal on c.last_hold_task = qal.id
		      )d                                                               ---- all subscriptions on the queue
		left join pe_pe2_pe2.subscription s1 ON d.entity_id = s1.id            ---- joining to retrieve root for each subscription on queue
		left join pe_pe2_pe2.subscription_origin_info soi ON s1.id=soi.subscription_id   
		left join pe_pe2_pe2.subscription s2 ON soi.root_subscription_id=s2.id
		where s2.created_at>='2019-01-01'
)--ORDER BY s2c_at;


---- conditions inferring success
If([final_action] IN ('accept', 'change-ndd-with-order', 'move-to-doctor-program', 'clone')
