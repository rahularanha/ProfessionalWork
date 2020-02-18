select order_year,order_month,order_day,state,supplier_city,demand_city,courier_city_flag,sub_source,order_source,
count(distinct order_id) as total_orders,
count(distinct case when status in (9,10) then order_id end) as delivered_orders,
count(distinct case when (rank > 1 or clone = 1) then order_id end) as refill_orders,
count(distinct case when (rank > 1 or clone = 1) and status in (9,10) then order_id end) as refill_delivered_orders,
count(distinct case when (rank > 1 or clone = 1) and is_rejected = true then order_id end) as refill_rejected_orders,
count(distinct case when (rank > 1 or clone = 1) and is_cancelled = true then order_id end) as refill_cancelled_orders,
count(distinct case when (rank > 1 or clone = 1) and status in (9,10) and delivery_date = 'on time' then order_id end) as refill_on_time_delivered_orders,
count(distinct case when (rank > 1 or clone = 1) and status in (9,10) and delivery_date = 'before time' then order_id end) as refill_before_time_delivered_orders,
count(distinct case when (rank > 1 or clone = 1) and status in (9,10) and delivery_date = 'after time' then order_id end) as refill_after_time_delivered_orders,
sum(case when (rank > 1 or clone = 1) and status in (9,10) then feedback end) as total_feedback_of_refill_delivered_orders, 
count(case when (rank > 1 or clone = 1) and status in (9,10) then feedback end) as number_of_feedback_of_refill_delivered_orders,
sum(case when (rank > 1 or clone = 1) and status in (9,10) then gmv end) as refill_orders_gmv,
sum(case when (rank > 1 or clone = 1) and status in (9,10) then discounted_value end) as refill_orders_discounted_value,
sum(case when status in (9,10) then gmv end) as sub_orders_gmv,
sum(case when status in (9,10) then discounted_value end) as sub_orders_discounted_value,
COUNT(distinct case when (rank > 1 or clone = 1) and status in (9,10) then order_id end) as total_refill_fulf_orders,
COUNT(distinct case when status in (9,10) then order_id end) as total_sub_fulf_orders,
count(distinct customer_id) as total_customers,
count(distinct case when status in (9,10) then customer_id end) as customers_with_delivered_orders,
count(distinct case when (rank > 1 or clone = 1) then customer_id end) as customers_with_refill_orders,
count(distinct case when (rank > 1 or clone = 1) and status in (9,10) then customer_id end) as customers_with_refill_delivered_orders
from
        (select case when soi.subscription_id is null then osi.subscription_id else soi.root_subscription_id end as subscription_id, osi.order_id,foc.customer_id, osi.rank,osi.order_source,
        datepart('d',dateadd(min,0,fo.order_placed_at)) as order_day,
        datepart('mon',dateadd(min,0,fo.order_placed_at)) as order_month,
        datepart('year',dateadd(min,0,fo.order_placed_at)) as order_year,
        date(dateadd(min,0,fo.order_placed_at)) as order_date,
        fo.delivery_city_name as demand_city, fo.supplier_city_name as supplier_city, 
        CASE WHEN fo.is_courier=true THEN 1 ELSE 0 END AS courier_city_flag,
        fo.delivery_state as state, foc.rating as feedback, foc.mrp as gmv, foc.discounted_mrp AS discounted_value,
		CASE WHEN foc.cancellation_rejection_bucket='Rejected' THEN true ELSE false END AS is_rejected,
		CASE WHEN foc.cancellation_rejection_bucket='Cancelled' THEN true ELSE false END AS is_cancelled,
        case when soi.subscription_id is null then
        case s.source
        when 0 then 'CMS'
        when 1 then 'CONSUMER_APP'
        when 2 then 'POTENTIAL_SUBSCRIPTION_APP'
        when 3 then 'POTENTIAL_SUBSCRIPTION_CALLER'
        when 4 then 'Order on call'
        else null end 
        else 
        case s1.source
        when 0 then 'CMS'
        when 1 then 'CONSUMER_APP'
        when 2 then 'POTENTIAL_SUBSCRIPTION_APP'
        when 3 then 'POTENTIAL_SUBSCRIPTION_CALLER'
        when 4 then 'Order on call'
        else null end end as sub_source,
        case when fo.original_order_edd = fo.final_order_edd then 'on time'
        when fo.original_order_edd > fo.final_order_edd then 'before time'
        else 'after time' end as delivery_date,
        fo.order_status_id AS status, case when soi.subscription_id is not null then 1 else 0 end as clone
        from (select osi.subscription_id, osi.order_id,
				        case osi.source
				        when 1 then 'STC'
				        when 2 then 'PUSH'
				        when 3 then 'NDD_UPDATE'
				        when 4 then 'OLD_APP' 
				        when 5 then 'CREATION_PAGE'
				        when 6 then 'TEMPLATE_ORDER'
				        when 7 then 'NDD_UPDATE_CMS_STATUS_HOLD' 
				        when 8 then 'NDD_UPDATE_CMS_STATUS_ACTIVE' 
				        when 9 then 'NDD_UPDATE_APP_STATUS_HOLD'
				        when 10 then 'NDD_UPDATE_APP_STATUS_ACTIVE' 
				        when 11 then 'PUSH_ON_ACTIVE' 
				        when 12 then 'PUSH_ON_HOLD' 
				        when 13 then 'NDD_UPDATE_STC' end as order_source,
				         rank() over (partition by osi.subscription_id order by osi.order_id) 
		        from pe2.order_subscription_info osi
				)osi
		INNER JOIN data_model.f_order fo ON osi.order_id=fo.order_id
		INNER JOIN data_model.f_order_consumer foc ON fo.order_id=foc.order_id
        left join pe2.subscription s on s.id = osi.subscription_id
        left join pe2.subscription_origin_info soi on s.id = soi.subscription_id
        left join pe2.subscription s1 on s1.id = soi.root_subscription_id
        where dateadd(min,0,fo.order_placed_at) > '2017-12-01')
group by 1,2,3,4,5,6,7,8,9
