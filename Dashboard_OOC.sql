
---- Skull Final

select a.*,
		count(mn.medicine_name) as no_of_meds_digitized, 
		count(case when mn.is_deleted = false then mn.ucode end) as no_of_meds_delivered, 
		count(case when d.is_chronic = true then mn.ucode end) as no_of_chronic_meds_digitized,
		count(case when (mn.is_deleted = false) and (d.is_chronic = true) then mn.ucode end) as no_of_chronic_meds_delivered
from (
        select o.order_id, o.customer_id, o.order_placed_at, date(o.order_placed_at) as order_date,
        o.mrp AS order_value,fo.delivery_city_name AS delivery_city,fo.supplier_city_name as supplier_city,
        CASE WHEN fdpo.order_id IS NOT NULL THEN 1 ELSE 0 END AS doctor_program_flag,
         CASE WHEN os.placement_reason=1 THEN 'Normal Order'
			  WHEN os.placement_reason=2 THEN 'Replacement Order'
			  WHEN os.placement_reason=3 THEN 'Order Modification Requested'
			  WHEN os.placement_reason=4 THEN 'Health Savers'
			  WHEN os.placement_reason=5 THEN 'Retention - cancelled order'
			  WHEN os.placement_reason=6 THEN 'Retention - App drop offs'
			  WHEN os.placement_reason=7 THEN 'Quantity capping'
			  WHEN os.placement_reason=8 THEN 'Test Order'
		 END AS order_placement_reason,
        u.username as agent, ur2.name AS placed_by_user_role,
        CASE WHEN o.order_status_id in (9,10) then 'Fulfilled'
       		 WHEN o.order_status_id=8 THEN 'Cancelled'
        	 WHEN o.order_status_id=2 THEN 'Rejected'
        	 ELSE 'Under Process' 
        end as order_status,
        cr1.name as final_cancel_reason, u1.username AS final_cancelled_by, ur.name AS cancelled_by_user_role,
        case when cu.customer_source = 'order-on-call' then 1 else 0 end as customer_acquired_through_order_on_call,
        case when fdpo.order_id is not null AND (rx.order_id IS NULL) then 'Chose DP'
        	 when rx.order_id is not null then 'Rx SMS/E-Mail option chosen'
        	 when fo.is_rx_required=false then 'All non RX Meds'
        	 when i.order_id is not null then 'Rx attached from previous'
        	 --else 'Rx attached from previous' 
        end as type_of_order,
        CASE when fdpo.latest_case_status=4 then 1 else 0 end as dp_rx_prescriped_flag,
        CASE when cc_canned_dp.order_id is not null then 1 else 0 end as cc_canned_dp_flag,
        CASE when osi.order_id is not null then 1 else 0 end as opted_for_refill,
        upload_rx.next_status AS upload_rx_next_status, upload_rx.next_status_at AS upload_rx_next_status_at, 
        upload_rx.next_status2 AS upload_rx_next_status2, upload_rx.next_status2_at AS upload_rx_next_status_at,
        CASE WHEN rx.order_id IS NOT NULL THEN  
	        CASE when upload_rx.next_status = 3 then 'upload RX'
	       		 WHEN (upload_rx.next_status = 49) THEN 'Move to DP'
	        	 when upload_rx.next_status in (8) then 'Order Cancelled'
	        	 when upload_rx.next_status in (2) then 'Order Rejected'
	        END 
	    END as uploaded_rx, 
        CASE when rx.order_id is not null THEN rx.link_sent_at end as link_sent_at,
        case when upload_rx.next_status = 3 then upload_rx.next_status_at end as upload_rx_at,
        CASE WHEN (upload_rx.next_status = 3)
        		THEN 
        			CASE WHEN (upload_rx.next_status_at - rx.link_sent_at)<='00:15:00' THEN 'Within 15 mins'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)<='01:00:00' THEN '15mins-1hour'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)<='02:00:00' THEN '1hours-2hours'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)<='03:00:00' THEN '2hours-3hours'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)>'03:00:00' THEN '>3hours'
    				END
		END AS upload_timing_bins,
        cr.name as cancel_reason, 
        case ocrh.action when 1 then 'Reassign'
        				 when 2 then 'Reject'
        				 when 3 then 'Move to DP' 
        end as cnr_action,
        o.user_type_monthly AS new_customer_flag
        from data_model.f_order_consumer o
        LEFT JOIN data_model.f_order fo ON o.order_id=fo.order_id
        LEFT JOIN data_model.f_doctor_program_order fdpo ON o.order_id=fdpo.order_id
        left join pe_pe2_pe2.order_cancel_reason ocr ON o.order_id = ocr.order_id
        left join pe_pe2_pe2.cancel_reason cr1 ON ocr.cancel_reason_id = cr1.id
        LEFT JOIN pe_pe2_pe2."user" u1 ON ocr.user_id=u1.id
        LEFT JOIN pe_pe2_pe2.user_roles ur ON u1.role_id=ur.id
        left join pe_pe2_pe2.customer cu ON o.customer_id = cu.id
        left join (select order_id, max(DATEADD(MIN,0,sent_at)) AS link_sent_at 
        			from pe_pe2_pe2.order_provide_rx_later_log 
        			GROUP BY 1
        			) rx ON o.order_id = rx.order_id
        left join (SELECT order_id, min(action_time) 
        			FROM pe_pe2_pe2.order_cancel_reason_history 
        			WHERE "action"=3 
        			GROUP BY 1
        			) cc_canned_dp ON o.order_id=cc_canned_dp.order_id
        left join (select distinct o.id AS order_id 
        			FROM pe_pe2_pe2."order" o
        			LEFT JOIN pe_pe2_pe2.order_image oi ON o.id=oi.order_id AND oi.is_duplicate=0 AND oi.is_valid=1
					LEFT JOIN pe_pe2_pe2.image i ON oi.image_id=i.id AND o.time_stamp>i.create_time
        			where i.id IS NOT NULL
        		  	) i on o.order_id = i.order_id
        left join (select oh.id, oh.order_id, oh.order_status,lead(oh.order_status,1) over (partition by oh.order_id order by oh.id) as next_status, 
                  DATEADD(MIN,0,lead(oh."timestamp",1) over (partition by oh.order_id order by oh.id)) as next_status_at, 
                  CASE WHEN oh.order_status=47 THEN rank() over (partition by oh.order_id,oh.order_status=47 order by oh.id DESC) END AS ranking,
                  lead(oh.order_status,2) over (partition by oh.order_id order by oh.id) as next_status2,
                  DATEADD(MIN,0,lead(oh."timestamp",2) over (partition by oh.order_id order by oh.id)) as next_status2_at
                  from pe_pe2_pe2.order_history oh
                  where oh."timestamp" > '2018-02-12'
                  ) upload_rx on o.order_id = upload_rx.order_id and upload_rx.order_status = 47 AND ranking=1
        left join (select a.order_id, ocrh.action, ocrh.cancel_reason_id
                   from
                          (select order_id, min(id) as id
                          from pe_pe2_pe2.order_cancel_reason_history 
                          where created_at > '2018-02-12'
                          group by 1)a
                   left join pe_pe2_pe2.order_cancel_reason_history ocrh
                   on a.id = ocrh.id
                  ) ocrh on upload_rx.next_status in (3,49) and upload_rx.next_status2=4 and o.order_id=ocrh.order_id
        left join pe_pe2_pe2.cancel_reason cr on ocrh.cancel_reason_id = cr.id
        left join (select distinct order_id 
        			from pe_pe2_pe2.order_subscription_info 
        			where created_at > '2018-02-12'
        			) osi on o.order_id = osi.order_id
        left join pe_pe2_pe2.order_source_info os on o.order_id = os.order_id
        left join pe_pe2_pe2."user" u on os.user_id = u.id
        LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u.role_id=ur2.id
        where lower(o.order_source) = 'order_on_call' and u.username not in ('testoocagent2','testoocagent')
    ) a 
left join pe_pe2_pe2.medicine_notes mn on a.order_id = mn.order_id
left join pe_pe2_pe2.medicine_disease_lookup mdl on mn.ucode = mdl.medicine_ucode
left join pe_pe2_pe2.diseases d on mdl.disease_id = d.id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31;



-- Skull Aggregated Overall  
   
	-- add CnR order stage and timestamp to understand where and why the drop out is happening - customer behaviour

select order_placed_date, 
		order_status,
		--delivery_pincode,
		delivery_city,
		supplier_city,
		--'India' AS delivery_country,
		payment_mode_at_placement,
		payment_mode_at_delivery,
		courier_flag,
		chronic_order_flag,
		new_customer_flag,
		doctor_program_flag,
		dp_rx_prescriped_flag,
		opted_for_refill,
		order_placement_reason,
		placed_by_agent,
		--placed_by_user_role,
		final_cancel_reason,
		--final_cancelled_by,
		--cancelled_by_user_role,
		customer_acquired_through_order_on_call,
		upload_rx_next_status,
		--upload_rx_next_status2,
		type_of_order,
		uploaded_rx,
		upload_timing_bins,
		COUNT(order_id) AS order_count,
		COUNT(customer_id) AS customer_count,
		COUNT(DISTINCT customer_id) AS unique_customer_count,
		SUM(order_value) AS gmv,
		SUM(no_of_meds_digitized) AS total_no_of_meds_digitized,
		SUM(no_of_meds_delivered) AS total_no_of_meds_delivered,
		SUM(no_of_chronic_meds_digitized) AS total_no_of_chronic_meds_digitized,
		SUM(no_of_chronic_meds_delivered) AS total_no_of_chronic_meds_delivered
from (
        select o.order_id, o.customer_id, o.order_placed_at, date(o.order_placed_at) as order_placed_date, 
        o.mrp AS order_value,fo.delivery_city_name AS delivery_city,fo.supplier_city_name as supplier_city,
        CASE WHEN o.user_type_monthly='Old User' THEN 'Old User'
        										 ELSE 'New User'
        END AS new_customer_flag, o.chronic_flag_old AS chronic_order_flag, fo.is_courier::INT AS courier_flag,
        CASE opm.customer_payment_mode_id WHEN 1 THEN 'COD'
	        							  WHEN 2 THEN 'Card At Delivery'
	        							  WHEN 3 THEN 'Pay Online'
	        							  WHEN 4 THEN 'PE Wallet'
        END AS payment_mode_at_placement, 
        CASE opm.delivery_payment_mode_id WHEN 1 THEN 'COD'
        								  WHEN 2 THEN 'Card At Delivery'
        								  WHEN 3 THEN 'Pay Online'
        								  WHEN 4 THEN 'PE Wallet'
        END AS payment_mode_at_delivery,
        CASE WHEN fdpo.order_id IS NOT NULL THEN 1 ELSE 0 END AS doctor_program_flag,
        CASE when fdpo.latest_case_status=4 then 1 else 0 end as dp_rx_prescriped_flag,
         CASE WHEN os.placement_reason=1 THEN 'Normal Order'
			  WHEN os.placement_reason=2 THEN 'Replacement Order'
			  WHEN os.placement_reason=3 THEN 'Order Modification Requested'
			  WHEN os.placement_reason=4 THEN 'Health Savers'
			  WHEN os.placement_reason=5 THEN 'Retention - cancelled order'
			  WHEN os.placement_reason=6 THEN 'Retention - App drop offs'
			  WHEN os.placement_reason=7 THEN 'Quantity capping'
			  WHEN os.placement_reason=8 THEN 'Test Order'
		 END AS order_placement_reason,
        u1.username as placed_by_agent, ur1.name AS placed_by_user_role,
        CASE WHEN o.order_status_id in (9,10) then 'Fulfilled'
       		 WHEN o.order_status_id=8 THEN 'Cancelled'
        	 WHEN o.order_status_id=2 THEN 'Rejected'
        	 ELSE 'Under Process' 
        end as order_status,
        cr1.name as final_cancel_reason, u2.username AS final_cancelled_by, ur2.name AS cancelled_by_user_role,
        case when cu.customer_source = 'order-on-call' then 1 else 0 end as customer_acquired_through_order_on_call,
        CASE when osi.order_id is not null then 1 else 0 end as opted_for_refill,
        upload_rx.next_status AS upload_rx_next_status, upload_rx.next_status_at AS upload_rx_next_status_at, 
        upload_rx.next_status2 AS upload_rx_next_status2, upload_rx.next_status2_at AS upload_rx_next_status2_at,
        case when fo.is_rx_required=false then 'All non RX Meds'
        	 when rx.order_id is not null then 'Cx will upload'
        	 when fdpo.order_id is not null AND (rx.order_id IS NULL) then 'Chose DP'
        	 when i.order_id is not null then 'Rx prefetch'
        	 --else 'Rx attached from previous' 
        end as type_of_order,
        CASE WHEN rx.order_id IS NOT NULL THEN  
	        CASE when upload_rx.next_status = 3 then 'upload RX'
	       		 WHEN (upload_rx.next_status = 49) THEN 'Moved to DP'
	        	 when upload_rx.next_status in (8) then 'Order Cancelled'
	        	 when upload_rx.next_status in (2) then 'Order Rejected'
	        END 
	    END as uploaded_rx, 
        CASE when rx.order_id is not null THEN rx.link_sent_at end as link_sent_at,
        case when upload_rx.next_status = 3 then upload_rx.next_status_at end as upload_rx_at,
        CASE WHEN (upload_rx.next_status = 3)
        		THEN 
        			CASE WHEN (upload_rx.next_status_at - rx.link_sent_at)<='00:15:00' THEN 'Within 15 mins'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)<='01:00:00' THEN '15mins-1hour'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)<='02:00:00' THEN '1hours-2hours'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)<='03:00:00' THEN '2hours-3hours'
        				 WHEN (upload_rx.next_status_at - rx.link_sent_at)>'03:00:00' THEN '>3hours'
    				END
		END AS upload_timing_bins,
        count(mn.medicine_name) as no_of_meds_digitized, 
		count(case when mn.is_deleted = false then mn.ucode end) as no_of_meds_delivered, 
		count(case when d.is_chronic = true then mn.ucode end) as no_of_chronic_meds_digitized,
		count(case when (mn.is_deleted = false) and (d.is_chronic = true) then mn.ucode end) as no_of_chronic_meds_delivered
        FROM data_model.f_order_consumer o
        LEFT JOIN data_model.f_order fo ON o.order_id=fo.order_id
        LEFT JOIN data_model.f_doctor_program_order fdpo ON o.order_id=fdpo.order_id
        left join pe_pe2_pe2.customer cu ON o.customer_id = cu.id
        left join pe_pe2_pe2.order_source_info os on o.order_id = os.order_id
        LEFT JOIN pe_pe2_pe2.order_payment_mode opm ON o.order_id=opm.order_id
        left join pe_pe2_pe2."user" u1 on os.user_id = u1.id
        LEFT JOIN pe_pe2_pe2.user_roles ur1 ON u1.role_id=ur1.id
        left join pe_pe2_pe2.order_cancel_reason ocr ON o.order_id = ocr.order_id
        left join pe_pe2_pe2.cancel_reason cr1 ON ocr.cancel_reason_id = cr1.id
        LEFT JOIN pe_pe2_pe2."user" u2 ON ocr.user_id=u2.id
        LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u2.role_id=ur2.id
        left join (select order_id, max(DATEADD(MIN,0,sent_at)) AS link_sent_at 
        			from pe_pe2_pe2.order_provide_rx_later_log 
        			GROUP BY 1
        			) rx ON o.order_id = rx.order_id
        left join (select distinct o.id AS order_id 
        			FROM pe_pe2_pe2."order" o
        			LEFT JOIN pe_pe2_pe2.order_image oi ON o.id=oi.order_id AND oi.is_duplicate=0 AND oi.is_valid=1
					LEFT JOIN pe_pe2_pe2.image i ON oi.image_id=i.id AND o.time_stamp>i.create_time
        			where i.id IS NOT NULL
        		  	) i on o.order_id = i.order_id
        left join (select oh.id, oh.order_id, oh.order_status,lead(oh.order_status,1) over (partition by oh.order_id order by oh.id) as next_status, 
                  DATEADD(MIN,0,lead(oh."timestamp",1) over (partition by oh.order_id order by oh.id)) as next_status_at, 
                  CASE WHEN oh.order_status=47 THEN rank() over (partition by oh.order_id,oh.order_status=47 order by oh.id DESC) END AS ranking,
                  lead(oh.order_status,2) over (partition by oh.order_id order by oh.id) as next_status2,
                  DATEADD(MIN,0,lead(oh."timestamp",2) over (partition by oh.order_id order by oh.id)) as next_status2_at
                  from pe_pe2_pe2.order_history oh
                  where oh."timestamp" > '2018-02-12'
                  ) upload_rx on o.order_id = upload_rx.order_id and upload_rx.order_status = 47 AND ranking=1
        left join (select distinct order_id 
        			from pe_pe2_pe2.order_subscription_info 
        			where created_at > '2018-02-12'
        			) osi on o.order_id = osi.order_id
        left join pe_pe2_pe2.medicine_notes mn on o.order_id = mn.order_id
		left join pe_pe2_pe2.medicine_disease_lookup mdl on mn.ucode = mdl.medicine_ucode
		left join pe_pe2_pe2.diseases d on mdl.disease_id = d.id
        where lower(o.order_source) = 'order_on_call' and u1.username not in ('testoocagent2','testoocagent')
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
    )
WHERE order_placed_date<CURRENT_DATE    
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;



---- OOC Order Stage Trend
CREATE TEMPORARY TABLE ost AS
SELECT 'Orders Placed' AS category, DATE(foc1.order_placed_at) AS date_of_inspection, COUNT(foc1.order_id)
FROM data_model.f_order_consumer foc1
WHERE lower(foc1.order_source) = 'order_on_call' AND (DATE(foc1.order_placed_at) BETWEEN '2018-03-01' AND (CURRENT_DATE-1))
GROUP BY 1,2
UNION
SELECT CASE WHEN order_status_id=2 THEN 'Orders Rejected'
			WHEN order_status_id=8 THEN 'Orders Cancelled'
		END AS category, 
		DATE(foc2.order_placed_at) AS date_of_inspection, COUNT(foc2.order_id)
FROM data_model.f_order_consumer foc2
WHERE foc2.order_status_id IN (2,8) AND lower(foc2.order_source) = 'order_on_call' AND (DATE(foc2.order_placed_at) BETWEEN '2018-03-01' AND (CURRENT_DATE-1))
GROUP BY 1,2
UNION
SELECT 'Orders Delivered' AS category, DATE(foc3.order_placed_at) AS date_of_inspection, COUNT(foc3.order_id)
FROM data_model.f_order_consumer foc3
WHERE foc3.order_status_id IN (9,10) AND lower(foc3.order_source) = 'order_on_call' AND (DATE(foc3.order_placed_at) BETWEEN '2018-03-01' AND (CURRENT_DATE-1))
GROUP BY 1,2
UNION
SELECT 'Customers Registered' AS category, DATE(DATEADD(MIN,330,cu.dateadded)) AS date_of_inspection, COUNT(cu.id)
FROM pe_pe2_pe2.customer cu
WHERE cu.customer_source = 'order-on-call' AND DATE(DATEADD(MIN,330,cu.dateadded)) BETWEEN '2018-03-01' AND (CURRENT_DATE-1)
GROUP BY 1,2
--ORDER BY 2 DESC,1
UNION
SELECT 'Customers Registered and Placed' AS category, DATE(DATEADD(MIN,330,cu.dateadded)) AS date_of_inspection, COUNT(foc4.customer_id)
FROM pe_pe2_pe2.customer cu
LEFT JOIN ( SELECT customer_id, order_id, order_placed_at, order_source, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_id) AS ranking
			FROM data_model.f_order_consumer foc
			--WHERE (DATE(foc1.order_placed_at) BETWEEN '2018-03-01' AND (CURRENT_DATE-1)) --lower(foc1.order_source) = 'order_on_call' AND 
			) foc4 ON cu.id=foc4.customer_id AND foc4.ranking=1 AND lower(foc4.order_source) = 'order_on_call'
WHERE cu.customer_source = 'order-on-call' AND DATE(DATEADD(MIN,330,cu.dateadded)) BETWEEN '2018-03-01' AND (CURRENT_DATE-1)
GROUP BY 1,2
UNION
SELECT 'Customers Registered and Placed Immediately' AS category, DATE(DATEADD(MIN,330,cu.dateadded)) AS date_of_inspection, 
		COUNT(CASE WHEN DATEDIFF(MIN,DATEADD(MIN,330,cu.dateadded),foc5.order_placed_at)<=60 THEN cu.id END)
FROM pe_pe2_pe2.customer cu
LEFT JOIN ( SELECT customer_id, order_id, order_placed_at, order_source, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_id) AS ranking
			FROM data_model.f_order_consumer foc
			--WHERE (DATE(foc1.order_placed_at) BETWEEN '2018-03-01' AND (CURRENT_DATE-1)) --lower(foc1.order_source) = 'order_on_call' AND 
			) foc5 ON cu.id=foc5.customer_id AND foc5.ranking=1 AND lower(foc5.order_source) = 'order_on_call'
WHERE cu.customer_source = 'order-on-call' AND DATE(DATEADD(MIN,330,cu.dateadded)) BETWEEN '2018-03-01' AND (CURRENT_DATE-1)
GROUP BY 1,2
ORDER BY 2 DESC,1
;
---- observation of above query
SELECT EXTRACT(YEAR FROM date_of_inspection) AS yoi,EXTRACT(MONTH FROM date_of_inspection) AS yoi, 
		SUM(CASE WHEN category='Orders Placed' THEN "count" END) AS Orders_Placed,
		SUM(CASE WHEN category='Orders Delivered' THEN "count" END) AS Orders_Delivered,
		SUM(CASE WHEN category='Customers Registered' THEN "count" END) AS Customers_Registered,
		SUM(CASE WHEN category='Customers Registered and placed' THEN "count" END) AS Customers_Registered_and_placed,
		SUM(CASE WHEN category='Customers Registered and placed immediately' THEN "count" END) AS Customers_Registered_and_placed_immediately
FROM ost GROUP BY 1,2 ORDER BY 1,2


--- Customer Behavior OOC GeoMap

SELECT DATE(foc.order_placed_at) AS order_placed_date, 
		fo.delivery_pincode,
		fo.delivery_city_name AS delivery_city,
		'India' AS delivery_country,
		COUNT(CASE WHEN lower(foc.order_source) = 'order_on_call' THEN foc.order_id END) AS ooc_orders_placed,
		COUNT(CASE WHEN lower(foc.order_source) NOT IN ('order_on_call') THEN foc.order_id END) AS cp_orders_placed,
		COUNT(CASE WHEN lower(foc.order_source) = 'order_on_call' AND fo.order_status_id IN (9,10) THEN foc.order_id END) AS ooc_orders_fulfilled,
		COUNT(CASE WHEN lower(foc.order_source) NOT IN ('order_on_call') AND fo.order_status_id IN (9,10) THEN foc.order_id END) AS cp_orders_fulfilled
FROM data_model.f_order_consumer foc
LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
WHERE (DATE(foc.order_placed_at) BETWEEN '2018-02-13' AND (CURRENT_DATE-1)) AND 
		foc.customer_id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)
GROUP BY 1,2,3,4;



---- Customer Behavior CP - OOC Platform Migration on Order Placement

SELECT customer_id, acquired_through_ooc, customer_registered_at, 
		COUNT(order_id) AS order_placed,
		SUM(order_fulfilled_flag) AS orders_fulfilled,
		MAX(CASE WHEN ooc_order_number=1 THEN 1 ELSE 0 END) AS placed_an_ooc_order,
		MIN(CASE WHEN order_number=1 THEN order_placed_at END) AS first_order_placed_at,
		MIN(CASE WHEN order_number_last=1 THEN order_placed_at END) AS last_order_placed_at,
		MIN(CASE WHEN order_number=1 THEN order_source END) AS first_order_placed_source,
		MIN(CASE WHEN order_number=2 THEN order_source END) AS second_order_placed_source,
		MIN(CASE WHEN order_number=3 THEN order_source END) AS third_order_placed_source,
		MIN(CASE WHEN order_number=4 THEN order_source END) AS fourth_order_placed_source,
		MIN(CASE WHEN order_number=5 THEN order_source END) AS fifth_order_placed_source,
		MIN(CASE WHEN order_number=6 THEN order_source END) AS sixth_order_placed_source,
		MIN(CASE WHEN ooc_order_number=1 THEN order_number END) AS order_number_for_first_ooc,
		MIN(CASE WHEN ooc_order_number=1 THEN order_placed_at END) AS ooc_first_order_placed_at,
		MIN(CASE WHEN ooc_order_number=1 THEN order_source END) AS ooc_first_order_placed_source,
		MIN(CASE WHEN ooc_order_number=2 THEN order_source END) AS ooc_second_order_placed_source,
		MIN(CASE WHEN ooc_order_number=3 THEN order_source END) AS ooc_third_order_placed_source,
		MIN(CASE WHEN ooc_order_number=4 THEN order_source END) AS ooc_fourth_order_placed_source,
		MIN(CASE WHEN ooc_order_number=5 THEN order_source END) AS ooc_fifth_order_placed_source,
		MIN(CASE WHEN ooc_order_number=6 THEN order_source END) AS ooc_sixth_order_placed_source	
FROM (
	SELECT foc.customer_id, DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, 
			CASE WHEN csrd.customer_outlier_flag='Outlier' THEN 1 ELSE 0 END AS customer_outlier_flag,
			foc.order_id, foc.order_placed_at, foc.order_source AS platform,
			CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
			CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc,
			ROW_NUMBER() OVER (PARTITION BY cu.id ORDER BY foc.order_id) AS order_number, 
			ROW_NUMBER() OVER (PARTITION BY cu.id ORDER BY foc.order_id DESC) AS order_number_last, 
			CASE WHEN foc.order_placed_at>=first_ooc_order_placed_at THEN (ROW_NUMBER() OVER (PARTITION BY cu.id,foc.order_placed_at>=first_ooc_order_placed_at ORDER BY foc.order_id)) END AS ooc_order_number,
			CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag
	FROM pe_pe2_pe2.customer cu
	INNER JOIN data_model.f_order_consumer foc ON cu.id=foc.customer_id
	LEFT JOIN data_model.customer_segmentation_raw_data csrd ON cu.id=csrd.customer_id
	LEFT JOIN (SELECT customer_id, MIN(order_placed_at) AS first_ooc_order_placed_at FROM data_model.f_order_consumer WHERE lower(order_source) = 'order_on_call' GROUP BY 1) AS first_ooc ON foc.customer_id=first_ooc.customer_id
	WHERE cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)
		--cu.id=3629969
)
WHERE DATE(customer_registered_at)>='2018-02-13'
GROUP BY 1,2,3
HAVING COUNT(order_id)>5
ORDER BY 3 DESC;



---- Customer Behavior CP - OOC Platform Migration on Order Fulfillment
		
SELECT customer_id, acquired_through_ooc, customer_registered_at, 
		MAX(CASE WHEN ooc_order_number=1 THEN 1 ELSE 0 END) AS placed_an_ooc_order,
		MIN(CASE WHEN order_number=1 THEN order_placed_at END) AS first_order_placed_at,
		MIN(CASE WHEN order_number_last=1 THEN order_placed_at END) AS last_order_placed_at,
		MIN(CASE WHEN order_number=1 THEN order_source END) AS first_order_placed_source,
		MIN(CASE WHEN order_number=2 THEN order_source END) AS second_order_placed_source,
		MIN(CASE WHEN order_number=3 THEN order_source END) AS third_order_placed_source,
		MIN(CASE WHEN order_number=4 THEN order_source END) AS fourth_order_placed_source,
		MIN(CASE WHEN order_number=5 THEN order_source END) AS fifth_order_placed_source,
		MIN(CASE WHEN order_number=6 THEN order_source END) AS sixth_order_placed_source,
		MIN(CASE WHEN ooc_order_number=1 THEN order_number END) AS order_number_for_first_ooc,
		MIN(CASE WHEN ooc_order_number=1 THEN order_placed_at END) AS ooc_first_order_placed_at,
		MIN(CASE WHEN ooc_order_number=1 THEN order_source END) AS ooc_first_order_placed_source,
		MIN(CASE WHEN ooc_order_number=2 THEN order_source END) AS ooc_second_order_placed_source,
		MIN(CASE WHEN ooc_order_number=3 THEN order_source END) AS ooc_third_order_placed_source,
		MIN(CASE WHEN ooc_order_number=4 THEN order_source END) AS ooc_fourth_order_placed_source,
		MIN(CASE WHEN ooc_order_number=5 THEN order_source END) AS ooc_fifth_order_placed_source,
		MIN(CASE WHEN ooc_order_number=6 THEN order_source END) AS ooc_sixth_order_placed_source,
		COUNT(order_id) AS orders_placed,
		SUM(order_fulfilled_flag) AS orders_fulfilled
FROM (
	SELECT foc.customer_id, DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, 
			CASE WHEN csrd.customer_outlier_flag='Outlier' THEN 1 ELSE 0 END AS customer_outlier_flag,
			foc.order_id, foc.order_placed_at, foc.order_source AS platform,
			CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
			CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc, 
			CASE WHEN foc.order_status_id IN (9,10) THEN ROW_NUMBER() OVER (PARTITION BY cu.id,foc.order_status_id IN (9,10) ORDER BY foc.order_id) END AS order_number, 
			CASE WHEN foc.order_status_id IN (9,10) THEN ROW_NUMBER() OVER (PARTITION BY cu.id,foc.order_status_id IN (9,10) ORDER BY foc.order_id DESC) END AS order_number_last, 
			CASE WHEN (foc.order_placed_at>=first_ooc_order_placed_at AND foc.order_status_id IN (9,10)) THEN (ROW_NUMBER() OVER (PARTITION BY cu.id,(foc.order_placed_at>=first_ooc_order_placed_at AND foc.order_status_id IN (9,10)) ORDER BY foc.order_id)) END AS ooc_order_number,
			CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag
	FROM pe_pe2_pe2.customer cu
	INNER JOIN data_model.f_order_consumer foc ON cu.id=foc.customer_id
	LEFT JOIN data_model.customer_segmentation_raw_data csrd ON cu.id=csrd.customer_id
	LEFT JOIN (SELECT customer_id, MIN(order_placed_at) AS first_ooc_order_placed_at FROM data_model.f_order_consumer WHERE lower(order_source) = 'order_on_call' GROUP BY 1) AS first_ooc ON foc.customer_id=first_ooc.customer_id
	WHERE cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)
		--cu.id=3629969
)
WHERE DATE(customer_registered_at)>='2018-02-13'
GROUP BY 1,2,3
HAVING SUM(order_fulfilled_flag)>=5
ORDER BY 3 DESC;



----- Final Platform Migration basis Customers placing atleast one OOC order and observing platform migration around the placement of first ooc order

SELECT customer_id, acquired_through_ooc, customer_registered_at, 
MAX(CASE WHEN ooc_order_number_placed=1 THEN 1 ELSE 0 END) AS placed_an_ooc_order,
MIN(CASE WHEN order_number_placed=1 THEN order_placed_at END) AS first_order_placed_at,
MIN(CASE WHEN order_number_last_placed=1 THEN order_placed_at END) AS last_order_placed_at,
MIN(CASE WHEN order_number_placed=1 THEN order_source END) AS first_order_placed_source,
MIN(CASE WHEN order_number_placed=2 THEN order_source END) AS second_order_placed_source,
MIN(CASE WHEN order_number_placed=3 THEN order_source END) AS third_order_placed_source,
MIN(CASE WHEN order_number_placed=4 THEN order_source END) AS fourth_order_placed_source,
MIN(CASE WHEN order_number_placed=5 THEN order_source END) AS fifth_order_placed_source,
MIN(CASE WHEN order_number_placed=6 THEN order_source END) AS sixth_order_placed_source,
MIN(CASE WHEN ooc_order_number_placed=1 THEN order_number END) AS order_number_for_first_ooc,
MIN(CASE WHEN ooc_order_number_placed=1 THEN order_placed_at END) AS ooc_first_order_placed_at,
MIN(CASE WHEN ooc_order_number_placed=1 THEN order_source END) AS ooc_first_order_placed_source,
MIN(CASE WHEN ooc_order_number_placed=2 THEN order_source END) AS ooc_second_order_placed_source,
MIN(CASE WHEN ooc_order_number_placed=3 THEN order_source END) AS ooc_third_order_placed_source,
MIN(CASE WHEN ooc_order_number_placed=4 THEN order_source END) AS ooc_fourth_order_placed_source,
MIN(CASE WHEN ooc_order_number_placed=5 THEN order_source END) AS ooc_fifth_order_placed_source,
MIN(CASE WHEN ooc_order_number_placed=6 THEN order_source END) AS ooc_sixth_order_placed_source,
MAX(CASE WHEN ooc_order_number=1 THEN 1 ELSE 0 END) AS fulfilled_an_ooc_order,
MIN(CASE WHEN order_number=1 THEN order_placed_at END) AS first_fulfilled_order_placed_at,
MIN(CASE WHEN order_number_last=1 THEN order_placed_at END) AS last_fulfilled_order_placed_at,
MIN(CASE WHEN order_number=1 THEN order_source END) AS first_order_fulfilled_source,
MIN(CASE WHEN order_number=2 THEN order_source END) AS second_order_fulfilled_source,
MIN(CASE WHEN order_number=3 THEN order_source END) AS third_order_fulfilled_source,
MIN(CASE WHEN order_number=4 THEN order_source END) AS fourth_order_fulfilled_source,
MIN(CASE WHEN order_number=5 THEN order_source END) AS fifth_order_fulfilled_source,
MIN(CASE WHEN order_number=6 THEN order_source END) AS sixth_order_fulfilled_source,
MIN(CASE WHEN ooc_order_number=1 THEN order_number END) AS order_number_for_first_fulfilled_ooc,
MIN(CASE WHEN ooc_order_number=1 THEN order_placed_at END) AS ooc_first_fulfilled_order_placed_at,
MIN(CASE WHEN ooc_order_number=1 THEN order_source END) AS ooc_first_order_fulfilled_source,
MIN(CASE WHEN ooc_order_number=2 THEN order_source END) AS ooc_second_order_fulfilled_source,
MIN(CASE WHEN ooc_order_number=3 THEN order_source END) AS ooc_third_order_fulfilled_source,
MIN(CASE WHEN ooc_order_number=4 THEN order_source END) AS ooc_fourth_order_fulfilled_source,
MIN(CASE WHEN ooc_order_number=5 THEN order_source END) AS ooc_fifth_order_fulfilled_source,
MIN(CASE WHEN ooc_order_number=6 THEN order_source END) AS ooc_sixth_order_fulfilled_source,
COUNT(order_id) AS orders_placed,
COUNT(CASE WHEN order_source='OOC' THEN order_id END) AS ooc_orders_placed,
SUM(order_fulfilled_flag) AS orders_fulfilled,
COUNT(CASE WHEN order_source='OOC' AND order_fulfilled_flag=1 THEN order_id END) AS ooc_orders_fulfilled
FROM (
SELECT foc.customer_id, DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, 
foc.order_id, foc.order_placed_at, foc.order_source AS platform,
CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc, 
ROW_NUMBER() OVER (PARTITION BY cu.id ORDER BY foc.order_id) AS order_number_placed, 
ROW_NUMBER() OVER (PARTITION BY cu.id ORDER BY foc.order_id DESC) AS order_number_last_placed, 
CASE WHEN foc.order_placed_at>=first_ooc_order_placed_at THEN (ROW_NUMBER() OVER (PARTITION BY cu.id,foc.order_placed_at>=first_ooc_order_placed_at ORDER BY foc.order_id)) END AS ooc_order_number_placed,
CASE WHEN foc.order_status_id IN (9,10) THEN ROW_NUMBER() OVER (PARTITION BY cu.id,foc.order_status_id IN (9,10) ORDER BY foc.order_id) END AS order_number, 
CASE WHEN foc.order_status_id IN (9,10) THEN ROW_NUMBER() OVER (PARTITION BY cu.id,foc.order_status_id IN (9,10) ORDER BY foc.order_id DESC) END AS order_number_last, 
CASE WHEN (foc.order_placed_at>=first_ooc_fulfilled_order_placed_at AND foc.order_status_id IN (9,10)) THEN (ROW_NUMBER() OVER (PARTITION BY cu.id,(foc.order_placed_at>=first_ooc_fulfilled_order_placed_at AND foc.order_status_id IN (9,10)) ORDER BY foc.order_id)) END AS ooc_order_number,
CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag
FROM pe_pe2_pe2.customer cu
INNER JOIN data_model.f_order_consumer foc ON cu.id=foc.customer_id
LEFT JOIN data_model.customer_segmentation_raw_data csrd ON cu.id=csrd.customer_id
LEFT JOIN (SELECT customer_id, MIN(order_placed_at) AS first_ooc_order_placed_at FROM data_model.f_order_consumer WHERE lower(order_source) = 'order_on_call' GROUP BY 1) AS first_ooc ON foc.customer_id=first_ooc.customer_id
LEFT JOIN (SELECT customer_id, MIN(order_placed_at) AS first_ooc_fulfilled_order_placed_at FROM data_model.f_order_consumer WHERE lower(order_source) = 'order_on_call' AND order_status_id IN (9,10) GROUP BY 1) AS first_ooc_fulf ON foc.customer_id=first_ooc_fulf.customer_id
WHERE cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)
--cu.id=3629969
)
WHERE DATE(customer_registered_at)>='2018-03-01'
GROUP BY 1,2,3
HAVING COUNT(CASE WHEN order_source='OOC' THEN order_id END)>0--SUM(order_fulfilled_flag)>=5
ORDER BY 3 DESC;
