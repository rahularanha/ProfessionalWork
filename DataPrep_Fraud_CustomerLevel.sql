
-----All consolidated on customer_id

WITH order_ucode AS (
	SELECT foc.customer_id, ci.order_id, supplier_city_name, cii.ucode, 
			SUM(cii.quantity*cii.mrp) AS total_mrp, SUM(cii.discount) AS total_discount, SUM(cii.quantity) AS qty_delivered
	FROM data_model.f_order_consumer foc
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoices ci ON foc.order_id=ci.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ci.id=cii.customer_invoice_id
	LEFT JOIN pe_pe2_pe2."return" r ON foc.order_id=r.order_id AND r.status=4
	WHERE foc.order_status_id IN (9,10) AND r.order_id IS NULL AND DATE(foc.order_placed_at)>='2019-10-01'
	GROUP BY 1,2,3,4
),
ucode_stats AS (
	SELECT p90.*
	FROM (
		SELECT ucode,
				PERCENTILE_DISC(0.98) 
				WITHIN GROUP (ORDER BY qty_delivered) 
				OVER (PARTITION BY ucode) AS p98_qty_delivered
		FROM order_ucode
	) p90
	GROUP BY 1,2
)
SELECT fin.customer_id, customer_firstname, customer_lastname, mobile_number,
		DATE(customer_registered_at) AS customer_registration_date, 
		already_banned_flag,
		address_count, patient_count, 
		COALESCE(total_orders_delivered,0) AS total_orders_delivered,
		COALESCE(total_orders_atleast_high_lowmargin,0) AS total_orders_atleast_high_lowmargin,
		COALESCE(total_orders_atleast_high_potabus,0) AS total_orders_atleast_high_potabus,
		COALESCE(total_orders_atleast_max,0) AS total_orders_atleast_max,
		COALESCE(total_orders_atleast_outlier,0) AS total_orders_atleast_outlier,
		COALESCE(total_orders_atleast_potabuse,0) AS total_orders_atleast_potabuse,
--		COUNT(CASE WHEN verticle='Customer Care' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_cc,
--		COUNT(CASE WHEN verticle='Docstat' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_docstat,
--		COUNT(CASE WHEN verticle='Warehouse' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_warehouse,
		COUNT(CASE WHEN verticle='Hub' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_hub,
		COUNT(CASE WHEN verticle='Last Mile' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_lastmile,
		SUM(cancelled_after_wh_flag) AS orders_cancelled_after_wh,
		SUM(cancelled_after_dc_flag) AS orders_cancelled_after_dc,
		SUM(cancelled_after_ofd_flag) AS orders_cancelled_after_ofd,
		SUM(cancelled_after_db_flag) AS orders_cancelled_after_db,
		SUM(cancelled_at_delivery_flag) AS orders_cancelled_at_delivery,
		COUNT(order_id) AS orders_placed,
		COUNT(CASE WHEN order_status_id NOT IN (2,8,9,10) THEN order_id END) AS orders_under_process,
		COUNT(CASE WHEN order_status_id IN (2,8,9,10) THEN order_id END) AS orders_closed,
		SUM(order_fulfilled_flag) AS orders_fulfilled,
		SUM(order_cancelled_flag) AS orders_cancelled,
		COUNT(CASE WHEN order_status_id=2 THEN order_id END) AS orders_rejected,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN mrp END) AS gmv,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN returned_gmv END) AS returns_returned_gmv,
		SUM(COALESCE(total_issue_refund_amount,0)) AS total_issue_refund_amount,
		COUNT(CASE WHEN order_fulfilled_flag=1 AND mrp=returned_gmv THEN order_id END) AS complete_returns,
--		SUM(line_item_returned_flag1) AS orders_atleast_one_return1,
		SUM(line_item_returned_flag) AS orders_atleast_one_return,
		SUM(issue_refunded_flag) AS orders_issue_refunded,
		COUNT(CASE WHEN fin.order_source='CP' THEN order_id END) AS orders_placed_from_cp,
		COUNT(CASE WHEN fin.order_source='OOC' THEN order_id END) AS orders_placed_from_ooc,
		MIN(order_placed_at) AS first_order_placed_at,
		MAX(order_placed_at) AS last_order_placed_at,
		MIN(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS first_fulfilled_order_placed_at,
		MAX(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS last_fulfilled_order_placed_at,
		SUM(order_dp_flag) AS orders_dp,
		SUM(app_chosen_dp_flag) AS app_chosen_dp,
		SUM(order_dp_sc_flag) AS order_dp_sc,
		SUM(app_chosen_sc_flag) AS app_chosen_sc,
		SUM(order_dp_sc_cancelled_flag) AS order_dp_sc_cancelled,
		SUM(app_chosen_sc_canned_flag) AS app_chosen_sc_cancelled,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3) AND cancelled_at_delivery_flag=1 THEN 1 ELSE 0 END) AS last3_cancel_at_delivery,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3,4,5) AND order_cancelled_flag=1 THEN 1 ELSE 0 END) AS last5_cancel,
		SUM(CASE WHEN order_number_cancel_last IN (1,2,3,4,5) AND order_dp_sc_flag=1 THEN 1 ELSE 0 END) AS last5_rx_cancel,
		SUM(CASE WHEN order_number_fulfill_last IN (1,2,3,4,5) AND line_item_returned_flag=1 THEN 1 ELSE 0 END) AS last5_return		
FROM (
	SELECT foc.customer_id, DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, 
			CASE WHEN cf.customer_id IS NOT NULL THEN 1 ELSE 0 END AS already_banned_flag,
			foc.mrp::INT, ror.returned_gmv, (iss.refund_amount+iss.gratification_amount) AS total_issue_refund_amount,
			cu.firstname AS customer_firstname, cu.lastname AS customer_lastname, cu.mobile_number,
			foc.order_id, foc.order_placed_at, foc.order_source AS platform, foc.order_status_id,
			CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
			CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc,
--			CASE WHEN r.order_id IS NOT NULL THEN 1 ELSE 0 END AS line_item_returned_flag1,
			CASE WHEN ror.order_id IS NOT NULL AND ror.returned_gmv>0 THEN 1 ELSE 0 END AS line_item_returned_flag,
			CASE WHEN (iss.refund_amount+iss.gratification_amount)>0 THEN 1 ELSE 0 END AS issue_refunded_flag,
			CASE WHEN foc.order_status_id IN (2,8,9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY cu.id, foc.order_status_id IN (2,8,9,10) ORDER BY foc.order_id DESC) 
			END AS order_number_endstate_last, 
			CASE WHEN foc.order_status_id=8 THEN 
					ROW_NUMBER() OVER (PARTITION BY cu.id, foc.order_status_id=8 ORDER BY foc.order_id DESC) 
			END AS order_number_cancel_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY cu.id, foc.order_status_id IN (9,10) ORDER BY foc.order_id DESC)
			END AS order_number_fulfill_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag,
			CASE WHEN foc.order_status_id=8 THEN 1 ELSE 0 END AS order_cancelled_flag,
			CASE WHEN fdpo.latest_case_status IS NOT NULL THEN 1 ELSE 0 END AS order_dp_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') THEN 1 ELSE 0 END AS app_chosen_dp_flag,
			CASE WHEN fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS order_dp_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS app_chosen_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_canned_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_sc_canned_flag,
			CASE WHEN fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS order_dp_sc_cancelled_flag,
			CASE WHEN foc.cnr_timestamp>fo.wh_min_invoice_generated_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_wh_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_retailer_billed_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_dc_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_out_for_delivery_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_ofd_flag,
			CASE WHEN foc.cnr_timestamp>fo.db_min_postponed_at_delivery_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_db_flag,
			CASE WHEN foc.cnr_reason='CANCELLED AT DELIVERY' THEN 1 ELSE 0 END AS cancelled_at_delivery_flag,
			foc.order_cancelled_at_stage, osvm.verticle
	FROM data_model.f_order_consumer foc 
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer cu ON foc.customer_id=cu.id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON foc.order_id=fdpo.order_id
	LEFT JOIN pre_analytics.order_status_vertical_mapping osvm ON foc.order_cancelled_at_stage=osvm.status_id
--	LEFT JOIN (SELECT order_id FROM pe_pe2_pe2."return" WHERE status=4 GROUP BY 1) r ON foc.order_id=r.order_id
	LEFT JOIN (SELECT order_id, SUM(order_value) AS returned_gmv FROM data_model.return_order_refunds GROUP BY 1) ror ON foc.order_id=ror.order_id
	LEFT JOIN (
				SELECT i.entity_id AS order_id, 
						SUM(COALESCE((id.refund_bank+id.refund_sources+id.refund_wallet),0)) AS refund_amount, 
						SUM(COALESCE(ig.amount,0)) AS gratification_amount
						--, i.id AS issue_id, ic.category_name AS issue_type, id.is_refund, 
				FROM pe_pe2_pe2.issue i
				LEFT JOIN pe_pe2_pe2.issue_category ic ON i.issue_category_id=ic.id
				LEFT JOIN pe_pe2_pe2.issue_details id ON i.id=id.issue_id
				LEFT JOIN pe_pe2_pe2.issue_gratification ig ON i.id=ig.issue_id
				WHERE i.entity_type=1 AND ic.category_name='Billed but not delivered'--DATE(DATEADD(MIN,330,i.created_at)) BETWEEN '2019-11-01' AND '2019-11-30'
				GROUP BY 1
	) iss ON foc.order_id=iss.order_id
	LEFT JOIN pe_pe2_pe2.customer_flags cf ON cu.id=cf.customer_id AND cf.flag_id=9
	WHERE DATE(foc.order_placed_at)<=(CURRENT_DATE-1) AND cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)  --cu.id=3629969 foc.order_source NOT IN ('CMS', 'Third_Party_API')
) fin
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT id) AS address_count FROM pe_pe2_pe2.customer_address GROUP BY 1) ca ON fin.customer_id=ca.customer_id
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT patient_id) AS patient_count FROM pe_pe2_pe2.rx GROUP BY 1) rx ON fin.customer_id=rx.customer_id
LEFT JOIN (
			SELECT customer_id, 
					COUNT(order_id) AS total_orders_delivered,
					COUNT(CASE WHEN total_items_low_margin>0 THEN order_id END) AS total_orders_atleast_high_lowmargin,
					COUNT(CASE WHEN total_items_potential_abuse>0 THEN order_id END) AS total_orders_atleast_high_potabus,
					COUNT(CASE WHEN total_items_max>0 THEN order_id END) AS total_orders_atleast_max,
					COUNT(CASE WHEN total_items_outlier>0 THEN order_id END) AS total_orders_atleast_outlier,
					COUNT(CASE WHEN total_items_pa>0 THEN order_id END) AS total_orders_atleast_potabuse
			--		COUNT(CASE WHEN total_items_max=total_line_items_delivered THEN order_id END) AS total_orders_all_max,
			--		COUNT(CASE WHEN total_items_outlier=total_line_items_delivered THEN order_id END) AS total_orders_all_outlier,
			--		COUNT(CASE WHEN total_items_lm=total_line_items_delivered THEN order_id END) AS total_orders_all_lowmargin,
			--		COUNT(CASE WHEN total_items_pa=total_line_items_delivered THEN order_id END) AS total_orders_all_potabuse
			FROM (
				SELECT customer_id, order_id, ----- customer_order level
						COUNT(ucode) AS total_line_items_delivered, 
						COUNT(CASE WHEN low_margin_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_low_margin,
						COUNT(CASE WHEN potential_abuse_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_potential_abuse,
						SUM(max_qty_delivered_flag) AS total_items_max,
						SUM(outlier_qty_delivered_flag) AS total_items_outlier,
						SUM(low_margin_flag) AS total_items_lm,
						SUM(potential_abuse_flag) AS total_items_pa
				FROM (
					SELECT ou.*, ----- customer_order_ucode level
							total_discount::FLOAT/total_mrp AS discount_fraction, 
							cum.margin_fraction, 
							((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp)) AS profit_fraction,
							CASE WHEN ((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp))<0 THEN 1 ELSE 0 END AS low_margin_flag,
							CASE WHEN pa.ucode IS NOT NULL THEN 1 ELSE 0 END AS potential_abuse_flag,
							CASE WHEN ou.qty_delivered>=qc.qty_capping OR (qc.qty_capping IS NULL AND ou.qty_delivered>=20) THEN 1 ELSE 0 END AS max_qty_delivered_flag,
							CASE WHEN ou.qty_delivered>=us.p98_qty_delivered THEN 1 ELSE 0 END AS outlier_qty_delivered_flag
					FROM order_ucode ou
					LEFT JOIN ucode_stats us ON ou.ucode=us.ucode
					LEFT JOIN (
						SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
									WHEN LENGTH(ucode)=5 THEN '0' || ucode
									WHEN LENGTH(ucode)=4 THEN '00' || ucode
									WHEN LENGTH(ucode)=3 THEN '000' || ucode
									WHEN LENGTH(ucode)=2 THEN '0000' || ucode
									WHEN LENGTH(ucode)=1 THEN '00000' || ucode
									WHEN LENGTH(ucode)=0 THEN '000000' || ucode
								END AS ucode
						FROM adhoc_analysis.potential_abuse_ucodes_nov19 
					) pa ON ou.ucode=pa.ucode
					LEFT JOIN (
						SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
									WHEN LENGTH(ucode)=5 THEN '0' || ucode
									WHEN LENGTH(ucode)=4 THEN '00' || ucode
									WHEN LENGTH(ucode)=3 THEN '000' || ucode
									WHEN LENGTH(ucode)=2 THEN '0000' || ucode
									WHEN LENGTH(ucode)=1 THEN '00000' || ucode
									WHEN LENGTH(ucode)=0 THEN '000000' || ucode
								END AS ucode,
								city_name AS supplier_city_name,
								margin_fraction
						FROM adhoc_analysis.cityucode_margin_oct2019
					) cum ON ou.ucode=cum.ucode AND ou.supplier_city_name=cum.supplier_city_name
					LEFT JOIN (
							SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
										WHEN LENGTH(ucode)=5 THEN '0' || ucode
										WHEN LENGTH(ucode)=4 THEN '00' || ucode
										WHEN LENGTH(ucode)=3 THEN '000' || ucode
										WHEN LENGTH(ucode)=2 THEN '0000' || ucode
										WHEN LENGTH(ucode)=1 THEN '00000' || ucode
										WHEN LENGTH(ucode)=0 THEN '000000' || ucode
									END AS ucode, 
									quantity_capping AS qty_capping
									FROM adhoc_analysis.quantity_capping_nov19
					) qc ON ou.ucode=qc.ucode
				)
				GROUP BY 1,2
			)
			GROUP BY 1
) qma ON fin.customer_id=qma.customer_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
HAVING COUNT(order_id)>=10 AND MAX(DATE(order_placed_at))>='2019-01-01'
ORDER BY 22 DESC;



---All consolidated on delivery phone number

WITH order_ucode AS (
	SELECT foc.customer_id, foc.order_contact_number, ci.order_id, supplier_city_name, cii.ucode, 
			SUM(cii.quantity*cii.mrp) AS total_mrp, SUM(cii.discount) AS total_discount, SUM(cii.quantity) AS qty_delivered
	FROM data_model.f_order_consumer foc
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoices ci ON foc.order_id=ci.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ci.id=cii.customer_invoice_id
	LEFT JOIN pe_pe2_pe2."return" r ON foc.order_id=r.order_id AND r.status=4
	WHERE foc.order_status_id IN (9,10) AND r.order_id IS NULL AND DATE(foc.order_placed_at)>='2019-10-01'
	GROUP BY 1,2,3,4,5
),
ucode_stats AS (
	SELECT p90.*
	FROM (
		SELECT ucode,
				PERCENTILE_DISC(0.98) 
				WITHIN GROUP (ORDER BY qty_delivered) 
				OVER (PARTITION BY ucode) AS p98_qty_delivered
		FROM order_ucode
	) p90
	GROUP BY 1,2
)
SELECT 	fin.order_contact_number,
--		fin.customer_id, customer_firstname, customer_lastname, mobile_number,
--		DATE(customer_registered_at) AS customer_registration_date, 
--		already_banned_flag,
		address_count, patient_count, 
		COALESCE(total_orders_delivered,0) AS total_orders_delivered,
		COALESCE(total_orders_atleast_high_lowmargin,0) AS total_orders_atleast_high_lowmargin,
		COALESCE(total_orders_atleast_high_potabus,0) AS total_orders_atleast_high_potabus,
		COALESCE(total_orders_atleast_max,0) AS total_orders_atleast_max,
		COALESCE(total_orders_atleast_outlier,0) AS total_orders_atleast_outlier,
		COALESCE(total_orders_atleast_potabuse,0) AS total_orders_atleast_potabuse,
--		COUNT(CASE WHEN verticle='Customer Care' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_cc,
--		COUNT(CASE WHEN verticle='Docstat' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_docstat,
--		COUNT(CASE WHEN verticle='Warehouse' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_warehouse,
		COUNT(CASE WHEN verticle='Hub' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_hub,
		COUNT(CASE WHEN verticle='Last Mile' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_lastmile,
		SUM(cancelled_after_wh_flag) AS orders_cancelled_after_wh,
		SUM(cancelled_after_dc_flag) AS orders_cancelled_after_dc,
		SUM(cancelled_after_ofd_flag) AS orders_cancelled_after_ofd,
		SUM(cancelled_after_db_flag) AS orders_cancelled_after_db,
		SUM(cancelled_at_delivery_flag) AS orders_cancelled_at_delivery,
		COUNT(order_id) AS orders_placed,
		COUNT(CASE WHEN order_status_id NOT IN (2,8,9,10) THEN order_id END) AS orders_under_process,
		COUNT(CASE WHEN order_status_id IN (2,8,9,10) THEN order_id END) AS orders_closed,
		SUM(order_fulfilled_flag) AS orders_fulfilled,
		SUM(order_cancelled_flag) AS orders_cancelled,
		COUNT(CASE WHEN order_status_id=2 THEN order_id END) AS orders_rejected,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN mrp END) AS gmv,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN returned_gmv END) AS returns_returned_gmv,
		SUM(COALESCE(total_issue_refund_amount,0)) AS total_issue_refund_amount,
		COUNT(CASE WHEN order_fulfilled_flag=1 AND mrp=returned_gmv THEN order_id END) AS complete_returns,
--		SUM(line_item_returned_flag1) AS orders_atleast_one_return1,
		SUM(line_item_returned_flag) AS orders_atleast_one_return,
		SUM(issue_refunded_flag) AS orders_issue_refunded,
--		COUNT(CASE WHEN fin.order_source='CP' THEN order_id END) AS orders_placed_from_cp,
--		COUNT(CASE WHEN fin.order_source='OOC' THEN order_id END) AS orders_placed_from_ooc,
		MIN(order_placed_at) AS first_order_placed_at,
		MAX(order_placed_at) AS last_order_placed_at,
		MIN(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS first_fulfilled_order_placed_at,
		MAX(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS last_fulfilled_order_placed_at,
		SUM(order_dp_flag) AS orders_dp,
		SUM(app_chosen_dp_flag) AS app_chosen_dp,
		SUM(order_dp_sc_flag) AS order_dp_sc,
		SUM(app_chosen_sc_flag) AS app_chosen_sc,
		SUM(order_dp_sc_cancelled_flag) AS order_dp_sc_cancelled,
		SUM(app_chosen_sc_canned_flag) AS app_chosen_sc_cancelled,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3) AND cancelled_at_delivery_flag=1 THEN 1 ELSE 0 END) AS last3_cancel_at_delivery,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3,4,5) AND order_cancelled_flag=1 THEN 1 ELSE 0 END) AS last5_cancel,
		SUM(CASE WHEN order_number_cancel_last IN (1,2,3,4,5) AND order_dp_sc_flag=1 THEN 1 ELSE 0 END) AS last5_rx_cancel,
		SUM(CASE WHEN order_number_fulfill_last IN (1,2,3,4,5) AND line_item_returned_flag=1 THEN 1 ELSE 0 END) AS last5_return		
FROM (
	SELECT foc.customer_id, foc.order_contact_number,
--			DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, 
--			CASE WHEN cf.customer_id IS NOT NULL THEN 1 ELSE 0 END AS already_banned_flag,
			foc.mrp::INT, ror.returned_gmv, (iss.refund_amount+iss.gratification_amount) AS total_issue_refund_amount,
--			cu.firstname AS customer_firstname, cu.lastname AS customer_lastname, cu.mobile_number,
			foc.order_id, foc.order_placed_at, foc.order_source AS platform, foc.order_status_id,
--			CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
--			CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc,
--			CASE WHEN r.order_id IS NOT NULL THEN 1 ELSE 0 END AS line_item_returned_flag1,
			CASE WHEN ror.order_id IS NOT NULL AND ror.returned_gmv>0 THEN 1 ELSE 0 END AS line_item_returned_flag,
			CASE WHEN (iss.refund_amount+iss.gratification_amount)>0 THEN 1 ELSE 0 END AS issue_refunded_flag,
			CASE WHEN foc.order_status_id IN (2,8,9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY foc.order_contact_number, foc.order_status_id IN (2,8,9,10) ORDER BY foc.order_id DESC) 
			END AS order_number_endstate_last, 
			CASE WHEN foc.order_status_id=8 THEN 
					ROW_NUMBER() OVER (PARTITION BY foc.order_contact_number, foc.order_status_id=8 ORDER BY foc.order_id DESC) 
			END AS order_number_cancel_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY foc.order_contact_number, foc.order_status_id IN (9,10) ORDER BY foc.order_id DESC)
			END AS order_number_fulfill_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag,
			CASE WHEN foc.order_status_id=8 THEN 1 ELSE 0 END AS order_cancelled_flag,
			CASE WHEN fdpo.latest_case_status IS NOT NULL THEN 1 ELSE 0 END AS order_dp_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') THEN 1 ELSE 0 END AS app_chosen_dp_flag,
			CASE WHEN fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS order_dp_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS app_chosen_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_canned_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_sc_canned_flag,
			CASE WHEN fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS order_dp_sc_cancelled_flag,
			CASE WHEN foc.cnr_timestamp>fo.wh_min_invoice_generated_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_wh_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_retailer_billed_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_dc_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_out_for_delivery_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_ofd_flag,
			CASE WHEN foc.cnr_timestamp>fo.db_min_postponed_at_delivery_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_db_flag,
			CASE WHEN foc.cnr_reason='CANCELLED AT DELIVERY' THEN 1 ELSE 0 END AS cancelled_at_delivery_flag,
			foc.order_cancelled_at_stage, osvm.verticle
	FROM data_model.f_order_consumer foc 
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer cu ON foc.customer_id=cu.id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON foc.order_id=fdpo.order_id
	LEFT JOIN pre_analytics.order_status_vertical_mapping osvm ON foc.order_cancelled_at_stage=osvm.status_id
--	LEFT JOIN (SELECT order_id FROM pe_pe2_pe2."return" WHERE status=4 GROUP BY 1) r ON foc.order_id=r.order_id
	LEFT JOIN (SELECT order_id, SUM(order_value) AS returned_gmv FROM data_model.return_order_refunds GROUP BY 1) ror ON foc.order_id=ror.order_id
	LEFT JOIN (
				SELECT i.entity_id AS order_id, 
						SUM(COALESCE((id.refund_bank+id.refund_sources+id.refund_wallet),0)) AS refund_amount, 
						SUM(COALESCE(ig.amount,0)) AS gratification_amount
						--, i.id AS issue_id, ic.category_name AS issue_type, id.is_refund, 
				FROM pe_pe2_pe2.issue i
				LEFT JOIN pe_pe2_pe2.issue_category ic ON i.issue_category_id=ic.id
				LEFT JOIN pe_pe2_pe2.issue_details id ON i.id=id.issue_id
				LEFT JOIN pe_pe2_pe2.issue_gratification ig ON i.id=ig.issue_id
				WHERE i.entity_type=1 AND ic.category_name='Billed but not delivered'--DATE(DATEADD(MIN,330,i.created_at)) BETWEEN '2019-11-01' AND '2019-11-30'
				GROUP BY 1
	) iss ON foc.order_id=iss.order_id
	LEFT JOIN pe_pe2_pe2.customer_flags cf ON cu.id=cf.customer_id AND cf.flag_id=9
	WHERE DATE(foc.order_placed_at)<=(CURRENT_DATE-1) AND cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)  --cu.id=3629969 foc.order_source NOT IN ('CMS', 'Third_Party_API')
) fin
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT id) AS address_count FROM pe_pe2_pe2.customer_address GROUP BY 1) ca ON fin.customer_id=ca.customer_id
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT patient_id) AS patient_count FROM pe_pe2_pe2.rx GROUP BY 1) rx ON fin.customer_id=rx.customer_id
LEFT JOIN (
			SELECT order_contact_number, 
					COUNT(order_id) AS total_orders_delivered,
					COUNT(CASE WHEN total_items_low_margin>0 THEN order_id END) AS total_orders_atleast_high_lowmargin,
					COUNT(CASE WHEN total_items_potential_abuse>0 THEN order_id END) AS total_orders_atleast_high_potabus,
					COUNT(CASE WHEN total_items_max>0 THEN order_id END) AS total_orders_atleast_max,
					COUNT(CASE WHEN total_items_outlier>0 THEN order_id END) AS total_orders_atleast_outlier,
					COUNT(CASE WHEN total_items_pa>0 THEN order_id END) AS total_orders_atleast_potabuse
			--		COUNT(CASE WHEN total_items_max=total_line_items_delivered THEN order_id END) AS total_orders_all_max,
			--		COUNT(CASE WHEN total_items_outlier=total_line_items_delivered THEN order_id END) AS total_orders_all_outlier,
			--		COUNT(CASE WHEN total_items_lm=total_line_items_delivered THEN order_id END) AS total_orders_all_lowmargin,
			--		COUNT(CASE WHEN total_items_pa=total_line_items_delivered THEN order_id END) AS total_orders_all_potabuse
			FROM (
				SELECT customer_id, order_id, order_contact_number, ----- customer_order level
						COUNT(ucode) AS total_line_items_delivered, 
						COUNT(CASE WHEN low_margin_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_low_margin,
						COUNT(CASE WHEN potential_abuse_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_potential_abuse,
						SUM(max_qty_delivered_flag) AS total_items_max,
						SUM(outlier_qty_delivered_flag) AS total_items_outlier,
						SUM(low_margin_flag) AS total_items_lm,
						SUM(potential_abuse_flag) AS total_items_pa
				FROM (
					SELECT ou.*, ----- customer_order_ucode level
							total_discount::FLOAT/total_mrp AS discount_fraction, 
							cum.margin_fraction, 
							((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp)) AS profit_fraction,
							CASE WHEN ((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp))<0 THEN 1 ELSE 0 END AS low_margin_flag,
							CASE WHEN pa.ucode IS NOT NULL THEN 1 ELSE 0 END AS potential_abuse_flag,
							CASE WHEN ou.qty_delivered>=qc.qty_capping OR (qc.qty_capping IS NULL AND ou.qty_delivered>=20) THEN 1 ELSE 0 END AS max_qty_delivered_flag,
							CASE WHEN ou.qty_delivered>=us.p98_qty_delivered THEN 1 ELSE 0 END AS outlier_qty_delivered_flag
					FROM order_ucode ou
					LEFT JOIN ucode_stats us ON ou.ucode=us.ucode
					LEFT JOIN (
						SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
									WHEN LENGTH(ucode)=5 THEN '0' || ucode
									WHEN LENGTH(ucode)=4 THEN '00' || ucode
									WHEN LENGTH(ucode)=3 THEN '000' || ucode
									WHEN LENGTH(ucode)=2 THEN '0000' || ucode
									WHEN LENGTH(ucode)=1 THEN '00000' || ucode
									WHEN LENGTH(ucode)=0 THEN '000000' || ucode
								END AS ucode
						FROM adhoc_analysis.potential_abuse_ucodes_nov19 
					) pa ON ou.ucode=pa.ucode
					LEFT JOIN (
						SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
									WHEN LENGTH(ucode)=5 THEN '0' || ucode
									WHEN LENGTH(ucode)=4 THEN '00' || ucode
									WHEN LENGTH(ucode)=3 THEN '000' || ucode
									WHEN LENGTH(ucode)=2 THEN '0000' || ucode
									WHEN LENGTH(ucode)=1 THEN '00000' || ucode
									WHEN LENGTH(ucode)=0 THEN '000000' || ucode
								END AS ucode,
								city_name AS supplier_city_name,
								margin_fraction
						FROM adhoc_analysis.cityucode_margin_oct2019
					) cum ON ou.ucode=cum.ucode AND ou.supplier_city_name=cum.supplier_city_name
					LEFT JOIN (
							SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
										WHEN LENGTH(ucode)=5 THEN '0' || ucode
										WHEN LENGTH(ucode)=4 THEN '00' || ucode
										WHEN LENGTH(ucode)=3 THEN '000' || ucode
										WHEN LENGTH(ucode)=2 THEN '0000' || ucode
										WHEN LENGTH(ucode)=1 THEN '00000' || ucode
										WHEN LENGTH(ucode)=0 THEN '000000' || ucode
									END AS ucode, 
									quantity_capping AS qty_capping
									FROM adhoc_analysis.quantity_capping_nov19
					) qc ON ou.ucode=qc.ucode
				)
				GROUP BY 1,2,3
			)
			GROUP BY 1
) qma ON fin.order_contact_number=qma.order_contact_number
GROUP BY 1,2,3,4,5,6,7,8,9--,10,11,12,13,14
HAVING COUNT(order_id)>=10 AND MAX(DATE(order_placed_at))>='2019-01-01'
ORDER BY 22 DESC;









----- Fraud customer level

SELECT fin.customer_id, customer_firstname, customer_lastname, mobile_number,
		DATE(customer_registered_at) AS customer_registration_date, 
		already_banned_flag,
		address_count, patient_count, 
--		COUNT(CASE WHEN verticle='Customer Care' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_cc,
--		COUNT(CASE WHEN verticle='Docstat' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_docstat,
--		COUNT(CASE WHEN verticle='Warehouse' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_warehouse,
		COUNT(CASE WHEN verticle='Hub' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_hub,
		COUNT(CASE WHEN verticle='Last Mile' AND order_cancelled_flag=1 THEN order_id END) AS cnr_at_lastmile,
		SUM(cancelled_after_wh_flag) AS orders_cancelled_after_wh,
		SUM(cancelled_after_dc_flag) AS orders_cancelled_after_dc,
		SUM(cancelled_after_ofd_flag) AS orders_cancelled_after_ofd,
		SUM(cancelled_after_db_flag) AS orders_cancelled_after_db,
		SUM(cancelled_at_delivery_flag) AS orders_cancelled_at_delivery,
		COUNT(order_id) AS orders_placed,
		COUNT(CASE WHEN order_status_id NOT IN (2,8,9,10) THEN order_id END) AS orders_under_process,
		COUNT(CASE WHEN order_status_id IN (2,8,9,10) THEN order_id END) AS orders_closed,
		SUM(order_fulfilled_flag) AS orders_fulfilled,
		SUM(order_cancelled_flag) AS orders_cancelled,
		COUNT(CASE WHEN order_status_id=2 THEN order_id END) AS orders_rejected,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN mrp END) AS gmv,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN returned_gmv END) AS returns_returned_gmv,
		SUM(COALESCE(total_issue_refund_amount,0)) AS total_issue_refund_amount,
		COUNT(CASE WHEN order_fulfilled_flag=1 AND mrp=returned_gmv THEN order_id END) AS complete_returns,
--		SUM(line_item_returned_flag1) AS orders_atleast_one_return1,
		SUM(line_item_returned_flag) AS orders_atleast_one_return,
		SUM(issue_refunded_flag) AS orders_issue_refunded,
		COUNT(CASE WHEN fin.order_source='CP' THEN order_id END) AS orders_placed_from_cp,
		COUNT(CASE WHEN fin.order_source='OOC' THEN order_id END) AS orders_placed_from_ooc,
		MIN(order_placed_at) AS first_order_placed_at,
		MAX(order_placed_at) AS last_order_placed_at,
		MIN(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS first_fulfilled_order_placed_at,
		MAX(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS last_fulfilled_order_placed_at,
		SUM(order_dp_flag) AS orders_dp,
		SUM(app_chosen_dp_flag) AS app_chosen_dp,
		SUM(order_dp_sc_flag) AS order_dp_sc,
		SUM(app_chosen_sc_flag) AS app_chosen_sc,
		SUM(order_dp_sc_cancelled_flag) AS order_dp_sc_cancelled,
		SUM(app_chosen_sc_canned_flag) AS app_chosen_sc_cancelled,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3) AND cancelled_at_delivery_flag=1 THEN 1 ELSE 0 END) AS last3_cancel_at_delivery,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3,4,5) AND order_cancelled_flag=1 THEN 1 ELSE 0 END) AS last5_cancel,
		SUM(CASE WHEN order_number_cancel_last IN (1,2,3,4,5) AND order_dp_sc_flag=1 THEN 1 ELSE 0 END) AS last5_rx_cancel,
		SUM(CASE WHEN order_number_fulfill_last IN (1,2,3,4,5) AND line_item_returned_flag=1 THEN 1 ELSE 0 END) AS last5_return		
FROM (
	SELECT foc.customer_id, DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, 
			CASE WHEN cf.customer_id IS NOT NULL THEN 1 ELSE 0 END AS already_banned_flag,
			foc.mrp::INT, ror.returned_gmv, (iss.refund_amount+iss.gratification_amount) AS total_issue_refund_amount,
			cu.firstname AS customer_firstname, cu.lastname AS customer_lastname, cu.mobile_number,
			foc.order_id, foc.order_placed_at, foc.order_source AS platform, foc.order_status_id,
			CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
			CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc,
--			CASE WHEN r.order_id IS NOT NULL THEN 1 ELSE 0 END AS line_item_returned_flag1,
			CASE WHEN ror.order_id IS NOT NULL AND ror.returned_gmv>0 THEN 1 ELSE 0 END AS line_item_returned_flag,
			CASE WHEN (iss.refund_amount+iss.gratification_amount)>0 THEN 1 ELSE 0 END AS issue_refunded_flag,
			CASE WHEN foc.order_status_id IN (2,8,9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY cu.id, foc.order_status_id IN (2,8,9,10) ORDER BY foc.order_id DESC) 
			END AS order_number_endstate_last, 
			CASE WHEN foc.order_status_id=8 THEN 
					ROW_NUMBER() OVER (PARTITION BY cu.id, foc.order_status_id=8 ORDER BY foc.order_id DESC) 
			END AS order_number_cancel_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY cu.id, foc.order_status_id IN (9,10) ORDER BY foc.order_id DESC)
			END AS order_number_fulfill_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag,
			CASE WHEN foc.order_status_id=8 THEN 1 ELSE 0 END AS order_cancelled_flag,
			CASE WHEN fdpo.latest_case_status IS NOT NULL THEN 1 ELSE 0 END AS order_dp_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') THEN 1 ELSE 0 END AS app_chosen_dp_flag,
			CASE WHEN fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS order_dp_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS app_chosen_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_canned_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_sc_canned_flag,
			CASE WHEN fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS order_dp_sc_cancelled_flag,
			CASE WHEN foc.cnr_timestamp>fo.wh_min_invoice_generated_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_wh_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_retailer_billed_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_dc_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_out_for_delivery_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_ofd_flag,
			CASE WHEN foc.cnr_timestamp>fo.db_min_postponed_at_delivery_time AND foc.order_status_id=8 THEN 1 ELSE 0 END AS cancelled_after_db_flag,
			CASE WHEN foc.cnr_reason='CANCELLED AT DELIVERY' THEN 1 ELSE 0 END AS cancelled_at_delivery_flag,
			foc.order_cancelled_at_stage, osvm.verticle
	FROM data_model.f_order_consumer foc 
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer cu ON foc.customer_id=cu.id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON foc.order_id=fdpo.order_id
	LEFT JOIN pre_analytics.order_status_vertical_mapping osvm ON foc.order_cancelled_at_stage=osvm.status_id
--	LEFT JOIN (SELECT order_id FROM pe_pe2_pe2."return" WHERE status=4 GROUP BY 1) r ON foc.order_id=r.order_id
	LEFT JOIN (SELECT order_id, SUM(order_value) AS returned_gmv FROM data_model.return_order_refunds GROUP BY 1) ror ON foc.order_id=ror.order_id
	LEFT JOIN (
				SELECT i.entity_id AS order_id, 
						SUM(COALESCE((id.refund_bank+id.refund_sources+id.refund_wallet),0)) AS refund_amount, 
						SUM(COALESCE(ig.amount,0)) AS gratification_amount
						--, i.id AS issue_id, ic.category_name AS issue_type, id.is_refund, 
				FROM pe_pe2_pe2.issue i
				LEFT JOIN pe_pe2_pe2.issue_category ic ON i.issue_category_id=ic.id
				LEFT JOIN pe_pe2_pe2.issue_details id ON i.id=id.issue_id
				LEFT JOIN pe_pe2_pe2.issue_gratification ig ON i.id=ig.issue_id
				WHERE i.entity_type=1 AND ic.category_name='Billed but not delivered'--DATE(DATEADD(MIN,330,i.created_at)) BETWEEN '2019-11-01' AND '2019-11-30'
				GROUP BY 1
	) iss ON foc.order_id=iss.order_id
	LEFT JOIN pe_pe2_pe2.customer_flags cf ON cu.id=cf.customer_id AND cf.flag_id=9
	WHERE DATE(foc.order_placed_at)<=(CURRENT_DATE-1) AND cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)  --cu.id=3629969 foc.order_source NOT IN ('CMS', 'Third_Party_API')
) fin
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT id) AS address_count FROM pe_pe2_pe2.customer_address GROUP BY 1) ca ON fin.customer_id=ca.customer_id
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT patient_id) AS patient_count FROM pe_pe2_pe2.rx GROUP BY 1) rx ON fin.customer_id=rx.customer_id
GROUP BY 1,2,3,4,5,6,7,8
HAVING COUNT(order_id)>=10 AND MAX(DATE(order_placed_at))>='2019-01-01'
ORDER BY 16 DESC;




----- Fraud delivery contact number

SELECT order_contact_number,
--fin.customer_id, customer_firstname, customer_lastname, mobile_number,
--		DATE(customer_registered_at) AS customer_registration_date, 
--		address_count, patient_count, 
		COUNT(CASE WHEN verticle='Customer Care' THEN order_id END) AS cnr_at_cc,
		COUNT(CASE WHEN verticle='Docstat' THEN order_id END) AS cnr_at_docstat,
		COUNT(CASE WHEN verticle='Warehouse' THEN order_id END) AS cnr_at_warehouse,
		COUNT(CASE WHEN verticle='Hub' THEN order_id END) AS cnr_at_hub,
		COUNT(CASE WHEN verticle='Last Mile' THEN order_id END) AS cnr_at_lastmile,
		SUM(cancelled_after_wh_flag) AS orders_cancelled_after_wh,
		SUM(cancelled_after_dc_flag) AS orders_cancelled_after_dc,
		SUM(cancelled_after_ofd_flag) AS orders_cancelled_after_ofd,
		SUM(cancelled_after_db_flag) AS orders_cancelled_after_db,
		SUM(cancelled_at_delivery_flag) AS orders_cancelled_at_delivery,
		COUNT(order_id) AS orders_placed,
		COUNT(CASE WHEN order_status_id NOT IN (2,8,9,10) THEN order_id END) AS orders_under_process,
		COUNT(CASE WHEN order_status_id IN (2,8,9,10) THEN order_id END) AS orders_closed,
		SUM(order_fulfilled_flag) AS orders_fulfilled,
		SUM(order_cancelled_flag) AS orders_cancelled,
		COUNT(CASE WHEN order_status_id=2 THEN order_id END) AS orders_rejected,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN mrp END) AS gmv,
		SUM(CASE WHEN order_fulfilled_flag=1 THEN returned_gmv END) AS returned_gmv,
		COUNT(CASE WHEN order_fulfilled_flag=1 AND mrp=returned_gmv THEN order_id END) AS complete_returns,
		SUM(line_item_returned_flag) AS orders_atleast_one_return,
		COUNT(CASE WHEN fin.order_source='CP' THEN order_id END) AS orders_placed_from_cp,
		COUNT(CASE WHEN fin.order_source='OOC' THEN order_id END) AS orders_placed_from_ooc,
		MIN(order_placed_at) AS first_order_placed_at,
		MAX(order_placed_at) AS last_order_placed_at,
		MIN(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS first_fulfilled_order_placed_at,
		MAX(CASE WHEN order_status_id IN (9,10) THEN order_placed_at END) AS last_fulfilled_order_placed_at,
		SUM(order_dp_flag) AS orders_dp,
		SUM(app_chosen_dp_flag) AS app_chosen_dp,
		SUM(order_dp_sc_flag) AS order_dp_sc,
		SUM(app_chosen_sc_flag) AS app_chosen_sc,
		SUM(order_dp_sc_cancelled_flag) AS order_dp_sc_cancelled,
		SUM(app_chosen_sc_canned_flag) AS app_chosen_sc_cancelled,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3) AND cancelled_at_delivery_flag=1 THEN 1 ELSE 0 END) AS last3_cancel,
		SUM(CASE WHEN order_number_endstate_last IN (1,2,3,4,5) AND order_cancelled_flag=1 THEN 1 ELSE 0 END) AS last5_cancel,
		SUM(CASE WHEN order_number_cancel_last IN (1,2,3,4,5) AND order_dp_sc_flag=1 THEN 1 ELSE 0 END) AS last5_rx_cancel,
		SUM(CASE WHEN order_number_fulfill_last IN (1,2,3,4,5) AND line_item_returned_flag=1 THEN 1 ELSE 0 END) AS last5_return		
FROM (
	SELECT foc.customer_id, DATEADD(MIN,330,cu.dateadded) AS customer_registered_at, foc.mrp::INT, ror.returned_gmv,
			cu.firstname AS customer_firstname, cu.lastname AS customer_lastname, cu.mobile_number, foc.order_contact_number,
			foc.order_id, foc.order_placed_at, foc.order_source AS platform, foc.order_status_id,
			CASE WHEN lower(foc.order_source)='order_on_call' THEN 'OOC' ELSE 'CP' END AS order_source,
			CASE WHEN cu.customer_source = 'order-on-call' THEN 1 ELSE 0 END AS acquired_through_ooc,
			CASE WHEN r.order_id IS NOT NULL THEN 1 ELSE 0 END AS line_item_returned_flag,
			CASE WHEN foc.order_status_id IN (2,8,9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY foc.order_contact_number, foc.order_status_id IN (2,8,9,10) ORDER BY foc.order_id DESC) 
			END AS order_number_endstate_last, 
			CASE WHEN foc.order_status_id=8 THEN 
					ROW_NUMBER() OVER (PARTITION BY foc.order_contact_number, foc.order_status_id=8 ORDER BY foc.order_id DESC) 
			END AS order_number_cancel_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 
					ROW_NUMBER() OVER (PARTITION BY foc.order_contact_number, foc.order_status_id IN (9,10) ORDER BY foc.order_id DESC)
			END AS order_number_fulfill_last, 
			CASE WHEN foc.order_status_id IN (9,10) THEN 1 ELSE 0 END AS order_fulfilled_flag,
			CASE WHEN foc.order_status_id=8 THEN 1 ELSE 0 END AS order_cancelled_flag,
			CASE WHEN fdpo.latest_case_status IS NOT NULL THEN 1 ELSE 0 END AS order_dp_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') THEN 1 ELSE 0 END AS app_chosen_dp_flag,
			CASE WHEN fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS order_dp_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 THEN 1 ELSE 0 END AS app_chosen_sc_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_canned_flag,
			CASE WHEN fdpo.order_source IN ('Order Without Prescription (Consumer App)', 'Order On Call') AND fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS app_chosen_sc_canned_flag,
			CASE WHEN fdpo.latest_case_status=4 AND foc.order_status_id=8 THEN 1 ELSE 0 END AS order_dp_sc_cancelled_flag,
			CASE WHEN foc.cnr_timestamp>fo.wh_min_invoice_generated_time THEN 1 ELSE 0 END AS cancelled_after_wh_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_retailer_billed_time THEN 1 ELSE 0 END AS cancelled_after_dc_flag,
			CASE WHEN foc.cnr_timestamp>fo.dc_min_out_for_delivery_time THEN 1 ELSE 0 END AS cancelled_after_ofd_flag,
			CASE WHEN foc.cnr_timestamp>fo.db_min_postponed_at_delivery_time THEN 1 ELSE 0 END AS cancelled_after_db_flag,
			CASE WHEN foc.cnr_reason='CANCELLED AT DELIVERY' THEN 1 ELSE 0 END AS cancelled_at_delivery_flag,
			foc.order_cancelled_at_stage, osvm.verticle
	FROM data_model.f_order_consumer foc 
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer cu ON foc.customer_id=cu.id
	LEFT JOIN data_model.f_doctor_program_order fdpo ON foc.order_id=fdpo.order_id
	LEFT JOIN pre_analytics.order_status_vertical_mapping osvm ON foc.order_cancelled_at_stage=osvm.status_id
	LEFT JOIN (SELECT order_id FROM pe_pe2_pe2."return" WHERE status=4 GROUP BY 1) r ON foc.order_id=r.order_id
	LEFT JOIN (SELECT order_id, SUM(order_value) AS returned_gmv FROM data_model.return_order_refunds GROUP BY 1) ror ON foc.order_id=ror.order_id
	WHERE DATE(foc.order_placed_at)<=(CURRENT_DATE-1) AND (foc.order_source NOT IN ('CMS', 'Third_Party_API'))--cu.id NOT IN (SELECT customer_id FROM data_model.f_order_consumer WHERE order_source IN ('CMS', 'Third_Party_API') OR order_source IS NULL GROUP BY 1)  --cu.id=3629969
) fin
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT id) AS address_count FROM pe_pe2_pe2.customer_address GROUP BY 1) ca ON fin.customer_id=ca.customer_id
LEFT JOIN (SELECT customer_id, COUNT(DISTINCT patient_id) AS patient_count FROM pe_pe2_pe2.rx GROUP BY 1) rx ON fin.customer_id=rx.customer_id
GROUP BY 1
HAVING COUNT(order_id)>=10 AND MAX(DATE(order_placed_at))>='2019-01-01'
ORDER BY 18 DESC;



----- potential abuse/low margin customer level

WITH order_ucode AS (
	SELECT foc.customer_id, ci.order_id, supplier_city_name, cii.ucode, 
			SUM(cii.quantity*cii.mrp) AS total_mrp, SUM(cii.discount) AS total_discount, SUM(cii.quantity) AS qty_delivered
	FROM data_model.f_order_consumer foc
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoices ci ON foc.order_id=ci.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ci.id=cii.customer_invoice_id
	LEFT JOIN pe_pe2_pe2."return" r ON foc.order_id=r.order_id AND r.status=4
	WHERE foc.order_status_id IN (9,10) AND r.order_id IS NULL AND DATE(foc.order_placed_at)>='2019-10-01'
	GROUP BY 1,2,3,4
),
ucode_stats AS (
	SELECT p90.*
	FROM (
		SELECT ucode,
				PERCENTILE_DISC(0.98) 
				WITHIN GROUP (ORDER BY qty_delivered) 
				OVER (PARTITION BY ucode) AS p98_qty_delivered
		FROM order_ucode
	) p90
	GROUP BY 1,2
)
SELECT customer_id, 
		COUNT(order_id) AS total_orders_delivered,
		COUNT(CASE WHEN total_items_low_margin>0 THEN order_id END) AS total_orders_atleast_high_lowmargin,
		COUNT(CASE WHEN total_items_potential_abuse>0 THEN order_id END) AS total_orders_atleast_high_potabus,
		COUNT(CASE WHEN total_items_max>0 THEN order_id END) AS total_orders_atleast_max,
		COUNT(CASE WHEN total_items_outlier>0 THEN order_id END) AS total_orders_atleast_outlier,
		COUNT(CASE WHEN total_items_pa>0 THEN order_id END) AS total_orders_atleast_potabuse,
--		COUNT(CASE WHEN total_items_max=total_line_items_delivered THEN order_id END) AS total_orders_all_max,
--		COUNT(CASE WHEN total_items_outlier=total_line_items_delivered THEN order_id END) AS total_orders_all_outlier,
--		COUNT(CASE WHEN total_items_lm=total_line_items_delivered THEN order_id END) AS total_orders_all_lowmargin,
--		COUNT(CASE WHEN total_items_pa=total_line_items_delivered THEN order_id END) AS total_orders_all_potabuse
FROM (
	SELECT customer_id, order_id, ----- customer_order level
			COUNT(ucode) AS total_line_items_delivered, 
			COUNT(CASE WHEN low_margin_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_low_margin,
			COUNT(CASE WHEN potential_abuse_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_potential_abuse,
			SUM(max_qty_delivered_flag) AS total_items_max,
			SUM(outlier_qty_delivered_flag) AS total_items_outlier,
			SUM(low_margin_flag) AS total_items_lm,
			SUM(potential_abuse_flag) AS total_items_pa
	FROM (
		SELECT ou.*, ----- customer_order_ucode level
				total_discount::FLOAT/total_mrp AS discount_fraction, 
				cum.margin_fraction, 
				((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp)) AS profit_fraction,
				CASE WHEN ((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp))<0 THEN 1 ELSE 0 END AS low_margin_flag,
				CASE WHEN pa.ucode IS NOT NULL THEN 1 ELSE 0 END AS potential_abuse_flag,
				CASE WHEN ou.qty_delivered>=qc.qty_capping OR (qc.qty_capping IS NULL AND ou.qty_delivered>=20) THEN 1 ELSE 0 END AS max_qty_delivered_flag,
				CASE WHEN ou.qty_delivered>=us.p98_qty_delivered THEN 1 ELSE 0 END AS outlier_qty_delivered_flag
		FROM order_ucode ou
		LEFT JOIN ucode_stats us ON ou.ucode=us.ucode
		LEFT JOIN (
			SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
						WHEN LENGTH(ucode)=5 THEN '0' || ucode
						WHEN LENGTH(ucode)=4 THEN '00' || ucode
						WHEN LENGTH(ucode)=3 THEN '000' || ucode
						WHEN LENGTH(ucode)=2 THEN '0000' || ucode
						WHEN LENGTH(ucode)=1 THEN '00000' || ucode
						WHEN LENGTH(ucode)=0 THEN '000000' || ucode
					END AS ucode
			FROM adhoc_analysis.potential_abuse_ucodes_nov19 
		) pa ON ou.ucode=pa.ucode
		LEFT JOIN (
			SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
						WHEN LENGTH(ucode)=5 THEN '0' || ucode
						WHEN LENGTH(ucode)=4 THEN '00' || ucode
						WHEN LENGTH(ucode)=3 THEN '000' || ucode
						WHEN LENGTH(ucode)=2 THEN '0000' || ucode
						WHEN LENGTH(ucode)=1 THEN '00000' || ucode
						WHEN LENGTH(ucode)=0 THEN '000000' || ucode
					END AS ucode,
					city_name AS supplier_city_name,
					margin_fraction
			FROM adhoc_analysis.cityucode_margin_oct2019
		) cum ON ou.ucode=cum.ucode AND ou.supplier_city_name=cum.supplier_city_name
		LEFT JOIN (
				SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
							WHEN LENGTH(ucode)=5 THEN '0' || ucode
							WHEN LENGTH(ucode)=4 THEN '00' || ucode
							WHEN LENGTH(ucode)=3 THEN '000' || ucode
							WHEN LENGTH(ucode)=2 THEN '0000' || ucode
							WHEN LENGTH(ucode)=1 THEN '00000' || ucode
							WHEN LENGTH(ucode)=0 THEN '000000' || ucode
						END AS ucode, 
						quantity_capping AS qty_capping
						FROM adhoc_analysis.quantity_capping_nov19
		) qc ON ou.ucode=qc.ucode
	)
	GROUP BY 1,2
)
GROUP BY 1
--HAVING COUNT(order_id)>5
ORDER BY 3 DESC;



----- potential abuse/low margin order contact number

WITH order_ucode AS (
	SELECT foc.customer_id, foc.order_contact_number, ci.order_id, supplier_city_name, cii.ucode, 
			SUM(cii.quantity*cii.mrp) AS total_mrp, SUM(cii.discount) AS total_discount, SUM(cii.quantity) AS qty_delivered
	FROM data_model.f_order_consumer foc
	LEFT JOIN data_model.f_order fo ON foc.order_id=fo.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoices ci ON foc.order_id=ci.order_id
	LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ci.id=cii.customer_invoice_id
	LEFT JOIN pe_pe2_pe2."return" r ON foc.order_id=r.order_id AND r.status=4
	WHERE foc.order_status_id IN (9,10) AND r.order_id IS NULL AND DATE(foc.order_placed_at)>='2019-10-01'
	GROUP BY 1,2,3,4,5
),
ucode_stats AS (
	SELECT p90.*
	FROM (
		SELECT ucode,
				PERCENTILE_DISC(0.98) 
				WITHIN GROUP (ORDER BY qty_delivered) 
				OVER (PARTITION BY ucode) AS p98_qty_delivered
		FROM order_ucode
	) p90
	GROUP BY 1,2
)
SELECT order_contact_number, 
		COUNT(order_id) AS total_orders_delivered,
		COUNT(CASE WHEN total_items_low_margin>0 THEN order_id END) AS total_orders_atleast_lowmargin,
		COUNT(CASE WHEN total_items_potential_abuse>0 THEN order_id END) AS total_orders_atleast_potabus,
		COUNT(CASE WHEN total_items_max>0 THEN order_id END) AS total_orders_atleast_max,
		COUNT(CASE WHEN total_items_outlier>0 THEN order_id END) AS total_orders_atleast_outlier,
		COUNT(CASE WHEN total_items_pa>0 THEN order_id END) AS total_orders_atleast_potabuse,
		COUNT(CASE WHEN total_items_max=total_line_items_delivered THEN order_id END) AS total_orders_all_max,
		COUNT(CASE WHEN total_items_outlier=total_line_items_delivered THEN order_id END) AS total_orders_all_outlier,
		COUNT(CASE WHEN total_items_lm=total_line_items_delivered THEN order_id END) AS total_orders_all_lowmargin,
		COUNT(CASE WHEN total_items_pa=total_line_items_delivered THEN order_id END) AS total_orders_all_potabuse
FROM (
	SELECT customer_id, order_id,order_contact_number, ----- customer_order level
			COUNT(ucode) AS total_line_items_delivered, 
			COUNT(CASE WHEN low_margin_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_low_margin,
			COUNT(CASE WHEN potential_abuse_flag=1 AND outlier_qty_delivered_flag=1 THEN ucode END) AS total_items_potential_abuse,
			SUM(max_qty_delivered_flag) AS total_items_max,
			SUM(outlier_qty_delivered_flag) AS total_items_outlier,
			SUM(low_margin_flag) AS total_items_lm,
			SUM(potential_abuse_flag) AS total_items_pa
	FROM (
		SELECT ou.*, ----- customer_order_ucode level
				total_discount::FLOAT/total_mrp AS discount_fraction, 
				cum.margin_fraction, 
				((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp)) AS profit_fraction,
				CASE WHEN ((cum.margin_fraction)-(ou.total_discount::FLOAT/ou.total_mrp))<0.05 THEN 1 ELSE 0 END AS low_margin_flag,
				CASE WHEN pa.ucode IS NOT NULL THEN 1 ELSE 0 END AS potential_abuse_flag,
				CASE WHEN ou.qty_delivered>=qc.qty_capping OR ou.qty_delivered>=20 THEN 1 ELSE 0 END AS max_qty_delivered_flag,
				CASE WHEN ou.qty_delivered>=us.p98_qty_delivered THEN 1 ELSE 0 END AS outlier_qty_delivered_flag
		FROM order_ucode ou
		LEFT JOIN ucode_stats us ON ou.ucode=us.ucode
		LEFT JOIN (
			SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
						WHEN LENGTH(ucode)=5 THEN '0' || ucode
						WHEN LENGTH(ucode)=4 THEN '00' || ucode
						WHEN LENGTH(ucode)=3 THEN '000' || ucode
						WHEN LENGTH(ucode)=2 THEN '0000' || ucode
						WHEN LENGTH(ucode)=1 THEN '00000' || ucode
						WHEN LENGTH(ucode)=0 THEN '000000' || ucode
					END AS ucode
			FROM adhoc_analysis.potential_abuse_ucodes_nov19 
		) pa ON ou.ucode=pa.ucode
		LEFT JOIN (
			SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
						WHEN LENGTH(ucode)=5 THEN '0' || ucode
						WHEN LENGTH(ucode)=4 THEN '00' || ucode
						WHEN LENGTH(ucode)=3 THEN '000' || ucode
						WHEN LENGTH(ucode)=2 THEN '0000' || ucode
						WHEN LENGTH(ucode)=1 THEN '00000' || ucode
						WHEN LENGTH(ucode)=0 THEN '000000' || ucode
					END AS ucode,
					CASE WHEN city_name='Gurugram' THEN 'Gurgaon' ELSE city_name END AS supplier_city_name,
					margin_fraction
			FROM adhoc_analysis.cityucodelevel_margin_nov19
		) cum ON ou.ucode=cum.ucode AND ou.supplier_city_name=cum.supplier_city_name
		LEFT JOIN (
				SELECT CASE WHEN LENGTH(ucode)=6 THEN ucode
							WHEN LENGTH(ucode)=5 THEN '0' || ucode
							WHEN LENGTH(ucode)=4 THEN '00' || ucode
							WHEN LENGTH(ucode)=3 THEN '000' || ucode
							WHEN LENGTH(ucode)=2 THEN '0000' || ucode
							WHEN LENGTH(ucode)=1 THEN '00000' || ucode
							WHEN LENGTH(ucode)=0 THEN '000000' || ucode
						END AS ucode, quantity_capping AS qty_capping
						FROM adhoc_analysis.quantity_capping_nov19
		) qc ON ou.ucode=qc.ucode
	)
	GROUP BY 1,2,3
)
GROUP BY 1
--HAVING COUNT(order_id)>5
ORDER BY 3 DESC;

