------- CC Substition Analysis

----- !Further Analysis!
SELECT ccsl.order_id, 
		CASE WHEN foc.order_status_id IN (9,10) THEN 'fulfilled'
			 WHEN foc.order_status_id=2 THEN 'rejected'
			 WHEN foc.order_status_id=8 THEN 'cancelled'
			 ELSE 'under process'
		END AS order_status,
		foc.cnr_reason,
		ccsl.original_ucode AS original_ucode_desired, 
		cp.product_name,
		u.username AS agent_name,
		subs.substitute_ucode,
		dcp2.product_name AS substitute_product_name,
		dcp2.manufacturer_company AS substitute_manufacturer_company,
		dcp2.manufacturer_name AS substitute_manufacturer_name,
		--cp.therapy,
		MIN(ccsl.created_at) AS time_of_digitization,
		MAX(CASE WHEN ccsl.log_type=1 THEN 1 ELSE 0 END) AS substitution_opportunity_flag,
		MAX(CASE WHEN ccsl.log_type=2 THEN 1 ELSE 0 END) AS substitution_viewed_flag,
		MAX(CASE WHEN ccsl.log_type=3 THEN 1 ELSE 0 END) AS substitute_selected_flag,
		MAX(CASE WHEN mn.order_id IS NOT NULL THEN 1 ELSE 0 END) AS substitute_added_to_cart_flag,
		MAX(CASE WHEN mn.is_deleted=0 THEN 1 ELSE 0 END) AS substitute_retained_flag,
		MAX(CASE WHEN ri.id IS NOT NULL THEN 1 ELSE 0 END) AS substitute_returned_flag,
		MAX(CASE WHEN ri.return_reason=1 THEN 'Medicine No Longer Needed'
						 WHEN ri.return_reason=2 THEN 'Damaged Medicines'
						 WHEN ri.return_reason=3 THEN 'Wrong Medicine'
						 WHEN ri.return_reason=4 THEN 'Ice Pack Issue'
						 WHEN ri.return_reason=5 THEN 'Non-Veg Medicine'
						 WHEN ri.return_reason=6 THEN 'Expired Medicines'
						 WHEN ri.return_reason=7 THEN 'Additional Medicine Returned At PickUp'
						 WHEN ri.return_reason=8 THEN 'Extra Medicines'
					END) AS substitute_return_reason,
		MAX(CASE WHEN cii2.ucode=subs.substitute_ucode THEN 'dSubstituteOrderedAgain'
				 WHEN cii2.ucode=ccsl.original_ucode THEN 'cOriginalOrderedAgain'
				 WHEN o2.id IS NOT NULL AND cii2.id IS NULL THEN 'bNoneOrderedAgain'
				 WHEN o2.id IS NULL THEN 'aNeverOrderedAgain'
			END) AS ucode_reordered_flag
FROM pe2.cc_substitution_log ccsl
LEFT JOIN (
			SELECT order_id, original_ucode, substitute_ucode, user_id, created_at, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC) AS ranking
			FROM pe2.cc_substitution_log
			WHERE log_type=3
		) subs ON ccsl.order_id=subs.order_id AND ccsl.original_ucode=subs.original_ucode AND ccsl.user_id=subs.user_id AND ranking=1
LEFT JOIN analytics.d_catalog_product cp ON ccsl.original_ucode=cp.ucode
LEFT JOIN pe2."user" u ON ccsl.user_id=u.id
LEFT JOIN pe2.medicine_notes mn ON ccsl.order_id=mn.order_id AND ccsl.substitute_ucode=mn.ucode
LEFT JOIN data_model.f_order_consumer foc ON ccsl.order_id=foc.order_id
LEFT JOIN pe2.customer_invoices ci ON ccsl.order_id=ci.order_id
LEFT JOIN pe2.customer_invoice_items cii ON ci.id=cii.customer_invoice_id AND subs.substitute_ucode=cii.ucode
LEFT JOIN pe2.return_item ri ON cii.id=ri.invoice_item_id
LEFT JOIN pe2."order" o2 ON foc.customer_id=o2.customer_id AND o2.time_stamp>foc.order_placed_at
LEFT JOIN pe2.customer_invoices ci2 ON o2.id=ci2.order_id
LEFT JOIN pe2.customer_invoice_items cii2 ON ci2.id=cii2.customer_invoice_id AND (cii2.ucode=ccsl.original_ucode OR cii2.ucode=subs.substitute_ucode)
LEFT JOIN analytics.d_catalog_product dcp2 ON subs.substitute_ucode=dcp2.ucode
WHERE DATE(ccsl.created_at) BETWEEN '2019-03-01' AND '2019-05-15'
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,2,11;



------ query for automated email

SELECT ccsl.order_id, 
		ccsl.original_ucode AS original_ucode_desired, 
		cp.product_name,
		u.username AS agent_name,
		--cp.therapy,
		MIN(DATEADD(MIN,330,ccsl.created_at)) AS time_of_digitization,
		MAX(CASE WHEN ccsl.log_type=1 THEN 1 ELSE 0 END) AS substitution_opportunity_flag,
		MAX(CASE WHEN ccsl.log_type=2 THEN 1 ELSE 0 END) AS substitution_viewed_flag,
		MAX(CASE WHEN ccsl.log_type=3 THEN 1 ELSE 0 END) AS substitute_selected_flag,
		MAX(CASE WHEN mn.order_id IS NOT NULL THEN 1 ELSE 0 END) AS substitute_added_to_cart_flag,
		MAX(CASE WHEN mn.is_deleted=0 THEN 1 ELSE 0 END) AS substitute_retained_flag
FROM pe_pe2_pe2.cc_substitution_log ccsl
LEFT JOIN data_model.d_catalog_product cp ON ccsl.original_ucode=cp.ucode
LEFT JOIN pe_pe2_pe2."user" u ON ccsl.user_id=u.id
LEFT JOIN pe_pe2_pe2.medicine_notes mn ON ccsl.order_id=mn.order_id AND ccsl.substitute_ucode=mn.ucode
WHERE DATE(DATEADD(MIN,330,ccsl.created_at))=(CURRENT_DATE-1) AND ccsl.substitute_type=1
GROUP BY 1,2,3,4
ORDER BY 1,2,5;
