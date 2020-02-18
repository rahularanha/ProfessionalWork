SELECT dpo.order_id, d.name AS doctor_name, oh.in_dp_at AS first_in_dp_at, oh.out_dp_at AS first_out_dp_at, oh_conf.min_confirmed_at,
		CASE WHEN o.status IN (9,10) THEN 'fulfilled'
			 WHEN o.status=2 THEN 'rejected'
			 WHEN o.status=8 THEN 'cancelled'
			 ELSE 'under process'
		END AS lastest_order_status,
		CASE WHEN dpo."source"=6 OR (dpo."source"=7 AND (oh.prev_status=1 OR prev_status IS NULL)) THEN 1 ELSE 0 END AS straight_to_dp_flag,
		CASE WHEN c.current_status_id=5 THEN 1 ELSE 0 END AS prescribed_in_docstat,
		CASE WHEN ocmn.order_id IS NOT NULL THEN 1 ELSE 0 END AS containing_custom_flag,
		CASE WHEN o.medicine_note IS NOT NULL THEN 1 ELSE 0 END AS containing_comment_flag,
		CASE WHEN call_req.order_id IS NOT NULL THEN 1 ELSE 0 END AS call_req_flag,
		CASE WHEN dpo.doctor_order_type=2 THEN 1 ELSE 0 END AS dp_issue_flag,
		CASE WHEN oh.next_status=5 THEN 1 ELSE 0 END AS cc_skipped_flag,
		CASE WHEN oh_conf.order_id IS NOT NULL THEN 1 ELSE 0 END AS order_confirmed_flag,
		MAX(CASE WHEN cdn.case_id iS NOT NULL THEN 1 ELSE 0 END) AS doc_comments_flag,
		MAX(CASE WHEN dcp.ucode IS NOT NULL THEN 1 ELSE 0 END) AS containing_infusion_flag,
		COUNT(DISTINCT CASE WHEN (mn.is_deleted=0 AND mn.create_time<oh.out_dp_at) OR (mn.is_deleted=1 AND mn.deleted_at>oh.out_dp_at) THEN mn.ucode END) AS atc_line_items_before_dp, 
		COUNT(DISTINCT pdp.prescribed_drug_id) AS dp_line_items,
		COUNT(DISTINCT CASE WHEN mn.is_deleted=0 OR mn.deleted_at>oh_conf.min_confirmed_at THEN mn.ucode END) AS atc_line_items_after_dp
FROM pe2.doctor_program_order dpo
INNER JOIN pe2."order" o ON dpo.order_id=o.id
INNER JOIN pe2.order_flags ordf ON dpo.order_id=ordf.order_id AND ordf.flag_id IN (22,23)
LEFT JOIN (SELECT order_id, order_status, "timestamp" AS in_dp_at, 
					LAG(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS prev_status,
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
			FROM pe2.order_history 
		) oh ON dpo.order_id=oh.order_id AND oh.dp_num=1 --AND dpo.created_at BETWEEN in_dp_at AND out_dp_at
LEFT JOIN pe2.medicine_notes mn ON dpo.order_id=mn.order_id
LEFT JOIN docstat."case" c ON dpo.order_id=c.order_id
LEFT JOIN docstat.doctor d ON c.doctor_id=d.id
LEFT JOIN docstat.patient_drug_prescriptions pdp ON c.id=pdp.case_id AND pdp.created_at BETWEEN oh.in_dp_at AND oh.out_dp_at
LEFT JOIN (SELECT order_id FROM pe2.order_customer_medicine_notes WHERE ucode IS NULL GROUP BY 1) ocmn ON dpo.order_id=ocmn.order_id
LEFT JOIN (SELECT order_id,MIN("timestamp") AS min_confirmed_at FROM pe2.order_history WHERE order_status=5 GROUP BY 1) oh_conf ON dpo.order_id=oh_conf.order_id
LEFT JOIN (SELECT order_id FROM pe2.order_flags WHERE flag_id=49 GROUP BY 1) call_req ON dpo.order_id=call_req.order_id
LEFT JOIN docstat.case_doctor_notes cdn ON c.id=cdn.case_id
LEFT JOIN analytics.d_catalog_product dcp ON mn.ucode=dcp.ucode AND dcp.dosage_form IN ('INJECTION','RESPULES','BOTTLE','CARTRIDGE','DRY VIAL','I M PRE-FILLED SYRINGE',
																						'I M PRE FILLED SYRINGE','I M VIAL','I M AMPOULE','I V AMPOULE','I V VIAL','INHALER',
																						'METERED-DOSE INHALER','INTRADERMAL VIAL','OPHTHAL PRE-FILLED SYRINGE','OPHTHAL VIAL',
																						'PRE-FILLED SYRINGE','PRE FILLED SYRINGE','PRE-FILLED AUTOINJECTOR','PRE-FILLED PEN',
																						'S C PRE-FILLED SYRINGE','S C VIAL','SYRINGE','VAIL','VIAL','PENFILL','FLEXPEN','S C AMPOULE',
																						'TOPICAL PRE-FILLED SYRINGE','GRANULE')
WHERE DATE(dpo.created_at) BETWEEN '2018-11-01' AND '2019-01-31'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 3;

SELECT dosage_form FROM analytics.d_catalog_product WHERE product_type='Medicine' GROUP BY 1 ORDER BY 1;

SELECT * FROM analytics.d_catalog_product;

SELECT * FROM docstat.case_doctor_notes LIMIT 100;


SELECT dpo.order_id, d.name AS doctor_name, oh.in_dp_at AS first_in_dp_at, oh.out_dp_at AS first_out_dp_at, oh_conf.min_confirmed_at,
		CASE WHEN o.status IN (9,10) THEN 'fulfilled'
			 WHEN o.status=2 THEN 'rejected'
			 WHEN o.status=8 THEN 'cancelled'
			 ELSE 'under process'
		END AS lastest_order_status,
		CASE WHEN dpo."source"=6 OR (dpo."source"=7 AND (oh.prev_status=1 OR prev_status IS NULL)) THEN 1 ELSE 0 END AS straight_to_dp_flag,
		CASE WHEN c.current_status_id=5 THEN 1 ELSE 0 END AS prescribed_in_docstat,
		CASE WHEN ocmn.order_id IS NOT NULL THEN 1 ELSE 0 END AS containing_custom_flag,
		CASE WHEN o.medicine_note IS NOT NULL THEN 1 ELSE 0 END AS containing_comment_flag,
		CASE WHEN call_req.order_id IS NOT NULL THEN 1 ELSE 0 END AS call_req_flag,
		CASE WHEN dpo.doctor_order_type=2 THEN 1 ELSE 0 END AS dp_issue_flag,
		CASE WHEN oh.next_status=5 THEN 1 ELSE 0 END AS cc_skipped_flag,
		CASE WHEN oh_conf.order_id IS NOT NULL THEN 1 ELSE 0 END AS order_confirmed_flag,
		MAX(CASE WHEN cdn.case_id iS NOT NULL THEN 1 ELSE 0 END) AS doc_comments_flag,
		MAX(CASE WHEN dcp.dosage_form IN ('INFUSION','INJECTION') THEN 1 ELSE 0 END) AS containing_infusion_flag,
		COUNT(DISTINCT CASE WHEN (mn.is_deleted=0 AND mn.create_time<oh.out_dp_at) OR (mn.is_deleted=1 AND mn.deleted_at>oh.out_dp_at) THEN mn.ucode END) AS atc_line_items_before_dp, 
		--COUNT(DISTINCT pdp.prescribed_drug_id) AS dp_line_items,
		COUNT(CASE WHEN ((mn.is_deleted=0 AND mn.create_time<oh.out_dp_at) OR (mn.is_deleted=1 AND mn.deleted_at>oh.out_dp_at)) AND dmi.ucode IS NOT NULL THEN dmi.ucode END) AS line_items_catered,
		COUNT(DISTINCT CASE WHEN mn.is_deleted=0 OR mn.deleted_at>oh_conf.min_confirmed_at THEN mn.ucode END) AS atc_line_items_after_dp
FROM pe2.doctor_program_order dpo
INNER JOIN pe2."order" o ON dpo.order_id=o.id
INNER JOIN pe2.order_flags ordf ON dpo.order_id=ordf.order_id AND ordf.flag_id IN (22,23)
LEFT JOIN (SELECT order_id, order_status, "timestamp" AS in_dp_at, 
					LAG(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS prev_status,
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
					LEAD(order_status,2) OVER (PARTITION BY order_id ORDER BY id) AS next_to_next_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
			FROM pe2.order_history 
		) oh ON dpo.order_id=oh.order_id AND oh.dp_num=1 --AND dpo.created_at BETWEEN in_dp_at AND out_dp_at
LEFT JOIN pe2.medicine_notes mn ON dpo.order_id=mn.order_id
LEFT JOIN ( SELECT oi.order_id,oi.image_id,dmi.digitization_id,dmi.ucode,dmi.created_at
			FROM pe2.order_image oi 
			LEFT JOIN pe2.digitization d ON oi.image_id=d.image_id 
			LEFT JOIN pe2.digitization_medicine_info dmi ON d.id=dmi.digitization_id
			WHERE oi.is_valid=1
			) dmi ON mn.order_id=dmi.order_id AND mn.ucode=dmi.ucode AND dmi.created_at<=oh.next_to_next_status
--LEFT JOIN pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
--LEFT JOIN pe2.digitization d ON mnd.digitization_id
--LEFT JOIN pe2.image i
LEFT JOIN docstat."case" c ON dpo.order_id=c.order_id
LEFT JOIN docstat.doctor d ON c.doctor_id=d.id
--LEFT JOIN docstat.patient_drug_prescriptions pdp ON c.id=pdp.case_id AND pdp.created_at BETWEEN oh.in_dp_at AND oh.out_dp_at
LEFT JOIN (SELECT order_id FROM pe2.order_customer_medicine_notes WHERE ucode IS NULL GROUP BY 1) ocmn ON dpo.order_id=ocmn.order_id
LEFT JOIN (SELECT order_id,MIN("timestamp") AS min_confirmed_at FROM pe2.order_history WHERE order_status=5 GROUP BY 1) oh_conf ON dpo.order_id=oh_conf.order_id
LEFT JOIN (SELECT order_id FROM pe2.order_flags WHERE flag_id=49 GROUP BY 1) call_req ON dpo.order_id=call_req.order_id
LEFT JOIN docstat.case_doctor_notes cdn ON c.id=cdn.case_id
LEFT JOIN analytics.d_catalog_product dcp ON mn.ucode=dcp.ucode
WHERE DATE(dpo.created_at) BETWEEN '2018-11-01' AND '2019-01-31' AND (dcp.is_rx_required=1 OR dcp.is_rx_required IS NULL)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 3;



---------------- orders coming back to queue

DROP TABLE dp_mismatch_superset;

CREATE TEMPORARY TABLE dp_mismatch_superset AS
SELECT dpo.order_id, d.name AS doctor_name, oh.in_dp_at AS first_in_dp_at, oh.out_dp_at AS first_out_dp_at,
		CASE WHEN o.status IN (9,10) THEN 'fulfilled'
			 WHEN o.status=2 THEN 'rejected'
			 WHEN o.status=8 THEN 'cancelled'
			 ELSE 'under process'
		END AS lastest_order_status,
		CASE WHEN c.current_status_id=5 THEN 1 ELSE 0 END AS prescribed_in_docstat,
		CASE WHEN ocmn.order_id IS NOT NULL THEN 1 ELSE 0 END AS containing_custom_flag,
		CASE WHEN o.medicine_note IS NOT NULL THEN 1 ELSE 0 END AS containing_comment_flag,
		CASE WHEN call_req.order_id IS NOT NULL THEN 1 ELSE 0 END AS call_req_flag,
		CASE WHEN dpo.doctor_order_type=2 THEN 1 ELSE 0 END AS dp_issue_flag,
		CASE WHEN oh.next_status=5 THEN 1 ELSE 0 END AS cc_skipped_flag,
		CASE WHEN oh_conf.order_id IS NOT NULL THEN 1 ELSE 0 END AS order_confirmed_flag,
		COUNT(DISTINCT CASE WHEN (mn.is_deleted=0 AND mn.create_time<oh.out_dp_at) OR (mn.is_deleted=1 AND mn.deleted_at>oh.out_dp_at) THEN mn.ucode END) AS atc_line_items_before_dp, 
		COUNT(DISTINCT pdp.prescribed_drug_id) AS dp_line_items,
		COUNT(DISTINCT CASE WHEN mn.is_deleted=0 OR mn.deleted_at>oh_conf.min_confirmed_at THEN mn.ucode END) AS atc_line_items_after_dp
FROM pe2.doctor_program_order dpo
INNER JOIN pe2."order" o ON dpo.order_id=o.id
INNER JOIN pe2.order_flags ordf ON dpo.order_id=ordf.order_id AND ordf.flag_id IN (22,23)
LEFT JOIN (SELECT order_id, order_status, "timestamp" AS in_dp_at, 
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS next_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
			FROM pe2.order_history 
		) oh ON dpo.order_id=oh.order_id AND oh.dp_num=1 --AND dpo.created_at BETWEEN in_dp_at AND out_dp_at
LEFT JOIN pe2.medicine_notes mn ON dpo.order_id=mn.order_id
LEFT JOIN docstat."case" c ON dpo.order_id=c.order_id
LEFT JOIN docstat.doctor d ON c.doctor_id=d.id
LEFT JOIN docstat.patient_drug_prescriptions pdp ON c.id=pdp.case_id AND pdp.created_at BETWEEN oh.in_dp_at AND oh.out_dp_at
LEFT JOIN (SELECT order_id FROM pe2.order_customer_medicine_notes WHERE ucode IS NULL GROUP BY 1) ocmn ON dpo.order_id=ocmn.order_id
LEFT JOIN (SELECT order_id,MIN("timestamp") AS min_confirmed_at FROM pe2.order_history WHERE order_status=5 GROUP BY 1) oh_conf ON dpo.order_id=oh_conf.order_id
LEFT JOIN (SELECT order_id FROM pe2.order_flags WHERE flag_id=49 GROUP BY 1) call_req ON dpo.order_id=call_req.order_id
WHERE DATE(dpo.created_at) BETWEEN '2018-11-01' AND '2019-01-31'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 3;


---- orders affected by prefetch logic after placement

SELECT EXTRACT(YEAR FROM o.time_stamp) AS year_of_order_placed, EXTRACT(MONTH FROM o.time_stamp) AS month_of_order_placed, 
		COUNT(dpo.order_id) AS total_orders,
		COUNT(CASE WHEN dpo."source"=7 AND oh.prev_status=1 THEN dpo.order_id END) AS affected_orders
		--COUNT(CASE WHEN o.status IN (9,10) THEN dpo.order_id END) AS orders_fulfilled,
		--COUNT(CASE WHEN dpo."source"=7 AND oh.prev_status=1 AND o.status IN (9,10) THEN dpo.order_id END) AS affected_orders_fulfilled
FROM pe2."order" o
INNER JOIN pe2.doctor_program_order dpo ON o.id=dpo.order_id
LEFT JOIN (SELECT order_id, order_status, "timestamp" AS in_dp_at, 
					LAG(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS prev_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
			FROM pe2.order_history 
		) oh ON dpo.order_id=oh.order_id AND oh.dp_num=1 
WHERE DATE(o.time_stamp) BETWEEN '2018-11-01' AND '2019-01-31' AND (retailer_id NOT IN (12,80,82,115,85,69,63) OR retailer_id IS NULL)
GROUP BY 1,2;



SELECT dpo.order_id, o.time_stamp AS order_placed_at
FROM pe2."order" o
INNER JOIN pe2.doctor_program_order dpo ON o.id=dpo.order_id
LEFT JOIN (SELECT order_id, order_status, "timestamp" AS in_dp_at, 
					LAG(order_status,1) OVER (PARTITION BY order_id ORDER BY id) AS prev_status,
					LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_dp_at,
					CASE WHEN order_status=49 THEN ROW_NUMBER() OVER (PARTITION BY order_id,order_status=49 ORDER BY id) END AS dp_num
			FROM pe2.order_history 
		) oh ON dpo.order_id=oh.order_id AND oh.dp_num=1 
WHERE (DATE(o.time_stamp) BETWEEN '2018-11-01' AND '2019-01-31') AND
		dpo."source"=7 AND 
		oh.prev_status=1 AND 
		(retailer_id NOT IN (12,80,82,115,85,69,63) OR retailer_id IS NULL)
GROUP BY 1,2
ORDER BY 2
