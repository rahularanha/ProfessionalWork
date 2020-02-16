
---- OOC AOV Analysis vs other platforms

SELECT order_source, 
		COUNT(order_id) AS total_orders, 
		ROUND(AVG(mrp))::INT AS AOV, 
		ROUND(AVG(discount_percentage::FLOAT),2) AS discount_percentage,
		SUM(chronic_flag_old) AS chronic_orders, 
		COUNT(CASE WHEN is_rx_required=true THEN order_id END) AS rx_req_orders,
		ROUND(AVG(total_line_items_delivered::FLOAT),3) AS avg_line_items_delivered,
		ROUND(AVG(total_line_items_delivered2::FLOAT),2) AS total_line_items_delivered2,
		SUM(total_quan)::FLOAT/COUNT(order_id) AS total_quantity_per_order,
		ROUND(AVG(total_line_items_deleted::FLOAT),3) AS avg_line_items_deleted,
		ROUND(AVG(avg_product_value),2) AS avg_product_value,
		ROUND(AVG(CASE WHEN is_rx_required=true THEN total_patients::FLOAT END),2) AS avg_patients,
		ROUND(AVG(patient_age))::INT AS avg_age_of_patient
		--AVG(total_meds_for_patient::FLOAT) AS total_meds_for_patient,
		--COUNT(patient_age) AS count_of_age_not_null
FROM (		
	SELECT abc.order_id,
			abc.order_source,
			abc.discount_percentage,
			abc.mrp, 
			abc.chronic_flag_old, 
			fo.is_rx_required,
			abc.total_line_items_delivered,
			abc.total_line_items_deleted,
			SUM(total_meds) AS total_line_items_delivered2,
			COUNT(abc.patient_id) AS total_patients,
			MIN(CASE WHEN ranking=1 THEN abc.age_of_patient END) AS patient_age,
			MIN(CASE WHEN ranking=1 THEN abc.total_meds END) AS total_meds_for_patient,
			SUM(total_quan) AS total_quan,
			SUM(medicine_total_cost)::FLOAT/SUM(total_meds) AS avg_product_value
	FROM (
		SELECT *, CASE WHEN age_of_patient IS NOT NULL THEN (ROW_NUMBER() OVER (PARTITION BY order_id, age_of_patient IS NOT NULL ORDER BY total_meds DESC)) END AS ranking
		FROM (		
			SELECT foc.order_id,
					foc.order_source,
					foc.discount_percentage,
					foc.mrp, 
					foc.chronic_flag_old, 
					foc.total_line_items_delivered,
					foc.total_line_items_deleted,
					rx.patient_id,
					FLOOR(EXTRACT(YEAR FROM order_placed_at)-p.year_of_birth) AS age_of_patient,
					COUNT(mn.ucode) AS total_meds,
					SUM(mn.full_quantity) AS total_quan,
					SUM(medicine_mrp) AS medicine_total_cost
			FROM data_model.f_order_consumer foc
			LEFT JOIN pe2.medicine_notes mn ON foc.order_id=mn.order_id AND mn.is_deleted=0
			LEFT JOIN pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
			LEFT JOIN pe2.digitization d ON mnd.digitization_id=d.id
			LEFT JOIN pe2.image i ON d.image_id=i.id
			LEFT JOIN pe2.rx ON i.rx_id=rx.id
			LEFT JOIN pe2.patient p ON rx.patient_id=p.id
			LEFT JOIN (
						SELECT ci.order_id, cii.ucode, AVG(cii.mrp) AS medicine_mrp
						FROM pe2.customer_invoices ci 
						LEFT JOIN pe2.customer_invoice_items cii ON ci.id=cii.customer_invoice_id 
						GROUP BY 1,2
			) cii ON mn.order_id=cii.order_id AND mn.ucode=cii.ucode
			WHERE DATE(order_placed_at) BETWEEN '2018-11-01' AND '2019-02-28' --AND chronic_flag_old=0
			GROUP BY 1,2,3,4,5,6,7,8,9
		)
	) abc
	LEFT JOIN data_model.f_order fo ON abc.order_id=fo.order_id
	GROUP BY 1,2,3,4,5,6,7,8
)
GROUP BY 1



----- basis metric complarisons of OOC vs other platforms

SELECT a.*, b.* 
FROM (
	SELECT foc1.customer_id,
			COUNT(DISTINCT foc1.order_id) AS ooc_orders,
			COUNT(DISTINCT CASE WHEN foc1.chronic_flag_old=1 THEN foc1.order_id END) AS ooc_chronic_orders,
			AVG(foc1.total_line_items_delivered::FLOAT) AS ooc_line_items_delivered,
			AVG(foc1.total_line_items_deleted::FLOAT) AS ooc_line_items_deleted,
			AVG(foc1.mrp) AS ooc_aov,
			COUNT(mn1.ucode) AS total_line_items_delivered,
			COUNT(CASE WHEN dcp1.is_chronic=0 THEN mn1.ucode END) AS total_acute_line_items
	FROM data_model.f_order_consumer foc1
	INNER JOIN data_model.customer_segmentation_raw_data csrd ON foc1.customer_id=csrd.customer_id AND csrd.registration_time>='2018-03-01'
	LEFT JOIN pe2.medicine_notes mn1 ON foc1.order_id=mn1.order_id AND mn1.is_deleted=0
	LEFT JOIN analytics.d_catalog_product dcp1 ON mn1.ucode=dcp1.ucode
	WHERE foc1.order_status_id IN (9,10) AND foc1.order_source='Order_On_Call'
	GROUP BY 1
) a
INNER JOIN (
	SELECT foc2.customer_id,
			COUNT(DISTINCT foc2.order_id) AS appweb_orders,
			COUNT(DISTINCT CASE WHEN foc2.chronic_flag_old=1 THEN foc2.order_id END) AS appweb_chronic_orders,
			AVG(foc2.total_line_items_delivered::FLOAT) AS appweb_line_items_delivered,
			AVG(foc2.total_line_items_deleted::FLOAT) AS ooc_line_items_deleted,
			AVG(foc2.mrp) AS appweb_aov,
			COUNT(mn2.ucode) AS total_line_items_delivered,
			COUNT(CASE WHEN dcp2.is_chronic=0 THEN mn2.ucode END) AS total_acute_line_items
	FROM data_model.f_order_consumer foc2
	INNER JOIN data_model.customer_segmentation_raw_data csrd ON foc2.customer_id=csrd.customer_id AND csrd.registration_time>='2018-03-01'
	LEFT JOIN pe2.medicine_notes mn2 ON foc2.order_id=mn2.order_id AND mn2.is_deleted=0
	LEFT JOIN analytics.d_catalog_product dcp2 ON mn2.ucode=dcp2.ucode
	WHERE foc2.order_status_id IN (9,10) AND foc2.order_source IN ('Mobile_Website','Android_App','Website','iOS_App')
	GROUP BY 1
) b ON a.customer_id=b.customer_id
ORDER BY 1



----- Order Frequency Comparison vs other platforms

SELECT customer_id, AVG(repeat_time) as order_frequency_in_hours
FROM (
	SELECT *, DATEDIFF(hour, order_placed_at, next_order_placed_at) AS repeat_time
	FROM (
		SELECT foc2.customer_id, foc2.order_id,
				foc2.order_placed_at,
				LEAD(foc2.order_placed_at,1) OVER (PARTITION BY foc2.customer_id ORDER BY foc2.order_placed_at) AS next_order_placed_at
		FROM data_model.f_order_consumer foc2
		INNER JOIN data_model.customer_segmentation_raw_data csrd ON foc2.customer_id=csrd.customer_id AND csrd.registration_time>='2018-03-01'			
		INNER JOIN (SELECT customer_id FROM data_model.f_order_consumer foc2 WHERE foc2.order_status_id IN (9,10) AND foc2.order_source='Order_On_Call' GROUP BY 1) a ON foc2.customer_id=a.customer_id
		INNER JOIN (SELECT customer_id FROM data_model.f_order_consumer foc2 WHERE foc2.order_status_id IN (9,10) AND foc2.order_source IN ('Mobile_Website','Android_App','Website','iOS_App') GROUP BY 1) b ON a.customer_id=b.customer_id
		--LEFT JOIN (SELECT customer_id FROM data_model.f_order_consumer foc2 WHERE foc2.order_status_id IN (9,10) AND foc2.order_source IN ('Third_Party_API', 'CMS') GROUP BY 1) c ON a.customer_id=c.customer_id
		WHERE foc2.order_source IN ('Order_On_Call','Mobile_Website','Android_App','Website','iOS_App') AND foc2.order_status_id IN (9,10)--c.customer_id IS NULL AND 
		)
	)
GROUP BY 1
ORDER BY 1;



---- Platform Migration Trend

SELECT customer_id, 
			COUNT(DISTINCT order_id) AS total_orders,
			COUNT(DISTINCT CASE WHEN order_source_bucket='OOC' THEN order_id END) AS ooc_orders,
			COUNT(DISTINCT CASE WHEN order_source_bucket='CP' THEN order_id END) AS cp_orders,
			MAX(CASE WHEN order_number=1 THEN order_source_bucket END) AS first_order_source,
			MAX(CASE WHEN order_number=2 THEN order_source_bucket END) AS second_order_source,
			MAX(CASE WHEN order_number=3 THEN order_source_bucket END) AS third_order_source,
			MAX(CASE WHEN order_number=4 THEN order_source_bucket END) AS fourth_order_source,
			MAX(CASE WHEN order_number=5 THEN order_source_bucket END) AS fifth_order_source,
			MAX(CASE WHEN order_number=6 THEN order_source_bucket END) AS sixth_order_source,
			MAX(CASE WHEN order_number=7 THEN order_source_bucket END) AS seventh_order_source,
			MAX(CASE WHEN order_number=8 THEN order_source_bucket END) AS eighth_order_source
	FROM (
		SELECT foc2.customer_id, foc2.order_id,
				foc2.order_placed_at, foc2.order_source,
				CASE WHEN foc2.order_source='Order_On_Call' THEN 'OOC' ELSE 'CP' END AS order_source_bucket, 
				ROW_NUMBER() OVER (PARTITION BY foc2.customer_id ORDER BY foc2.order_placed_at) AS order_number
		FROM data_model.f_order_consumer foc2
		INNER JOIN data_model.customer_segmentation_raw_data csrd ON foc2.customer_id=csrd.customer_id AND csrd.registration_time>='2018-10-01'			
		INNER JOIN (SELECT customer_id FROM data_model.f_order_consumer foc2 WHERE foc2.order_status_id IN (9,10) AND foc2.order_source='Order_On_Call' GROUP BY 1) a ON foc2.customer_id=a.customer_id
		INNER JOIN (SELECT customer_id FROM data_model.f_order_consumer foc2 WHERE foc2.order_status_id IN (9,10) AND foc2.order_source IN ('Mobile_Website','Android_App','Website','iOS_App') GROUP BY 1) b ON a.customer_id=b.customer_id
		LEFT JOIN (SELECT customer_id FROM data_model.f_order_consumer foc2 WHERE foc2.order_status_id IN (9,10) AND foc2.order_source IN ('Third_Party_API', 'CMS') GROUP BY 1) c ON a.customer_id=c.customer_id
		WHERE c.customer_id IS NULL AND foc2.order_source IS NOT NULL AND foc2.order_status_id IN (9,10)
		)
	GROUP BY 1
	ORDER BY 1
