CREATE TEMPORARY TABLE wmrt AS
SELECT fo.order_id,	
		fo.supplier_city_name AS supplier_city,
		fo.order_placed_at,
		fo.delivered_at,		
		DATEADD(MIN,330,r.created_at) AS return_raised_at, 
		CASE WHEN roi."source" IS NOT NULL 
			 THEN CASE roi."source" WHEN 1 THEN 'CMS'
							        WHEN 2 THEN 'APP/Web-Customer'
				  END
		END AS return_raised_source,
		u1.username AS return_raised_agent_name,
		ur1.name AS return_raised_agent_role,
		cii.ucode AS invoiced_ucode, 
		rwid.received_med_ucode AS delivered_med_ucode, 
		rwid.desired_med_ucode,  
		cii.medicine_name AS invoiced_med_name,
		dcp1.product_name AS delivered_med_name,
		dcp2.product_name AS desired_med_name,
		CASE WHEN u2.username IS NULL THEN 'APP/Web-Customer' ELSE 'CMS' END AS digitization_source,
		u2.username AS digitized_by_agent_name,
		ur2.name AS digitized_by_agent_role,  
		CASE WHEN rwid.id IS NOT NULL 
			 THEN CASE WHEN cii.ucode=rwid.desired_med_ucode THEN 'Barcoding/Picker-WMS' 
			 		   ELSE CASE WHEN mdd."source" IN (1,2,3) THEN 'Digitization-Customer'
			 		   			 WHEN mdd."source"=11 THEN 'Digitization-WMS'
			 		   			 WHEN fdpo.order_id IS NOT NULL AND LEFT(i.image_name,3)='dp_' AND (DATEADD(MIN,330,i.create_time)>fo.order_placed_at AND DATEADD(MIN,330,mn.create_time)>=fdpo.rx_first_prescribed_at) THEN 'Digitization-DP'
			 		   			 ELSE 'Digitization-CC'
			 		   		END
			 	  END
		END AS error_responsibility
FROM pe_pe2_pe2."return" r
LEFT JOIN pe_pe2_pe2.return_item ri ON r.id=ri.return_id
LEFT JOIN pe_pe2_pe2.return_origin_info roi ON r.id=roi.return_id
LEFT JOIN pe_pe2_pe2."user" u1 ON roi.user_id=u1.id AND roi."source"=1
LEFT JOIN pe_pe2_pe2.user_roles ur1 ON u1.role_id=ur1.id
LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ri.invoice_item_id=cii.id
LEFT JOIN pe_pe2_pe2.return_wrong_item_detail rwid ON ri.id=rwid.return_item_id
LEFT JOIN pe_pe2_pe2.medicine_notes mn ON r.order_id=mn.order_id AND cii.ucode=mn.ucode
LEFT JOIN pe_pe2_pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
LEFT JOIN pe_pe2_pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id
LEFT JOIN pe_pe2_pe2.digitization_origin_info doi ON mnd.digitization_id=doi.digitization_id
LEFT JOIN pe_pe2_pe2.digitization d ON doi.digitization_id=d.id
LEFT JOIN pe_pe2_pe2.image i ON d.image_id=i.id
LEFT JOIN pe_pe2_pe2."user" u2 ON mdd.user_id=u2.id
LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u2.role_id=ur2.id
LEFT JOIN data_model.d_catalog_product dcp1 ON rwid.received_med_ucode=dcp1.ucode
LEFT JOIN data_model.d_catalog_product dcp2 ON rwid.desired_med_ucode=dcp2.ucode
LEFT JOIN data_model.f_order fo ON r.order_id=fo.order_id
LEFT JOIN data_model.f_doctor_program_order fdpo ON fo.order_id=fdpo.order_id
WHERE ri.return_reason=3 AND DATE(DATEADD(MIN,330,r.created_at))=(CURRENT_DATE-1) -- BETWEEN '2019-06-01' AND '2019-06-15'  ---rwid.id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 5;



SELECT * FROM wmrt WHERE error_responsibility='Digitization-CC';




--- CC
		
SELECT fo.order_id,	
--		fo.supplier_city_name AS supplier_city,
--		fo.order_placed_at,
--		fo.delivered_at,		
		DATEADD(MIN,330,r.created_at) AS return_raised_at, 
--		CASE WHEN roi."source" IS NOT NULL 
--			 THEN CASE roi."source" WHEN 1 THEN 'CMS'
--							        WHEN 2 THEN 'APP/Web-Customer'
--				  END
--		END AS return_raised_source,
--		u1.username AS return_raised_agent_name,
--		ur1.name AS return_raised_agent_role,
		cii.ucode AS invoiced_ucode, 
		rwid.received_med_ucode AS delivered_med_ucode, 
		rwid.desired_med_ucode,  
		cii.medicine_name AS invoiced_med_name,
		dcp1.product_name AS delivered_med_name,
		dcp2.product_name AS desired_med_name,
--		CASE WHEN u2.username IS NULL THEN 'APP/Web-Customer' ELSE 'CMS' END AS digitization_source,
		u2.username AS digitized_by_agent_name,
		ur2.name AS digitized_by_agent_role
--		CASE WHEN rwid.id IS NOT NULL 
--			 THEN CASE WHEN cii.ucode=rwid.desired_med_ucode THEN 'Barcoding/Picker-WMS' 
--			 		   ELSE CASE WHEN u2.username IS NULL THEN 'Digitization-Customer'  
--			 		   			 ELSE CASE WHEN DATEADD(MIN,330,mn.create_time)>fdpo.rx_first_prescribed_at THEN 'Digitization-Docstat' 
--			 		   			 		   ELSE 'Digitization-CC' 
--			 		   			 	  END
--			 		   		END
--			 	  END
--		END AS error_responsibility
FROM pe_pe2_pe2."return" r
LEFT JOIN pe_pe2_pe2.return_item ri ON r.id=ri.return_id
LEFT JOIN pe_pe2_pe2.return_origin_info roi ON r.id=roi.return_id
LEFT JOIN pe_pe2_pe2."user" u1 ON roi.user_id=u1.id AND roi."source"=1
LEFT JOIN pe_pe2_pe2.user_roles ur1 ON u1.role_id=ur1.id
LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ri.invoice_item_id=cii.id
LEFT JOIN pe_pe2_pe2.return_wrong_item_detail rwid ON ri.id=rwid.return_item_id
LEFT JOIN pe_pe2_pe2.medicine_notes mn ON r.order_id=mn.order_id AND cii.ucode=mn.ucode
LEFT JOIN pe_pe2_pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
LEFT JOIN pe_pe2_pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id
LEFT JOIN pe_pe2_pe2.digitization_origin_info doi ON mnd.digitization_id=doi.digitization_id
LEFT JOIN pe_pe2_pe2.digitization d ON doi.digitization_id=d.id
LEFT JOIN pe_pe2_pe2.image i ON d.image_id=i.id
LEFT JOIN pe_pe2_pe2."user" u2 ON mdd.user_id=u2.id
LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u2.role_id=ur2.id
LEFT JOIN data_model.d_catalog_product dcp1 ON rwid.received_med_ucode=dcp1.ucode
LEFT JOIN data_model.d_catalog_product dcp2 ON rwid.desired_med_ucode=dcp2.ucode
LEFT JOIN data_model.f_order fo ON r.order_id=fo.order_id
LEFT JOIN data_model.f_doctor_program_order fdpo ON fo.order_id=fdpo.order_id
--LEFT JOIN pe_docstat_91streets_media_technologies."case" c ON fdpo.order_id=c.order_id
--LEFT JOIN pe_docstat_91streets_media_technologies.doctor doc ON c.doctor_id=doc.id
WHERE ri.return_reason=3 AND DATE(DATEADD(MIN,330,r.created_at))=(CURRENT_DATE-1) 
		AND cii.ucode!=rwid.desired_med_ucode 
		AND mdd."source" IN (0,5,6,7,8,10)  ---mdd."source" NOT IN (1,2,3,4,11)
		AND (fdpo.order_id IS NULL OR (fdpo.order_id IS NOT NULL AND ((LEFT(i.image_name,3)='dp_' AND (DATEADD(MIN,330,i.create_time)<=fo.order_placed_at OR mn.create_time<fdpo.rx_first_prescribed_at)) OR LEFT(i.image_name,3)!='dp_' OR i.image_name IS NULL)))
		--AND (mdd."source" NOT IN (1,2,3,4,11) OR (fdpo.order_id IS NOT NULL AND (LEFT(i.image_name,3)!='dp_' OR i.image_name IS NULL)))
GROUP BY 1,2,3,4,5,6,7,8,9,10--,11,12,13,14,15,16,17,18
ORDER BY 2;

	


--- DP
			 	  
SELECT fo.order_id,	
--		fo.supplier_city_name AS supplier_city,
--		fo.order_placed_at,
--		fo.delivered_at,		
		DATEADD(MIN,330,r.created_at) AS return_raised_at, 
--		CASE WHEN roi."source" IS NOT NULL 
--			 THEN CASE roi."source" WHEN 1 THEN 'CMS'
--							        WHEN 2 THEN 'APP/Web-Customer'
--				  END
--		END AS return_raised_source,
--		u1.username AS return_raised_agent_name,
--		ur1.name AS return_raised_agent_role,
		cii.ucode AS invoiced_ucode, 
		rwid.received_med_ucode AS delivered_med_ucode, 
		rwid.desired_med_ucode,  
		cii.medicine_name AS invoiced_med_name,
		dcp1.product_name AS delivered_med_name,
		dcp2.product_name AS desired_med_name,
--		CASE WHEN u2.username IS NULL THEN 'APP/Web-Customer' ELSE 'CMS' END AS digitization_source,
		fdpo.doctor_category,
		doc.name AS doctor_name
--		u2.username AS digitized_by_agent_name,
--		ur2.name AS digitized_by_agent_role,  
--		CASE WHEN rwid.id IS NOT NULL 
--			 THEN CASE WHEN cii.ucode=rwid.desired_med_ucode THEN 'Barcoding/Picker-WMS' 
--			 		   ELSE CASE WHEN u2.username IS NULL THEN 'Digitization-Customer'  ELSE 'Digitization-CC/Docstat' END
--			 	  END
--		END AS error_responsibility
FROM pe_pe2_pe2."return" r
LEFT JOIN pe_pe2_pe2.return_item ri ON r.id=ri.return_id
LEFT JOIN pe_pe2_pe2.return_origin_info roi ON r.id=roi.return_id
LEFT JOIN pe_pe2_pe2."user" u1 ON roi.user_id=u1.id AND roi."source"=1
LEFT JOIN pe_pe2_pe2.user_roles ur1 ON u1.role_id=ur1.id
LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ri.invoice_item_id=cii.id
LEFT JOIN pe_pe2_pe2.return_wrong_item_detail rwid ON ri.id=rwid.return_item_id
--LEFT JOIN pe_pe2_pe2.retailer_return_task rrt ON ri.return_id=rrt.return_id	
--LEFT JOIN pe_pe2_pe2.order_issue oi ON rrt.issue_id=oi.id AND oi."type"=1
LEFT JOIN pe_pe2_pe2.medicine_notes mn ON r.order_id=mn.order_id AND cii.ucode=mn.ucode
LEFT JOIN pe_pe2_pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
LEFT JOIN pe_pe2_pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id
LEFT JOIN pe_pe2_pe2.digitization_origin_info doi ON mnd.digitization_id=doi.digitization_id
LEFT JOIN pe_pe2_pe2.digitization d ON doi.digitization_id=d.id
LEFT JOIN pe_pe2_pe2.image i ON d.image_id=i.id
LEFT JOIN pe_pe2_pe2."user" u2 ON doi.user_id=u2.id
LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u2.role_id=ur2.id
LEFT JOIN data_model.d_catalog_product dcp1 ON rwid.received_med_ucode=dcp1.ucode
LEFT JOIN data_model.d_catalog_product dcp2 ON rwid.desired_med_ucode=dcp2.ucode
LEFT JOIN data_model.f_order fo ON r.order_id=fo.order_id
LEFT JOIN data_model.f_doctor_program_order fdpo ON fo.order_id=fdpo.order_id
LEFT JOIN pe_docstat_91streets_media_technologies."case" c ON fdpo.order_id=c.order_id
LEFT JOIN pe_docstat_91streets_media_technologies.doctor doc ON c.doctor_id=doc.id
WHERE ri.return_reason=3 AND DATE(DATEADD(MIN,330,r.created_at))=(CURRENT_DATE-1)
		AND cii.ucode!=rwid.desired_med_ucode 
		AND mdd."source" NOT IN (1,2,3,10)
		AND fdpo.order_id IS NOT NULL AND LEFT(i.image_name,3)='dp_' AND (mdd."source"=4 OR (DATEADD(MIN,330,i.create_time)>fo.order_placed_at AND DATEADD(MIN,330,mn.create_time)>=fdpo.rx_first_prescribed_at))
		-- BETWEEN '2019-06-01' AND '2019-06-15'  ---rwid.id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10--,11,12,13--,14,15,16,17,18
ORDER BY 5;





----- Overall

CREATE TEMPORARY TABLE wmrt_overall AS
SELECT fo.order_id,	
		fo.supplier_city_name AS supplier_city,
		fo.order_placed_at,
		fo.delivered_at,		
		DATEADD(MIN,330,r.created_at) AS return_raised_at, 
		CASE WHEN roi."source" IS NOT NULL 
			 THEN CASE roi."source" WHEN 1 THEN 'CMS'
							        WHEN 2 THEN 'APP/Web-Customer'
				  END
		END AS return_raised_source,
		u1.username AS return_raised_agent_name,
		ur1.name AS return_raised_agent_role,
		cii.ucode AS invoiced_ucode, 
		rwid.received_med_ucode AS delivered_med_ucode, 
		rwid.desired_med_ucode,  
		cii.medicine_name AS invoiced_med_name,
		dcp1.product_name AS delivered_med_name,
		dcp2.product_name AS desired_med_name,
		CASE WHEN u2.username IS NULL THEN 'APP/Web-Customer' ELSE 'CMS' END AS digitization_source,
		u2.username AS digitized_by_agent_name,
		ur2.name AS digitized_by_agent_role,  
		CASE WHEN rwid.id IS NOT NULL 
			 THEN CASE WHEN cii.ucode=rwid.desired_med_ucode THEN 'Barcoding/Picker-WMS' 
			 		   ELSE CASE WHEN mdd."source" IN (1,2,3) THEN 'Digitization-Customer'
			 		   			 WHEN mdd."source"=11 THEN 'Digitization-WMS'
			 		   			 WHEN fdpo.order_id IS NOT NULL AND LEFT(i.image_name,3)='dp_' AND (mdd."source"=4 OR (DATEADD(MIN,330,i.create_time)>fo.order_placed_at AND DATEADD(MIN,330,mn.create_time)>=fdpo.rx_first_prescribed_at)) THEN 'Digitization-DP'
			 		   			 ELSE 'Digitization-CC'
			 		   		END
			 	  END
		END AS error_responsibility
FROM pe_pe2_pe2."return" r
LEFT JOIN pe_pe2_pe2.return_item ri ON r.id=ri.return_id
LEFT JOIN pe_pe2_pe2.return_origin_info roi ON r.id=roi.return_id
LEFT JOIN pe_pe2_pe2."user" u1 ON roi.user_id=u1.id AND roi."source"=1
LEFT JOIN pe_pe2_pe2.user_roles ur1 ON u1.role_id=ur1.id
LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ri.invoice_item_id=cii.id
LEFT JOIN pe_pe2_pe2.return_wrong_item_detail rwid ON ri.id=rwid.return_item_id
LEFT JOIN pe_pe2_pe2.medicine_notes mn ON r.order_id=mn.order_id AND cii.ucode=mn.ucode
LEFT JOIN pe_pe2_pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
LEFT JOIN pe_pe2_pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id
LEFT JOIN pe_pe2_pe2.digitization_origin_info doi ON mnd.digitization_id=doi.digitization_id
LEFT JOIN pe_pe2_pe2.digitization d ON doi.digitization_id=d.id
LEFT JOIN pe_pe2_pe2.image i ON d.image_id=i.id
LEFT JOIN pe_pe2_pe2."user" u2 ON mdd.user_id=u2.id
LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u2.role_id=ur2.id
LEFT JOIN data_model.d_catalog_product dcp1 ON rwid.received_med_ucode=dcp1.ucode
LEFT JOIN data_model.d_catalog_product dcp2 ON rwid.desired_med_ucode=dcp2.ucode
LEFT JOIN data_model.f_order fo ON r.order_id=fo.order_id
LEFT JOIN data_model.f_doctor_program_order fdpo ON fo.order_id=fdpo.order_id
WHERE ri.return_reason=3 AND DATE(DATEADD(MIN,330,r.created_at))=(CURRENT_DATE-1) -- BETWEEN '2019-06-01' AND '2019-06-15'  ---rwid.id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 5;



SELECT error_responsibility, COUNT(order_id) FROM wmrt_overall GROUP BY 1 ORDER BY 2 DESC;
SELECT order_id FROM wmrt_overall WHERE error_responsibility='Digitization-CC';

--- WMS

SELECT fo.order_id,	
		fo.supplier_city_name AS supplier_city,
--		fo.order_placed_at,
--		fo.delivered_at,	
		fo.delivery_city_name,
		fo.retailer_name,
		DATEADD(MIN,330,r.created_at) AS return_raised_at, 
--		CASE WHEN roi."source" IS NOT NULL 
--			 THEN CASE roi."source" WHEN 1 THEN 'CMS'
--							        WHEN 2 THEN 'APP/Web-Customer'
--				  END
--		END AS return_raised_source,
--		u1.username AS return_raised_agent_name,
--		ur1.name AS return_raised_agent_role,
		CASE WHEN r.status=1 THEN 'return_request_raised'
			WHEN r.status=2 THEN 'return_pickup_task_created'
			WHEN r.status=3 THEN 'return_pickup_completed'
			WHEN r.status=4 THEN 'return_reconciled'
			WHEN r.status=5 THEN 'return_request_cancelled'
			WHEN r.status=6 THEN 'return_request_declined'	
		END AS return_status,
		cii.ucode AS invoiced_ucode, 
		rwid.received_med_ucode AS delivered_med_ucode,  
		rwid.desired_med_ucode,  
		cii.medicine_name AS invoiced_med_name,
		dcp1.product_name AS delivered_med_name,
		dcp2.product_name AS desired_med_name,
		dcp1.product_mrp AS delivered_med_mrp,
		SUM(ri.qty_raised) AS quantity_delivered,
		CASE WHEN cii.ucode=rwid.desired_med_ucode THEN 'Barcoding/Picker' ELSE 'Digitization' END AS error_reason
--		CASE WHEN u2.username IS NULL THEN 'APP/Web-Customer' ELSE 'CMS' END AS digitization_source,
--		u2.username AS digitized_by_agent_name,
--		ur2.name AS digitized_by_agent_role,  
--		CASE WHEN rwid.id IS NOT NULL 
--			 THEN CASE WHEN cii.ucode=rwid.desired_med_ucode THEN 'Barcoding/Picker-WMS' 
--			 		   ELSE CASE WHEN u2.username IS NULL THEN 'Digitization-Customer'  ELSE 'Digitization-CC/Docstat' END
--			 	  END
--		END AS error_responsibility
FROM pe_pe2_pe2."return" r
LEFT JOIN pe_pe2_pe2.return_item ri ON r.id=ri.return_id
LEFT JOIN pe_pe2_pe2.return_origin_info roi ON r.id=roi.return_id
LEFT JOIN pe_pe2_pe2."user" u1 ON roi.user_id=u1.id AND roi."source"=1
LEFT JOIN pe_pe2_pe2.user_roles ur1 ON u1.role_id=ur1.id
LEFT JOIN pe_pe2_pe2.customer_invoice_items cii ON ri.invoice_item_id=cii.id
LEFT JOIN pe_pe2_pe2.return_wrong_item_detail rwid ON ri.id=rwid.return_item_id
LEFT JOIN pe_pe2_pe2.medicine_notes mn ON r.order_id=mn.order_id AND cii.ucode=mn.ucode
--LEFT JOIN pe_pe2_pe2.medicine_notes_digitization mnd ON mn.id=mnd.medicine_notes_id
LEFT JOIN pe_pe2_pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id
--LEFT JOIN pe_pe2_pe2.digitization_origin_info doi ON mnd.digitization_id=doi.digitization_id
--LEFT JOIN pe_pe2_pe2.digitization d ON doi.digitization_id=d.id
--LEFT JOIN pe_pe2_pe2.image i ON d.image_id=i.id
--LEFT JOIN pe_pe2_pe2."user" u2 ON doi.user_id=u2.id
--LEFT JOIN pe_pe2_pe2.user_roles ur2 ON u2.role_id=ur2.id
LEFT JOIN data_model.d_catalog_product dcp1 ON rwid.received_med_ucode=dcp1.ucode
LEFT JOIN data_model.d_catalog_product dcp2 ON rwid.desired_med_ucode=dcp2.ucode
LEFT JOIN data_model.f_order fo ON r.order_id=fo.order_id
WHERE ri.return_reason=3 AND DATE(DATEADD(MIN,330,r.created_at))=(CURRENT_DATE-1) 
		AND (cii.ucode=rwid.desired_med_ucode OR (cii.ucode!=rwid.desired_med_ucode AND mdd."source"=11))-- BETWEEN '2019-06-01' AND '2019-06-15'  ---rwid.id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,15
ORDER BY 5;
