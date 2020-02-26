SELECT order_id, customer_id, order_placed_at, order_status,
                COUNT(ucode) AS num_of_rx_req_line_items,
                COUNT(CASE WHEN line_item_digitization_source='caller' THEN ucode END) AS line_items_digitized_from_caller,
                SUM(line_item_prefetchable) AS line_items_prefetchable
FROM
            (SELECT o.id AS order_id, o.customer_id, o.time_stamp AS order_placed_at,
                    CASE WHEN o.status IN (9,10) THEN 'Fulfilled' WHEN o.status IN (2,8) THEN 'Cancelled/Rejected' ELSE 'Under Process' END AS order_status,
                    mn.ucode, mn.create_time AS line_item_created_at, mn.updated_at AS line_item_updated_at,
                    CASE WHEN mdd.source=6 THEN 'caller' ELSE 'other' END AS line_item_digitization_source,
                    MAX(CASE WHEN dmi.ucode IS NOT NULL THEN 1 ELSE 0 END) AS line_item_prefetchable
            FROM pe2."order" o
            LEFT JOIN (SELECT order_id FROM pe2.custom_medicine_mapping WHERE ucode IS NULL AND DATE(created_at)>='2018-06-01' GROUP BY 1) cust ON o.id=cust.order_id
            LEFT JOIN pe2.medicine_notes mn ON o.id=mn.order_id AND DATE(mn.create_time)>='2018-06-01'
            LEFT JOIN pe2.order_digitization_flag odf ON mn.id=odf.medicine_notes_id AND odf.flag_id=34 AND DATE(odf.created_at)>='2018-06-01'
            LEFT JOIN pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id AND DATE(mdd.created_at)>='2018-06-01'
            --LEFT JOIN pe2.order_image oi ON o.id=oi.order_id AND oi.is_duplicate=0 AND oi.is_valid=1
            --LEFT JOIN pe2.image i1 ON oi.image_id=i1.id AND i1.image_name NOT LIKE 'dp%'
            --LEFT JOIN pe2.digitization d1 ON i1.id=d1.image_id
            --LEFT JOIN pe2.digitization_medicine_info dmi1 ON d1.id=dmi1.digitization_id
            LEFT JOIN pe2.rx ON o.customer_id=rx.customer_id AND (rx.doctor_name IS NOT NULL OR rx.hospital_name IS NOT NULL) AND (rx.patient_id IS NOT NULL)
            LEFT JOIN pe2.image i ON rx.id=i.rx_id AND i.create_time<o.time_stamp
            LEFT JOIN pe2.digitization d ON i.id=d.image_id
            LEFT JOIN pe2.digitization_medicine_info dmi ON d.id=dmi.digitization_id AND dmi.ucode=mn.ucode --contains ucodes that need to be incorporated in the join
            WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31' AND cust.order_id IS NULL AND odf.medicine_notes_id IS NULL AND mn.ucode IS NOT NULL
            GROUP BY 1,2,3,4,5,6,7,8
			)
GROUP BY 1,2,3,4
ORDER BY 1 DESC;

grant select on all tables in schema pe2 to rahul.aranha;


SELECT o.id AS order_id, o.customer_id, o.time_stamp AS order_placed_at,
                    CASE WHEN o.status IN (9,10) THEN 'Fulfilled' WHEN o.status IN (2,8) THEN 'Cancelled/Rejected' ELSE 'Under Process' END AS order_status,
                    mn.id AS mn_id, mn.ucode, mn.create_time AS line_item_created_at, mn.updated_at AS line_item_updated_at,
                    CASE WHEN mdd.source=6 THEN 'caller' ELSE 'other' END AS line_item_digitization_source, rx.id AS rx_id
    FROM pe2."order" o
    LEFT JOIN (SELECT order_id FROM pe2.custom_medicine_mapping WHERE ucode IS NULL AND DATE(created_at)>='2018-06-01' GROUP BY 1) cust ON o.id=cust.order_id
    LEFT JOIN pe2.medicine_notes mn ON o.id=mn.order_id AND DATE(mn.create_time)>='2018-06-01'
    LEFT JOIN pe2.order_digitization_flag odf ON mn.id=odf.medicine_notes_id AND odf.flag_id=34 AND DATE(odf.created_at)>='2018-06-01'
    LEFT JOIN pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id AND DATE(mdd.created_at)>='2018-06-01'
    LEFT JOIN pe2.rx ON o.customer_id=rx.customer_id AND (rx.doctor_name IS NOT NULL OR rx.hospital_name IS NOT NULL) AND (rx.patient_id IS NOT NULL)
    WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31' AND cust.order_id IS NULL AND odf.medicine_notes_id IS NULL AND mn.ucode IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10;

INSERT INTO adhoc_analytics.rahul_aranha (
	SELECT o.id AS order_id, o.customer_id, o.time_stamp AS order_placed_at,
                    CASE WHEN o.status IN (9,10) THEN 'Fulfilled' WHEN o.status IN (2,8) THEN 'Cancelled/Rejected' ELSE 'Under Process' END AS order_status,
                    mn.id AS mn_id, mn.ucode, mn.create_time AS line_item_created_at, mn.updated_at AS line_item_updated_at,
                    CASE WHEN mdd.source=6 THEN 'caller' ELSE 'other' END AS line_item_digitization_source, rx.id AS rx_id
    FROM pe2."order" o
    LEFT JOIN (SELECT order_id FROM pe2.custom_medicine_mapping WHERE ucode IS NULL AND DATE(created_at)>='2018-06-01' GROUP BY 1) cust ON o.id=cust.order_id
    LEFT JOIN pe2.medicine_notes mn ON o.id=mn.order_id AND DATE(mn.create_time)>='2018-06-01'
    LEFT JOIN pe2.order_digitization_flag odf ON mn.id=odf.medicine_notes_id AND odf.flag_id=34 AND DATE(odf.created_at)>='2018-06-01'
    LEFT JOIN pe2.medicine_digitization_details mdd ON mn.id=mdd.medicine_note_id AND DATE(mdd.created_at)>='2018-06-01'
    LEFT JOIN pe2.rx ON o.customer_id=rx.customer_id AND (rx.doctor_name IS NOT NULL OR rx.hospital_name IS NOT NULL) AND (rx.patient_id IS NOT NULL)
    WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31' AND cust.order_id IS NULL AND odf.medicine_notes_id IS NULL AND mn.ucode IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    ) ;
    
    
    
CREATE TEMPORARY TABLE base_table2 AS (
	SELECT bt.*, MAX(CASE WHEN dmi.ucode IS NOT NULL THEN 1 ELSE 0 END) AS line_item_prefetchable
	FROM base_table bt 
	LEFT JOIN pe2.image i ON rx.id=i.rx_id AND i.create_time<o.time_stamp
    LEFT JOIN pe2.digitization d ON i.id=d.image_id
    LEFT JOIN pe2.digitization_medicine_info dmi ON d.id=dmi.digitization_id AND dmi.ucode=mn.ucode
)





--VCM ANALYSIS

SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, oh1.first_accepted_at,
		oh.in_vcm_at,
		CASE WHEN cmm.ucode IS NOT NULL AND is_printed IS NULL THEN 'HemTeamMapped'
			 WHEN cmm.ucode IS NOT NULL AND is_printed IS NOT NULL THEN 'TheaMapped'
			 WHEN cmm.is_deleted=1 THEN 'Deleted'
		END AS free_flow,
		oh.out_vcm_at, (out_vcm_at-in_vcm_at) AS in_vcm_for,
		--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
		--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
		--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
		--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
		--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
		mndi.delete_reason_text AS mn_delete_reason, mn.deleted_at AS mn_deleted_at
		--#, u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role
		--CASE WHEN POSITION('Send to Thea' IN oh.notes) THEN CAST(('2018-'||SUBSTRING(oh.notes,POSITION('Send to Thea' IN oh.notes)-14,POSITION('Send to Thea' IN oh.notes)-4)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
		--CASE WHEN POSITION('Ucode added' IN oh.notes) THEN CAST(('2018-'||SUBSTRING(oh.notes,POSITION('Ucode added' IN oh.notes)-14,POSITION('Ucode added' IN oh.notes)-4)||':00') AS TIMESTAMP) END AS ucode_added_at
FROM pe2."order" o
INNER JOIN (SELECT id, order_id, order_status, "timestamp" AS in_vcm_at, LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_vcm_at FROM pe2.order_history WHERE "timestamp">'2018-05-31') oh ON o.id=oh.order_id AND oh.order_status=55 
INNER JOIN (SELECT id, order_id, order_status, RANK() OVER (PARTITION BY order_id ORDER BY id) AS ranking FROM pe2.order_history WHERE "timestamp">'2018-05-31' AND order_status=55) oh2 ON oh.id=oh2.id AND ranking=1
LEFT JOIN (SELECT order_id, MIN("timestamp") AS first_accepted_at FROM pe2.order_history WHERE order_status=5 AND "timestamp">'2018-05-31' GROUP BY 1) oh1 ON o.id=oh1.order_id 
LEFT JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id AND (cmm.created_at<oh1.first_accepted_at OR oh1.order_id IS NULL)
LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
--LEFT JOIN pe2."user" u ON mndi.user_id=u.id
--LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31' AND oh.order_id IS NOT NULL AND cmm.order_id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 2,1
;



--VCM mod

SELECT COUNT(id) FROM pe2.custom_medicine_mapping WHERE DATE(created_at) BETWEEN '2018-06-01' AND '2018-08-31';
--Modified VCM Analysis

WITH order_bt AS (
	SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at--, CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking
		FROM pe2.order_history
		WHERE "timestamp">'2018-05-31' --AND order_id IN (4629564,4619523)
		ORDER BY order_id--,id
	)
	WHERE order_status=55 AND order_id IN (4629564,4619523)
	GROUP BY 1
	--ORDER BY order_id,id
)

-- VCM mod Ucode Level
WITH oh_bt AS (
	--SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	--FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking,
				notes
		FROM pe2.order_history
		WHERE DATE("timestamp")>='2018-06-01' --AND order_id IN (4629564,4619523)
		ORDER BY order_id,id
	--)
	--WHERE order_status=55 ANDorder_id IN (4629564,4619523)
	--ORDER BY order_id,id
)
SELECT *
FROM(
	SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, 
			CASE WHEN DATE(cmm.created_at)<=DATE(cp.created_at) THEN 'Newly Mapped' WHEN DATE(cmm.created_at)>DATE(cp.created_at) THEN 'Already Mapped' END AS mapping_legitimacy,
			cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, --oh1.first_accepted_at,
			oh_bt.in_status_at AS in_vcm_at,
			CASE WHEN cmm.ucode IS NOT NULL AND is_printed IS NULL THEN 'HemTeamMapped'
				 WHEN cmm.ucode IS NOT NULL AND is_printed IS NOT NULL THEN 'TheaMapped'
				 WHEN cmm.is_deleted=1 THEN 'Deleted'
			END AS free_flow,
			oh_bt.out_status_at AS out_vcm_at, (out_status_at-in_status_at) AS in_vcm_for,
			--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
			--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
			--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
			CASE WHEN mn.delete_reason=-2 THEN 'Custom Medicine Deleted' ELSE mndi.delete_reason_text END AS mn_delete_reason, mn.deleted_at AS mn_deleted_at, 
			u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role, RANK() OVER (PARTITION BY cmm.id ORDER BY oh_bt.id) AS ranking1,
			CASE WHEN POSITION('Send to Thea' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Send to Thea' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
			CASE WHEN POSITION('Deleted' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Deleted' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS custom_line_item_deleted_at,
			CASE WHEN POSITION('Ucode added' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Ucode added' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS ucode_mapped_at
	FROM pe2."order" o
	INNER JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id
	INNER JOIN oh_bt ON o.id=oh_bt.order_id AND oh_bt.ranking IS NOT NULL AND cmm.created_at<oh_bt.in_status_at 
	LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
	LEFT JOIN pe2.medicine_notes_deletion_info mndi ON mn.id=mndi.medicine_notes_id
	--LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
	LEFT JOIN pe2."user" u ON mndi.user_id=u.id
	LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
	LEFT JOIN inventory.catalog_products cp ON cmm.ucode=cp.ucode
	WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
WHERE ranking1=1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
ORDER BY 2,1
;


SELECT * FROM inventory.catalog_products WHERE created_at>='2018-06-01'


-- VCM mod Order Level

WITH oh_bt AS (
	--SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	--FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking,
				notes
		FROM pe2.order_history
		WHERE DATE("timestamp")>='2018-06-01' --AND order_id IN (4629564,4619523)
		ORDER BY order_id,id
	--)
	--WHERE order_status=55 ANDorder_id IN (4629564,4619523)
	--ORDER BY order_id,id
),
oh_bt_agg AS (
	SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	FROM oh_bt
	WHERE order_status=55 --AND order_id IN (4629564,4619523)
	GROUP BY 1
	--ORDER BY order_id,id
)
SELECT xyz.order_id, order_placed_at, num_of_times_in_vcm, 
		CASE WHEN o.status IN (9,10) THEN 'Fulfilled'
			 WHEN o.status=8 THEN 'Cancelled'
			 WHEN o.status=2 THEN 'Rejected'
			 ELSE 'Under Process'
		END AS order_status,
		cr.name AS canrej_reason,
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/60 AS in_vcm_for_in_minutes, 
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/3600 AS in_vcm_for_in_hours,
		COUNT(xyz.ucode) AS num_of_custom_line_items, 
		MIN(mapping_legitimacy) AS atleast_one_custom_med_mapping_status, 
		MIN(free_flow) AS flow_of_custom_med, MIN(is_deleted) AS custom_retained_flag
		--AVG(CASE WHEN sent_to_thea_at IS NULL THEN (ucode_mapped_at-in_vcm_at) END) AS hemlata_mapping_tat,
FROM(
	SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, 
			CASE WHEN DATE(cmm.created_at)<=DATE(cp.created_at) THEN 'aNewly Mapped' WHEN DATE(cmm.created_at)>DATE(cp.created_at) THEN 'bAlready Mapped' ELSE 'cDeleted' END AS mapping_legitimacy,
			cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, --oh1.first_accepted_at,
			oh_bt.in_status_at AS in_vcm_at,
			CASE WHEN cmm.ucode IS NOT NULL AND is_printed IS NULL THEN 'cHemTeamMapped'
				 WHEN cmm.ucode IS NOT NULL AND is_printed IS NOT NULL THEN 'aTheaMapped'
				 WHEN cmm.is_deleted=1 THEN 'bDeleted'
			END AS free_flow,
			
			oh_bt.out_status_at AS out_vcm_at, (out_status_at-in_status_at) AS in_vcm_for, cmm.is_deleted,
			--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
			--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
			--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
			CASE WHEN mn.delete_reason=-2 THEN 'Custom Medicine Deleted' ELSE mndi.delete_reason_text END AS mn_delete_reason, mn.deleted_at AS mn_deleted_at, 
			u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role, RANK() OVER (PARTITION BY cmm.id ORDER BY oh_bt.id) AS ranking1,
			CASE WHEN POSITION('Send to Thea' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Send to Thea' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
			CASE WHEN POSITION('Deleted' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Deleted' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS custom_line_item_deleted_at,
			CASE WHEN POSITION('Ucode added' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Ucode added' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS ucode_mapped_at
	FROM pe2."order" o
	INNER JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id
	INNER JOIN oh_bt ON o.id=oh_bt.order_id AND oh_bt.ranking IS NOT NULL AND cmm.created_at<oh_bt.in_status_at 
	LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
	LEFT JOIN pe2.medicine_notes_deletion_info mndi ON mn.id=mndi.medicine_notes_id
	--LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
	LEFT JOIN pe2."user" u ON mndi.user_id=u.id
	LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
	LEFT JOIN inventory.catalog_products cp ON cmm.ucode=cp.ucode
	WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) xyz
LEFT JOIN oh_bt_agg ON xyz.order_id=oh_bt_agg.order_id
LEFT JOIN pe2."order" o ON xyz.order_id=o.id
LEFT JOIN (SELECT order_id, MAX(created_at) AS last_status_updated_at FROM pe2.order_history GROUP BY 1) oh1 ON o.id=oh1.order_id
LEFT JOIN pe2.order_cancel_reason ocr ON o.id=ocr.order_id
LEFT JOIN pe2.cancel_reason cr ON ocr.cancel_reason_id=cr.id
WHERE xyz.ranking1=1
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1
;



-- VCM mod Order Level Final

WITH oh_bt AS (
	--SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	--FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking,
				notes
		FROM pe2.order_history
		WHERE DATE("timestamp")>='2018-06-01' --AND order_id IN (4629564,4619523)
		ORDER BY order_id,id
	--)
	--WHERE order_status=55 ANDorder_id IN (4629564,4619523)
	--ORDER BY order_id,id
),
oh_bt_agg AS (
	SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	FROM oh_bt
	WHERE order_status=55 --AND order_id IN (4629564,4619523)
	GROUP BY 1
	--ORDER BY order_id,id
),
canned_in_vcm AS (
	SELECT order_id
	FROM (
			SELECT order_id, order_status, oh.timestamp AS in_status_at, 
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY oh.id) AS next_status, 
					CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY oh.id DESC) END AS ranking,
					notes
			FROM pe2.order_history oh 
			INNER JOIN pe2."order" o ON o.id=oh.order_id AND DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	)
	WHERE order_status=55 AND next_status=8
	GROUP BY 1
	--ORDER BY 2 DESC
)
SELECT xyz.order_id, order_placed_at, num_of_times_in_vcm, 
		CASE WHEN c.supplier_city_id IN (1,2,3,5,8,9,10) THEN 1 ELSE 0 END AS thea_city,
		CASE WHEN o.status IN (9,10) THEN 'Fulfilled'
			 WHEN o.status=8 THEN 'Cancelled'
			 WHEN o.status=2 THEN 'Rejected'
			 ELSE 'Under Process'
		END AS order_status,
		cr.name AS canrej_reason,
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/60 AS in_vcm_for_in_minutes, 
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/3600 AS in_vcm_for_in_hours,
		COUNT(DISTINCT xyz.cmm_id) AS num_of_custom_line_items, 
		COUNT(DISTINCT mn.id) AS total_line_items,
		MIN(mapping_legitimacy) AS atleast_one_custom_med_mapping_status, 
		MIN(flow) AS flow_of_custom_med, 
		MIN("action") AS custom_med_final_action,
		MIN(flow_status) AS flow_status,
		MIN(live_action) AS final_in_vcm_action,
		MAX(custom_retained_flag) AS custom_retained_flag
		--AVG(CASE WHEN sent_to_thea_at IS NULL THEN (ucode_mapped_at-in_vcm_at) END) AS hemlata_mapping_tat,
FROM(
	SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, 
			CASE WHEN DATE(cmm.created_at)<=DATE(cp.created_at) THEN 'aNewly Mapped' WHEN DATE(cmm.created_at)>DATE(cp.created_at) THEN 'bAlready Mapped' ELSE 'cDeleted' END AS mapping_legitimacy,
			cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, --oh1.first_accepted_at,
			oh_bt.in_status_at AS in_vcm_at,
			CASE WHEN is_printed IS NOT NULL OR send_to_thea=1 THEN 'aThea'
				 ELSE 'bHem'
			END AS flow,
			CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1 THEN 'bDeleted'
				 WHEN cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
			END AS "action",
			CASE WHEN o.status=8 
				 THEN CASE WHEN canned_in_vcm.order_id IS NOT NULL
				 		   THEN	CASE WHEN cmm.is_printed IS NOT NULL OR cmm.send_to_thea=1 THEN 'aInThea' ELSE 'bInHemTeam' END	
				 		   ELSE CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1  THEN 'bDeleted'
						   			 WHEN mn.ucode IS NOT NULL OR cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
					  			END
					  END
				 ELSE CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1  THEN 'bDeleted'
						   WHEN mn.ucode IS NOT NULL OR cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
					  END
			END AS live_action,
			CASE WHEN cmm.updated_at>oh1.last_status_updated_at THEN 'aOrderClosed'
				 ELSE 'bLive'
			END AS flow_status,
			oh_bt.out_status_at AS out_vcm_at, (out_status_at-in_status_at) AS in_vcm_for, 
			CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1 THEN 0 ELSE 1 END AS custom_retained_flag,
			--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
			--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
			--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
			CASE WHEN mn.delete_reason=-2 THEN 'Custom Medicine Deleted' ELSE mndi.delete_reason_text END AS mn_delete_reason, mn.deleted_at AS mn_deleted_at, 
			u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role, RANK() OVER (PARTITION BY cmm.id ORDER BY oh_bt.id) AS ranking1,
			CASE WHEN POSITION('Send to Thea' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Send to Thea' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
			CASE WHEN POSITION('Deleted' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Deleted' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS custom_line_item_deleted_at,
			CASE WHEN POSITION('Ucode added' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Ucode added' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS ucode_mapped_at
	FROM pe2."order" o
	INNER JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id
	INNER JOIN oh_bt ON o.id=oh_bt.order_id AND oh_bt.ranking IS NOT NULL AND cmm.created_at<oh_bt.in_status_at 
	LEFT JOIN (SELECT order_id, MAX("timestamp") AS last_status_updated_at FROM pe2.order_history GROUP BY 1) oh1 ON o.id=oh1.order_id
	LEFT JOIN canned_in_vcm ON o.id=canned_in_vcm.order_id
	LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
	LEFT JOIN pe2.medicine_notes_deletion_info mndi ON mn.id=mndi.medicine_notes_id
	--LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
	LEFT JOIN pe2."user" u ON mndi.user_id=u.id
	LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
	LEFT JOIN inventory.catalog_products cp ON cmm.ucode=cp.ucode
	WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) xyz
LEFT JOIN oh_bt_agg ON xyz.order_id=oh_bt_agg.order_id
LEFT JOIN pe2."order" o ON xyz.order_id=o.id
LEFT JOIN pe2.medicine_notes mn ON o.id=mn.order_id
LEFT JOIN pe2.city c ON o.city_id=c.id
LEFT JOIN pe2.order_cancel_reason ocr ON o.id=ocr.order_id
LEFT JOIN pe2.cancel_reason cr ON ocr.cancel_reason_id=cr.id
WHERE xyz.ranking1=1
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1
;

SELECT * FROM pe2.medicine_notes WHERE order_id=3717759;
SELECT * FROM pe2.custom_medicine_mapping WHERE order_id=3717759;


--Cancelled by User in VCM

-SELECT * FROM (
SELECT order_id, --COUNT(order_id) AS times_in_vcm
FROM (
		SELECT order_id, order_status, oh.timestamp AS in_status_at, 
				LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY oh.id) AS next_status, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY oh.id DESC) END AS ranking,
				notes
		FROM pe2.order_history oh 
		INNER JOIN pe2."order" o ON o.id=oh.order_id AND DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
		--WHERE DATE("timestamp")>='2018-06-01'
)
WHERE order_status=55 AND next_status=8
GROUP BY 1
ORDER BY 2 DESC
--HAVING COUNT(order_id)=1
--)
--WHERE aborted>0
--ORDER BY 3 DESC;


--Testing

WITH oh_bt AS (
	--SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	--FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking,
				notes
		FROM pe2.order_history
		WHERE DATE("timestamp")>='2018-06-01' --AND order_id IN (4629564,4619523)
		ORDER BY order_id,id
	--)
	--WHERE order_status=55 ANDorder_id IN (4629564,4619523)
	--ORDER BY order_id,id
),
oh_bt_agg AS (
	SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	FROM oh_bt
	WHERE order_status=55 --AND order_id IN (4629564,4619523)
	GROUP BY 1
	--ORDER BY order_id,id
)
SELECT xyz.order_id, order_placed_at, oh_bt_agg.num_of_times_in_vcm, --xyz.ucode, 
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/60 AS in_vcm_for_in_minutes, 
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/3600 AS in_vcm_for_in_hours,
		COUNT(DISTINCT xyz.ucode) AS num_of_newly_created_custom_line_items, 
		MAX(CASE WHEN xyz.is_deleted=0 THEN 1 ELSE 0 END) AS custom_retained_flag,
		COUNT(DISTINCT mn.order_id) AS times_newly_added_CM_ordered,
		COUNT(DISTINCT CASE WHEN mn.is_deleted=0 THEN mn.order_id END) AS times_newly_added_CM_ordered_and_retained,
		COUNT(CASE WHEN o.status IN (9,10) THEN mn.order_id END) AS fulf_order,
		COUNT(CASE WHEN o.status IN (2,8) THEN mn.order_id END) AS rejcan_order,
		COUNT(CASE WHEN o.status NOT IN (2,8,9,10) THEN mn.order_id END) AS underprocess_order
		--MIN(free_flow) AS flow_of_custom_med, 
		--AVG(CASE WHEN sent_to_thea_at IS NULL THEN (ucode_mapped_at-in_vcm_at) END) AS hemlata_mapping_tat,
FROM(
	SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, 
			CASE WHEN DATE(cmm.created_at)<=DATE(cp.created_at) THEN 'aNewly Mapped' WHEN DATE(cmm.created_at)>DATE(cp.created_at) THEN 'bAlready Mapped' ELSE 'cDeleted' END AS mapping_legitimacy,
			cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, --oh1.first_accepted_at,
			oh_bt.in_status_at AS in_vcm_at,
			CASE WHEN cmm.ucode IS NOT NULL AND is_printed IS NULL THEN 'bHemTeamMapped'
				 WHEN cmm.ucode IS NOT NULL AND is_printed IS NOT NULL THEN 'aTheaMapped'
				 WHEN cmm.is_deleted=1 THEN 'cDeleted'
			END AS free_flow,
			oh_bt.out_status_at AS out_vcm_at, (out_status_at-in_status_at) AS in_vcm_for, cmm.is_deleted,
			--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
			--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
			--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
			CASE WHEN mn.delete_reason=-2 THEN 'Custom Medicine Deleted' ELSE mndi.delete_reason_text END AS mn_delete_reason, mn.deleted_at AS mn_deleted_at, 
			u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role, RANK() OVER (PARTITION BY cmm.id ORDER BY oh_bt.id) AS ranking1,
			CASE WHEN POSITION('Send to Thea' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Send to Thea' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
			CASE WHEN POSITION('Deleted' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Deleted' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS custom_line_item_deleted_at,
			CASE WHEN POSITION('Ucode added' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Ucode added' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS ucode_mapped_at
	FROM pe2."order" o
	INNER JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id
	INNER JOIN oh_bt ON o.id=oh_bt.order_id AND oh_bt.ranking IS NOT NULL AND cmm.created_at<oh_bt.in_status_at 
	LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
	LEFT JOIN pe2.medicine_notes_deletion_info mndi ON mn.id=mndi.medicine_notes_id
	--LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
	LEFT JOIN pe2."user" u ON mndi.user_id=u.id
	LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
	LEFT JOIN inventory.catalog_products cp ON cmm.ucode=cp.ucode
	WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) xyz
LEFT JOIN oh_bt_agg ON xyz.order_id=oh_bt_agg.order_id
LEFT JOIN pe2.medicine_notes mn ON xyz.ucode=mn.ucode AND xyz.mapping_legitimacy='aNewly Mapped' AND mn.order_id!=xyz.order_id
LEFT JOIN pe2."order" o ON mn.order_id=o.id
WHERE xyz.ranking1=1 AND xyz.mapping_legitimacy='aNewly Mapped'
GROUP BY 1,2,3,4,5
ORDER BY 1
;




---Testing Median

WITH oh_bt AS (
	--SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	--FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking,
				notes
		FROM pe2.order_history
		WHERE DATE("timestamp")>='2018-06-01' --AND order_id IN (4629564,4619523)
		ORDER BY order_id,id
	--)
	--WHERE order_status=55 ANDorder_id IN (4629564,4619523)
	--ORDER BY order_id,id
),
oh_bt_agg AS (
	SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	FROM oh_bt
	WHERE order_status=55 --AND order_id IN (4629564,4619523)
	GROUP BY 1
	--ORDER BY order_id,id
),
canned_in_vcm AS (
	SELECT order_id
	FROM (
			SELECT order_id, order_status, oh.timestamp AS in_status_at, 
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY oh.id) AS next_status, 
					CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY oh.id DESC) END AS ranking,
					notes
			FROM pe2.order_history oh 
			INNER JOIN pe2."order" o ON o.id=oh.order_id AND DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	)
	WHERE order_status=55 AND next_status=8
	GROUP BY 1
	--ORDER BY 2 DESC
)
SELECT order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action,
		COUNT(flow_of_custom_med) AS orders,
		AVG(in_vcm_for_in_hours) AS avg_hrs_in_vcm,
		MAX(median) AS median_hrs_in_vcm,
		MAX(p30) AS p30_hrs_in_vcm,
		MAX(p25) AS p25_hrs_in_vcm,
		MAX(p20) AS p20_hrs_in_vcm,
		MAX(p15) AS p15_hrs_in_vcm,
		MAX(p10) AS p10_hrs_in_vcm,
		MAX(p05) AS p05_hrs_in_vcm
FROM (
SELECT order_status, canrej_reason, flow_of_custom_med, final_in_vcm_action, in_vcm_for_in_hours, 
		PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p05,
    	PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p10,
    	PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p15,
    	PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p20,
    	PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p25,
    	PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p30,
    	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS median
FROM (
SELECT xyz.order_id, order_placed_at, num_of_times_in_vcm, 
		CASE WHEN c.supplier_city_id IN (1,2,3,5,8,9,10) THEN 1 ELSE 0 END AS thea_city,
		CASE WHEN o.status IN (9,10) THEN 'Fulfilled'
			 WHEN o.status=8 THEN 'Cancelled'
			 WHEN o.status=2 THEN 'Rejected'
			 ELSE 'Under Process'
		END AS order_status,
		CASE WHEN o.status IN (2,8) THEN cr.name END AS canrej_reason,
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/60 AS in_vcm_for_in_minutes, 
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/3600 AS in_vcm_for_in_hours,
		COUNT(xyz.cmm_id) AS num_of_custom_line_items, 
		MIN(mapping_legitimacy) AS atleast_one_custom_med_mapping_status, 
		MIN(flow) AS flow_of_custom_med, 
		MIN("action") AS custom_med_final_action,
		MIN(flow_status) AS flow_status,
		MIN(live_action) AS final_in_vcm_action,
		MAX(custom_retained_flag) AS custom_retained_flag
		--AVG(CASE WHEN sent_to_thea_at IS NULL THEN (ucode_mapped_at-in_vcm_at) END) AS hemlata_mapping_tat,
FROM(
	SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, 
			CASE WHEN DATE(cmm.created_at)<=DATE(cp.created_at) THEN 'aNewly Mapped' WHEN DATE(cmm.created_at)>DATE(cp.created_at) THEN 'bAlready Mapped' ELSE 'cDeleted' END AS mapping_legitimacy,
			cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, --oh1.first_accepted_at,
			oh_bt.in_status_at AS in_vcm_at,
			CASE WHEN is_printed IS NOT NULL OR send_to_thea=1 THEN 'aThea'
				 ELSE 'bHem'
			END AS flow,
			CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1 THEN 'bDeleted'
				 WHEN cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
			END AS "action",
			CASE WHEN o.status=8 
				 THEN CASE WHEN canned_in_vcm.order_id IS NOT NULL
				 		   THEN	CASE WHEN cmm.is_printed IS NOT NULL OR cmm.send_to_thea=1 THEN 'aInThea' ELSE 'bInHemTeam' END	
				 		   ELSE CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1  THEN 'bDeleted'
						   			 WHEN mn.ucode IS NOT NULL OR cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
					  			END
					  END
				 ELSE CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1  THEN 'bDeleted'
						   WHEN mn.ucode IS NOT NULL OR cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
					  END
			END AS live_action,
			CASE WHEN cmm.updated_at>oh1.last_status_updated_at THEN 'aOrderClosed'
				 ELSE 'bLive'
			END AS flow_status,
			oh_bt.out_status_at AS out_vcm_at, (out_status_at-in_status_at) AS in_vcm_for, 
			CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1 THEN 0 ELSE 1 END AS custom_retained_flag,
			--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
			--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
			--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
			CASE WHEN mn.delete_reason=-2 THEN 'Custom Medicine Deleted' ELSE mndi.delete_reason_text END AS mn_delete_reason, mn.deleted_at AS mn_deleted_at, 
			u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role, RANK() OVER (PARTITION BY cmm.id ORDER BY oh_bt.id) AS ranking1,
			CASE WHEN POSITION('Send to Thea' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Send to Thea' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
			CASE WHEN POSITION('Deleted' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Deleted' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS custom_line_item_deleted_at,
			CASE WHEN POSITION('Ucode added' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Ucode added' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS ucode_mapped_at
	FROM pe2."order" o
	INNER JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id
	INNER JOIN oh_bt ON o.id=oh_bt.order_id AND oh_bt.ranking IS NOT NULL AND cmm.created_at<oh_bt.in_status_at 
	LEFT JOIN (SELECT order_id, MAX("timestamp") AS last_status_updated_at FROM pe2.order_history GROUP BY 1) oh1 ON o.id=oh1.order_id
	LEFT JOIN canned_in_vcm ON o.id=canned_in_vcm.order_id
	LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
	LEFT JOIN pe2.medicine_notes_deletion_info mndi ON mn.id=mndi.medicine_notes_id
	--LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
	LEFT JOIN pe2."user" u ON mndi.user_id=u.id
	LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
	LEFT JOIN inventory.catalog_products cp ON cmm.ucode=cp.ucode
	WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) xyz
LEFT JOIN oh_bt_agg ON xyz.order_id=oh_bt_agg.order_id
LEFT JOIN pe2."order" o ON xyz.order_id=o.id
LEFT JOIN pe2.city c ON o.city_id=c.id
LEFT JOIN pe2.order_cancel_reason ocr ON o.id=ocr.order_id
LEFT JOIN pe2.cancel_reason cr ON ocr.cancel_reason_id=cr.id
WHERE xyz.ranking1=1
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1
)
--WHERE order_status IN ('Cancelled','Rejected')
--GROUP BY 1,2,3
)
GROUP BY 1,2,3,4
ORDER BY 5 DESC
; -- Just remove WHERE order_status='Cancelled' and add order_status in the final grouping




--- Final Combined

WITH oh_bt AS (
	--SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	--FROM (
		SELECT id, order_id, order_status, timestamp AS in_status_at, 
				LEAD("timestamp",1) OVER (PARTITION BY order_id ORDER BY id) AS out_status_at, 
				CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY id DESC) END AS ranking,
				notes
		FROM pe2.order_history
		WHERE DATE("timestamp")>='2018-06-01' --AND order_id IN (4629564,4619523)
		ORDER BY order_id,id
	--)
	--WHERE order_status=55 ANDorder_id IN (4629564,4619523)
	--ORDER BY order_id,id
),
oh_bt_agg AS (
	SELECT order_id, SUM(out_status_at-in_status_at) AS in_vcm_for, COUNT(id) AS num_of_times_in_vcm
	FROM oh_bt
	WHERE order_status=55 --AND order_id IN (4629564,4619523)
	GROUP BY 1
	--ORDER BY order_id,id
),
canned_in_vcm AS (
	SELECT order_id
	FROM (
			SELECT order_id, order_status, oh.timestamp AS in_status_at, 
					LEAD(order_status,1) OVER (PARTITION BY order_id ORDER BY oh.id) AS next_status, 
					CASE WHEN order_status=55 THEN RANK() OVER (PARTITION BY order_id,order_status=55 ORDER BY oh.id DESC) END AS ranking,
					notes
			FROM pe2.order_history oh 
			INNER JOIN pe2."order" o ON o.id=oh.order_id AND DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	)
	WHERE order_status=55 AND next_status=8
	GROUP BY 1
	--ORDER BY 2 DESC
)
SELECT order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action,
		COUNT(flow_of_custom_med) AS orders,
		AVG(in_vcm_for_in_hours) AS avg_hrs_in_vcm,
		MAX(median) AS median_hrs_in_vcm,
		MAX(p30) AS p30_hrs_in_vcm,
		MAX(p25) AS p25_hrs_in_vcm,
		MAX(p20) AS p20_hrs_in_vcm,
		MAX(p15) AS p15_hrs_in_vcm,
		MAX(p10) AS p10_hrs_in_vcm,
		MAX(p05) AS p05_hrs_in_vcm
FROM (
SELECT order_status, canrej_reason, flow_of_custom_med, final_in_vcm_action, in_vcm_for_in_hours, 
		PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p05,
    	PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p10,
    	PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p15,
    	PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p20,
    	PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p25,
    	PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS p30,
    	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY in_vcm_for_in_hours) OVER (PARTITION BY order_status,canrej_reason, flow_of_custom_med, final_in_vcm_action) AS median
FROM (
SELECT xyz.order_id, order_placed_at, num_of_times_in_vcm, 
		CASE WHEN c.supplier_city_id IN (1,2,3,5,8,9,10) THEN 1 ELSE 0 END AS thea_city,
		CASE WHEN o.status IN (9,10) THEN 'Fulfilled'
			 WHEN o.status=8 THEN 'Cancelled'
			 WHEN o.status=2 THEN 'Rejected'
			 ELSE 'Under Process'
		END AS order_status,
		CASE WHEN o.status IN (2,8) THEN cr.name END AS canrej_reason,
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/60 AS in_vcm_for_in_minutes, 
		EXTRACT(epoch FROM oh_bt_agg.in_vcm_for)/3600 AS in_vcm_for_in_hours,
		COUNT(xyz.cmm_id) AS num_of_custom_line_items, 
		COUNT(mn.id) AS non_deleted_non_custom_line_items,
		MIN(mapping_legitimacy) AS atleast_one_custom_med_mapping_status, 
		MIN(flow) AS flow_of_custom_med, 
		MIN("action") AS custom_med_final_action,
		MIN(flow_status) AS flow_status,
		MIN(live_action) AS final_in_vcm_action,
		MAX(custom_retained_flag) AS custom_retained_flag
		--AVG(CASE WHEN sent_to_thea_at IS NULL THEN (ucode_mapped_at-in_vcm_at) END) AS hemlata_mapping_tat,
FROM(
	SELECT cmm.id AS cmm_id, cmm.order_id, o.time_stamp AS order_placed_at, cmm.ucode, 
			CASE WHEN DATE(cmm.created_at)<=DATE(cp.created_at) THEN 'aNewly Mapped' WHEN DATE(cmm.created_at)>DATE(cp.created_at) THEN 'bAlready Mapped' ELSE 'cDeleted' END AS mapping_legitimacy,
			cmm.medicine_name, cmm.created_at AS custom_digitization_created_at, --oh1.first_accepted_at,
			oh_bt.in_status_at AS in_vcm_at,
			CASE WHEN is_printed IS NOT NULL OR send_to_thea=1 THEN 'aThea'
				 ELSE 'bHem'
			END AS flow,
			CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1 THEN 'bDeleted'
				 WHEN cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
			END AS "action",
			CASE WHEN o.status=8 
				 THEN CASE WHEN canned_in_vcm.order_id IS NOT NULL
				 		   THEN	CASE WHEN cmm.is_printed IS NOT NULL OR cmm.send_to_thea=1 THEN 'aInThea' ELSE 'bInHemTeam' END	
				 		   ELSE CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1  THEN 'bDeleted'
						   			 WHEN mn.ucode IS NOT NULL OR cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
					  			END
					  END
				 ELSE CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1  THEN 'bDeleted'
						   WHEN mn.ucode IS NOT NULL OR cmm.ucode IS NOT NULL THEN 'aMapped&Retained'
					  END
			END AS live_action,
			CASE WHEN cmm.updated_at>oh1.last_status_updated_at THEN 'aOrderClosed'
				 ELSE 'bLive'
			END AS flow_status,
			oh_bt.out_status_at AS out_vcm_at, (out_status_at-in_status_at) AS in_vcm_for, 
			CASE WHEN mn.is_deleted=1 OR cmm.is_deleted=1 THEN 0 ELSE 1 END AS custom_retained_flag,
			--CASE WHEN is_printed IS NULL THEN 'HemTeam' ELSE 'SendToThea' END AS custom_med_flow, 
			--CASE WHEN cmm.ucode IS NOT NULL THEN 'ucode mapped' WHEN cmm.is_deleted=1 THEN 'deleted' END AS custom_med_status, 
			--CASE WHEN is_printed IS NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS mapped_by_hem_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.is_deleted=1 THEN cmm.updated_at END AS thea_deleted_at,
			--CASE WHEN is_printed IS NOT NULL AND cmm.ucode IS NOT NULL THEN cmm.updated_at END AS thea_hem_mapped_at,
			CASE WHEN mn.delete_reason=-2 THEN 'Custom Medicine Deleted' ELSE mndi.delete_reason_text END AS mn_delete_reason, mn.deleted_at AS mn_deleted_at, 
			u.username AS mn_deleted_by, ur.name AS mn_deleted_by_role, RANK() OVER (PARTITION BY cmm.id ORDER BY oh_bt.id) AS ranking1,
			CASE WHEN POSITION('Send to Thea' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Send to Thea' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS sent_to_thea_at,
			CASE WHEN POSITION('Deleted' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Deleted' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS custom_line_item_deleted_at,
			CASE WHEN POSITION('Ucode added' IN oh_bt.notes) THEN CAST(('2018-'||SUBSTRING(oh_bt.notes,POSITION('Ucode added' IN oh_bt.notes)-14,11)||':00') AS TIMESTAMP) END AS ucode_mapped_at
	FROM pe2."order" o
	INNER JOIN pe2.custom_medicine_mapping cmm ON cmm.order_id=o.id
	INNER JOIN oh_bt ON o.id=oh_bt.order_id AND oh_bt.ranking IS NOT NULL AND cmm.created_at<oh_bt.in_status_at 
	LEFT JOIN (SELECT order_id, MAX("timestamp") AS last_status_updated_at FROM pe2.order_history GROUP BY 1) oh1 ON o.id=oh1.order_id
	LEFT JOIN canned_in_vcm ON o.id=canned_in_vcm.order_id
	LEFT JOIN pe2.medicine_notes mn ON cmm.digitization_id=mn.id
	LEFT JOIN pe2.medicine_notes_deletion_info mndi ON mn.id=mndi.medicine_notes_id
	--LEFT JOIN (SELECT delete_reason,delete_reason_text FROM pe2.medicine_notes_deletion_info GROUP BY 1,2) mndi ON mn.delete_reason=mndi.delete_reason
	LEFT JOIN pe2."user" u ON mndi.user_id=u.id
	LEFT JOIN pe2.user_roles ur ON u.role_id=ur.id
	LEFT JOIN inventory.catalog_products cp ON cmm.ucode=cp.ucode
	WHERE DATE(o.time_stamp) BETWEEN '2018-06-01' AND '2018-08-31'
	--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) xyz
LEFT JOIN oh_bt_agg ON xyz.order_id=oh_bt_agg.order_id
LEFT JOIN pe2."order" o ON xyz.order_id=o.id
LEFT JOIN pe2.medicine_notes mn ON o.id=mn.order_id AND (mn.is_deleted=0 OR (mn.is_deleted=1 AND mn.delete_reason IN (-1,0,1,8,10,11,13,14,15)))
LEFT JOIN pe2.custom_medicine_mapping cmm ON o.id=cmm.order_id AND mn.id=cmm.digitization_id
LEFT JOIN pe2.city c ON o.city_id=c.id
LEFT JOIN pe2.order_cancel_reason ocr ON o.id=ocr.order_id
LEFT JOIN pe2.cancel_reason cr ON ocr.cancel_reason_id=cr.id
WHERE xyz.ranking1=1 AND cmm.id IS NULL AND mn.id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1
)
--WHERE order_status IN ('Cancelled','Rejected')
--GROUP BY 1,2,3
)
GROUP BY 1,2,3,4
ORDER BY 5 DESC
