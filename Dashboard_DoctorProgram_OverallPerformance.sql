
---- Query powered mainly on the data model that is a flat table treasure of all possible metrics

SELECT  DATE(fdpo.order_placed_at) AS order_placed_date, 
  CASE  WHEN fdpo.successful_consultation_type='Repeat' THEN 'Repeat' ELSE 'First Time' END AS successful_consultation_type,
  CASE  WHEN fdpo.latest_case_status=4 AND fo.order_status_id IN (9,10) THEN '1_Successful Consultations Fulfilled' 
           WHEN fdpo.latest_case_status=4 THEN '2_Successful Consultations' 
           WHEN fdpo.first_response_at IS NOT NULL THEN '3_Reached Out' 
           WHEN fdpo.first_assigned_to_doc_at IS NOT NULL THEN '4_Assigned' 
           WHEN fdpo.latest_case_status IS NOT NULL THEN '5_Moved To DP'
  END AS funnel_drop,
  foc.order_source, fo.supplier_city_name AS supplier_city, 
  CASE WHEN foc.third_party_name='MEDIASSIST' THEN 1 ELSE 0 END AS is_mediassist_order,
  CASE WHEN foc.user_type_monthly='Old User' THEN 0 ELSE 1 END AS new_customer_flag, 
  fo.is_add_to_cart::INT AS atc_flag, foc.chronic_flag_old AS chronic_flag, fo.is_courier::INT AS courier_flag, 
  CASE WHEN foc.cancellation_rejection_bucket='Cancelled' THEN 'Cancellation' 
    WHEN foc.cancellation_rejection_bucket='Rejected' THEN 'Rejection'
  END AS cnr_bucket, 
  foc.cnr_reason AS can_rej_reason, 
  CASE WHEN dpo.doctor_program_id IN (2,7,9,25,47,62,66,67,68,8,11,43,49,55,70,172,1) THEN 'Other_Third_Party'
    --WHEN dpo.doctor_program_id=2 THEN 'DocsApp'
    WHEN dpo.doctor_program_id IN (75,158,159) THEN 'LetsDoc'
    WHEN dpo.doctor_program_id IS NULL THEN 'Not Applicable'
    ELSE 'Docstat'
  END AS doc_category, 
  fdpo.order_source AS dp_order_source,
  fdpo.dp_rejection_reason AS dp_rej_reason,
  CASE WHEN fdpo.dp_rejection_reason IS NOT NULL 
    THEN CASE WHEN fdpo.dp_rejection_reason IN ('Order Cancelled By Moderator', 'Admin removed order from Doctor Program', 'Order Cancelled by user') 
         THEN 'Admin/User'
                   ELSE 'Doctor'
               END
  END AS dp_rejection_bucket,
  CASE WHEN dp_frt_tat_office_hours<=30 THEN '1) 0 to 30 mins'
    WHEN dp_frt_tat_office_hours<=60 THEN '2) 30 to 60 mins'
    WHEN dp_frt_tat_office_hours<=90 THEN '3) 60 to 90 mins'
    WHEN dp_frt_tat_office_hours<=120 THEN '4) 90 to 120 mins'
    ELSE '5) > 120 mins'
  END AS frt_sla_buckets,
  COUNT(fdpo.order_id) AS total_orders, 
  COUNT(fdpo.customer_id) AS total_customers, 
  COUNT(CASE WHEN fo.order_status_id IN (9,10) THEN fo.order_id END) AS total_fulfilled_orders,
  COUNT(CASE WHEN fo.order_status_id=2 THEN fo.order_id END) AS total_rejected_orders,
  COUNT(CASE WHEN fo.order_status_id=8 THEN fo.order_id END) AS total_cancelled_orders,
  COUNT(CASE WHEN fo.order_status_id NOT IN (2,8,9,10) THEN fo.order_id END) AS total_under_process_orders,
  COUNT(CASE WHEN fdpo.issue_flag=1 THEN fdpo.order_id END) AS total_issue_orders,
  COUNT(CASE WHEN fdpo.first_response_at IS NOT NULL THEN fdpo.order_id END) AS total_frt_orders,
  SUM(CASE WHEN fdpo.first_response_at IS NOT NULL THEN fdpo.num_of_doc_attempts END) AS total_doc_attempts,
  COUNT(CASE WHEN fdpo.num_of_times_on_hold>0 THEN fdpo.order_id END) AS total_hold_orders,
  SUM(fdpo.num_of_times_on_hold) AS total_hold_attempts,
  SUM(foc.mrp) AS total_gmv,
  SUM(rx_info.rx_prescribed_count) AS total_rx_prescribed,
  SUM(DATEDIFF(min,fdpo.first_moved_to_dp_at,foc.cnr_timestamp)) AS total_cnr_from_m2dp,
  COUNT(CASE WHEN fdpo.rx_first_prescribed_at IS NOT NULL THEN fdpo.order_id END) AS total_rx_prescribed_orders,
  SUM(fdpo.frt_sla_breached_internal) AS total_breaching_frt_internal_sla,
  SUM(fdpo.frt_sla_breached_committed) AS total_breaching_frt_committed_sla,
  SUM(fdpo.overall_sla_breached_internal) AS total_breaching_overall_internal_sla,
  SUM(fdpo.last_prescribed_tat_office_hours) AS total_last_prescribed_tat_office_hours, 
  SUM(fdpo.first_prescribed_tat_office_hours) AS total_first_prescribed_tat_office_hours,
  SUM(fdpo.dp_frt_tat_office_hours) AS total_dp_frt_tat_office_hours
FROM data_model.f_doctor_program_order fdpo
INNER JOIN data_model.f_order_consumer foc ON fdpo.order_id=foc.order_id
INNER JOIN data_model.f_order fo ON fdpo.order_id=fo.order_id
LEFT JOIN pe_pe2_pe2.doctor_program_order dpo ON fdpo.order_id=dpo.order_id
LEFT JOIN pe_pe2_pe2.doctor_program dp ON dpo.doctor_program_id=dp.doctor_program_id
LEFT JOIN (
   SELECT o.order_id, COUNT(DISTINCT rx.id) AS rx_prescribed_count
   FROM data_model.f_doctor_program_order o 
   LEFT JOIN pe_pe2_pe2.order_image oi ON o.order_id=oi.order_id AND oi.is_duplicate=0 AND oi.is_valid=1
   LEFT JOIN pe_pe2_pe2.image i ON oi.image_id = i.id
   LEFT JOIN pe_pe2_pe2.rx ON i.rx_id=rx.id
   WHERE o.latest_case_status=4 AND DATEADD(MIN,330,i.create_time)>o.first_moved_to_dp_at 
     AND (LEFT(LOWER(i.image_name),3) IN ('dp_','med'))
   GROUP BY 1
) rx_info ON fdpo.order_id=rx_info.order_id
WHERE DATE(fdpo.order_placed_at) BETWEEN '2019-01-01' AND (CURRENT_DATE-1)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
ORDER BY 1 DESC;
