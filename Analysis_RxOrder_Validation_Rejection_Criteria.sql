SELECT 
foc.order_id,
fo.order_placed_at,
fo.delivery_city_name AS demand_city,
fo.supplier_city_name AS supplier_city,
--i_d_info.rx_id,
fo.is_doctor_program::INT AS doctor_program_flag,
MAX(CASE WHEN i_d_info.ucode IS NULL THEN 1 ELSE 0 END) AS dont_have_rx_for_atleast_onelineitem_flag,
MIN(i_d_info.prescribed_at) AS rx_prescribed_at,
MAX(CASE WHEN i_d_info.hospital_name IS NOT NULL THEN 1 ELSE 0 END) AS hospital_name_flag,
MAX(CASE WHEN i_d_info.doctor_name IS NOT NULL THEN 1 ELSE 0 END) AS doctor_name_flag,
MAX(CASE WHEN i_d_info.doctor_degree IS NOT NULL THEN 1 ELSE 0 END) AS doctor_degree_flag,
MAX(CASE WHEN i_d_info.doctor_reg_no IS NOT NULL THEN 1 ELSE 0 END) AS doctor_reg_no_flag,
MAX(CASE WHEN i_d_info.is_doctor_signature_present IS NOT NULL THEN 1 ELSE 0 END) AS doctor_signature_flag,
MAX(CASE WHEN i_d_info.display_name IS NOT NULL THEN 1 ELSE 0 END) AS patient_name_flag,
MIN(case when i_d_info.duration_unit =  'day' then i_d_info.duration_value*1
		when i_d_info.duration_unit =  'month' then i_d_info.duration_value*30
		when i_d_info.duration_unit =  'year' then i_d_info.duration_value*365
		when i_d_info.duration_unit =  'week' then i_d_info.duration_value*7
		when i_d_info.duration_unit =  'lifetime' or i_d_info.duration_unit = 'not-mentioned' then case when dcp.is_chronic = 1 then 365*3 else 30*6 end--considering lifetime to be two years
	end) as duration_days,
MIN(i_d_info.morning+i_d_info.afternoon+i_d_info.evening) dosage_per_day,
MIN(case when i_d_info.frequency = 0 then 1 --Daily
		when i_d_info.frequency = 1 then 0.143 ----Once a week 1/7
		when i_d_info.frequency = 2 then 0.286 ----Twice a week 2/7
		when i_d_info.frequency = 3 then 0.067 ----Once in 15 days 1/15
		when i_d_info.frequency = 4 then 0.033 ----Once a month 1/30
	end) as frequency
from data_model.f_order_consumer foc 
inner join data_model.f_order fo ON foc.order_id=fo.order_id
left join pe_pe2_pe2.medicine_notes mn ON foc.order_id=mn.order_id
left join data_model.d_catalog_product dcp on mn.ucode = dcp.ucode
left join ( SELECT oi.order_id, r.id as rx_id, r.prescribed_at, r.hospital_name, r.doctor_name, r.doctor_degree, r.doctor_reg_no,
					r.is_doctor_signature_present, p.display_name, d.image_id, d.id AS digitization_id, dmi.ucode, 
					ddi.duration_unit, ddi.duration_value, dp.frequency, dp.morning, dp.afternoon, dp.evening
			from pe_pe2_pe2.order_image oi 
			left join pe_pe2_pe2.image i on oi.image_id = i.id --image attached to the Rx
			left join pe_pe2_pe2.rx r on i.rx_id=r.id
			left join pe_pe2_pe2.patient p ON r.patient_id=p.id
			left join pe_pe2_pe2.Digitization d on i.id=d.image_id
			left join pe_pe2_pe2.digitization_medicine_info dmi on d.id=dmi.digitization_id 
			left join pe_pe2_pe2.digitization_dosage_info ddi on d.id=ddi.digitization_id -- dosage for the medicine
			left join pe_pe2_pe2.dosage_pattern dp on dp.id = ddi.dosage_pattern_id
			WHERE (LOWER(i.image_name) NOT LIKE ('dp_%')) and r.id IS NOT NULL  ---oi.is_valid=1 AND 
			) i_d_info ON foc.order_id=i_d_info.order_id AND mn.ucode=i_d_info.ucode --digitization data w.r.t image_id
where DATE(foc.order_placed_at) BETWEEN '2019-07-01' AND '2019-07-31' -- and '2018-10-31' --Rx created in October
and fo.cc_confirmation_instances>0 --orders which were confirmed
and fo.is_rx_required=1 
and dcp.is_rx_required = 1
--and i_d_info.frequency not in (5,6)
--and ddi.duration_unit != 'not-mentioned'
--and dmi.ucode is not null 
group by 1,2,3,4,5
order by 6 desc
;
