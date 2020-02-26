

SELECT EXTRACT(MONTH FROM first_interaction_month) AS month_of_interaction,
		COUNT(DISTINCT customer_id) AS month0,
		COUNT(DISTINCT CASE WHEN following_month1_order_placed_flag=1 THEN customer_id END) AS month1,
		COUNT(DISTINCT CASE WHEN following_month2_order_placed_flag=1 THEN customer_id END) AS month2,
		COUNT(DISTINCT CASE WHEN following_month3_order_placed_flag=1 THEN customer_id END) AS month3
FROM (
	SELECT foc1.customer_id, foc1.order_id, foc1.order_placed_at, foc1.call_date AS first_interaction_month, 
			MAX(CASE WHEN EXTRACT(MONTH FROM foc2.order_placed_at)=(EXTRACT(MONTH FROM foc1.order_placed_at)+1) THEN 1 ELSE 0 END) AS following_month1_order_placed_flag,
			MAX(CASE WHEN EXTRACT(MONTH FROM foc2.order_placed_at)=(EXTRACT(MONTH FROM foc1.order_placed_at)+2) THEN 1 ELSE 0 END) AS following_month2_order_placed_flag,
			MAX(CASE WHEN EXTRACT(MONTH FROM foc2.order_placed_at)=(EXTRACT(MONTH FROM foc1.order_placed_at)+3) THEN 1 ELSE 0 END) AS following_month3_order_placed_flag
	FROM (
		SELECT call_id,call_type, location, caller_no, call_date,Start_Time, disposition,comments, ocd.order_id, foc1.customer_id, foc1.order_placed_at,
				ROW_NUMBER() OVER (PARTITION BY foc1.customer_id ORDER BY Start_Time) AS ranking
		FROM
		(
			SELECT call_id,call_type, location, caller_no, call_date,Start_Time, disposition,comments, 
					CASE WHEN order_chk is TRUE and lower(comments) like 'm%' and lower(comments) not like '%# %' THEN SUBSTRING(trim(replace(comments, ' ','')),2,7)
						WHEN order_chk is TRUE and lower(comments) like 'm %' and lower(comments) not like '%#%' THEN SUBSTRING(trim(replace(comments, ' ','')),3,7)
						WHEN order_chk is TRUE and lower(comments) like 'o%' THEN SUBSTRING(trim(replace(comments, ' ','')),position('#' in comments),7)
						WHEN order_chk is TRUE and lower(comments) like 'm%'and lower(comments) like '%#%' THEN SUBSTRING(trim(replace(comments, ' ','')),position('#' in comments)+1,7)
						when order_chk is FALSE then comments
					END as order_id
			FROM
			(SELECT call_id,call_type, location, caller_no, start_time,Call_Date,
			        disposition, status, comments, REGEXP_INSTR(comments, '\\D') > 0 AS order_chk ----REGEXP_INSTR count of integer in string
			        from ozonetel.ozonetel_calls_dump ocd
					        WHERE call_date  between '2018-07-01' AND '2018-11-30'
						    AND status = 'Answered' 
						    AND call_type = 'Inbound'
						    and lower(Skill) like '%voice_support%'
						    and comments is not null
			    	ORDER BY caller_no, call_date
			)
		) ocd
		LEFT JOIN data_model.f_order_consumer foc1 ON ocd.order_id=foc1.order_id AND foc1.order_id>3000000
	) foc1
	INNER JOIN data_model.f_order_consumer foc2 ON foc1.customer_id=foc2.customer_id AND foc1.ranking=1
	GROUP BY 1,2,3,4
)
GROUP BY 1;



---- Final Query

SELECT EXTRACT(MONTH FROM first_interaction_month) AS month_of_interaction,
		COUNT(DISTINCT customer_id) AS month0,
		COUNT(DISTINCT CASE WHEN following_month1_order_placed_flag=1 THEN customer_id END) AS month1,
		COUNT(DISTINCT CASE WHEN following_month2_order_placed_flag=1 THEN customer_id END) AS month2,
		COUNT(DISTINCT CASE WHEN following_month3_order_placed_flag=1 THEN customer_id END) AS month3
FROM (
	SELECT foc1.customer_id, foc1.order_id, foc1.order_placed_at, foc1.call_date AS first_interaction_month, 
			MAX(CASE WHEN DATE(foc2.order_placed_at) BETWEEN DATE(DATEADD(MONTH,1,DATE_TRUNC('month', foc1.call_date))) AND LAST_DAY(DATEADD(MONTH,1,DATE_TRUNC('month', foc1.call_date))) THEN 1 ELSE 0 END) AS following_month1_order_placed_flag,
			MAX(CASE WHEN DATE(foc2.order_placed_at) BETWEEN DATE(DATEADD(MONTH,2,DATE_TRUNC('month', foc1.call_date))) AND LAST_DAY(DATEADD(MONTH,2,DATE_TRUNC('month', foc1.call_date))) THEN 1 ELSE 0 END) AS following_month2_order_placed_flag,
			MAX(CASE WHEN DATE(foc2.order_placed_at) BETWEEN DATE(DATEADD(MONTH,3,DATE_TRUNC('month', foc1.call_date))) AND LAST_DAY(DATEADD(MONTH,3,DATE_TRUNC('month', foc1.call_date))) THEN 1 ELSE 0 END) AS following_month3_order_placed_flag
	FROM (
		SELECT call_id,call_type, location, caller_no, call_date,Start_Time, disposition,comments, ocd.order_id, foc1.customer_id, foc1.order_placed_at,
				ROW_NUMBER() OVER (PARTITION BY foc1.customer_id ORDER BY Start_Time) AS ranking
		FROM
		(
			SELECT call_id,call_type, location, caller_no, call_date,Start_Time, disposition,comments, 
					CASE WHEN order_chk is TRUE and lower(comments) like 'm%' and lower(comments) not like '%# %' THEN SUBSTRING(trim(replace(comments, ' ','')),2,7)
						WHEN order_chk is TRUE and lower(comments) like 'm %' and lower(comments) not like '%#%' THEN SUBSTRING(trim(replace(comments, ' ','')),3,7)
						WHEN order_chk is TRUE and lower(comments) like 'o%' THEN SUBSTRING(trim(replace(comments, ' ','')),position('#' in comments),7)
						WHEN order_chk is TRUE and lower(comments) like 'm%'and lower(comments) like '%#%' THEN SUBSTRING(trim(replace(comments, ' ','')),position('#' in comments)+1,7)
						when order_chk is FALSE then comments
					END as order_id
			FROM
			(SELECT call_id,call_type, location, caller_no, start_time,Call_Date,
			        disposition, status, comments, REGEXP_INSTR(comments, '\\D') > 0 AS order_chk ----REGEXP_INSTR count of integer in string
			        from ozonetel.ozonetel_calls_dump ocd
					        WHERE call_date  between '2018-07-01' AND '2018-11-30'
						    AND status = 'Answered' 
						    AND call_type = 'Inbound'
						    and lower(Skill) like '%voice_support%'
						    and comments is not null
			    	ORDER BY caller_no, call_date
			)
		) ocd
		LEFT JOIN data_model.f_order_consumer foc1 ON ocd.order_id=foc1.order_id AND foc1.order_id>3000000
	) foc1
	INNER JOIN data_model.f_order_consumer foc2 ON foc1.customer_id=foc2.customer_id AND foc1.ranking=1
	--WHERE foc1.customer_id=26610
	GROUP BY 1,2,3,4
	ORDER BY call_date
)
GROUP BY 1
ORDER BY 1;
