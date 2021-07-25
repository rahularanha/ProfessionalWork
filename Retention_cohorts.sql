-- Retention Cohorts

select o2.acquisition_month, 
            count(distinct o1.user_id) as customers_acquired,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+1) then o1.user_id end) as m_1,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+2) then o1.user_id end) as m_2,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+3) then o1.user_id end) as m_3,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+4) then o1.user_id end) as m_4,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+5) then o1.user_id end) as m_5,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+6) then o1.user_id end) as m_6,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+7) then o1.user_id end) as m_7,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+8) then o1.user_id end) as m_8,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+9) then o1.user_id end) as m_9,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+10) then o1.user_id end) as m_10,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+11) then o1.user_id end) as m_11,
            count(distinct case when extract(month from o1.date)=(o2.acquisition_month+12) then o1.user_id end) as m_12
from order o1
left join (
    select userid, min(extract(month from date)) as acquisition_month
    from order
    group by 1
) o2 on o1.user_id=o2.user_id
group by 1
