create temporary table t1 as
select month_date as flag,
        (case when datediff(month, Acquisition_month, to_date(month_date, 'YYYY-MM-DD')) between 1 and 3 then 'M'|| cast(datediff(month, Acquisition_month, to_date(month_date, 'YYYY-MM-DD'))as char)
              when datediff(month, Acquisition_month, to_date(month_date, 'YYYY-MM-DD')) between 4 and 6 then 'M'|| cast(datediff(month, Acquisition_month, to_date(month_date, 'YYYY-MM-DD'))as char)
              else 'M6+'
        end)as Acquisition_bucket,
        (case when datediff(month, Latest_Order_Month, to_date(month_date, 'YYYY-MM-DD')) between 1 and 3 then 'M'|| cast(datediff(month, Latest_Order_Month, to_date(month_date, 'YYYY-MM-DD'))as char)
              when datediff(month, Latest_Order_Month, to_date(month_date, 'YYYY-MM-DD')) between 4 and 6 then 'M'|| cast(datediff(month, Latest_Order_Month, to_date(month_date, 'YYYY-MM-DD'))as char)
              else 'M6+'
        end)as Latest_order_bucket, 
        chronic_flag_old, base_discount_order, supplier_city_name, is_courier, install_source_attribution, sum(count)as count 
from (
      select DATEADD(day, -DATEPART(day, DATE(first_delivered_order_time))+1, DATE(first_delivered_order_time))as Acquisition_month,
            DATEADD(day, -DATEPART(day, DATE(order_placed_date))+1, DATE(order_placed_date))as Latest_Order_Month,
            chronic_flag_old, base_discount_order :: integer, supplier_city_name, is_courier :: integer,
            (case when csrd.install_source_attribution in ('Organic', 'Affiliates', 'Google', 'Facebook') then csrd.install_source_attribution else 'Others' end)as install_source_attribution,
            count(*)
      from (
            select *, row_number() over(partition by customer_id order by order_id desc)as rownum 
            from (
                  select foc.*,(case when supplier_city_name in ('Delhi','Gurgaon') then 'Gurgaon' else supplier_city_name end)as supplier_city_name, fo.is_courier 
                  from data_model.f_order_consumer foc
                  inner join data_model.f_order fo on fo.order_id = foc.order_id
                  where foc.order_placed_date<month_date and foc.order_status_id in (9,10)
            )
      )as a
      inner join data_model.customer_segmentation_raw_data csrd on a.customer_id = csrd.customer_id
      where rownum=1
      group by 1,2,3,4,5,6,7
)
group by 1,2,3,4,5,6,7,8;
