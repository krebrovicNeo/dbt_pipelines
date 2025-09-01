{{ config(materialized='table') }}

with product2 as (select *,
             sysdate as                                                       mv_refresh_date,
             (partition_0 || partition_1 || partition_2)::date                partition_date,
             row_number() over (partition by id order by systemmodstamp desc) rn
      from dl_bi.product2 a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from dl_bi.product2 )
        and isdeleted = 'false')
select *
from product2
where rn = 1