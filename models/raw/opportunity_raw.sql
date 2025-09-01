{{ config(materialized='table') }}

with opportunity as (select *,
             sysdate as                                                       mv_refresh_date,
             (partition_0 || partition_1 || partition_2)::date                partition_date,
             row_number() over (partition by id order by systemmodstamp desc) rn
      from dl_bi.opportunity a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from dl_bi.opportunity)
        and isdeleted = 'false')
select *
from opportunity
where rn = 1