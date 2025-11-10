{{ config(materialized='view') }}

with opportunitylineitem as (select *,
             sysdate as                                                       mv_refresh_date,
             (partition_0 || partition_1 || partition_2)::date                partition_date,
             row_number() over (partition by id order by systemmodstamp desc) rn
      from {{ source('src','opportunitylineitem') }} a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from {{ source('src','opportunitylineitem') }})
        and isdeleted = 'false')
select *
from opportunitylineitem
where rn = 1