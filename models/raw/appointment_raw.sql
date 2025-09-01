-- models/raw/appointment_raw.sql
{{ config(materialized='table') }}

with appointment as (
  select *,
         sysdate as mv_refresh_date,
         (partition_0 || partition_1 || partition_2)::date as partition_date,
         row_number() over (partition by id order by systemmodstamp desc) as rn
  from {{ source('src','appointment') }} a
  where (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
        (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date)
           from {{ source('src','appointment') }})
    and isdeleted = 'false'
)
select * from appointment where rn = 1