{{ config(materialized='table') }}

select distinct a.*,
                sysdate                                                         as mv_refresh_date,
                (partition_0 || '-' || partition_1 || '-' || partition_2)::date as partition_date
from {{ source('src','HistoryHCM') }} a
where 1 = 1
  and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
      (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from {{ source('src','HistoryHCM') }})