{{ config(materialized='table') }}

with pm as (select *,
             sysdate as                                                       mv_refresh_date,
             (partition_0 || partition_1 || partition_2)::date                partition_date
      from bidb.dl_bi.new_product2_mapping a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from bidb.dl_bi.new_product2_mapping)
       )
select *
from pm
