{{ config(materialized='table') }}

select *
from (select a.*,
             sysdate as                                                       mv_refresh_date,
             (partition_0 || '-' || partition_1 || '-' || partition_2)::date  partition_date,
             row_number() over (partition by id order by systemmodstamp desc) rn
      from dl_bi.at_risk_product__c a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date)
             from dl_bi.at_risk_product__c)
        and isdeleted = 'false')
where rn = 1