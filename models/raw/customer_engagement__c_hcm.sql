{{ config(materialized='table') }}

select id,
       createddate,
       additional_information_reason_if_known__c,
       comments__c,
       date_aware_of_risk__c,
       account__c,
       name,
       isdeleted,
       sysdate as mv_refresh_date
from dl_bi.customer_engagement__c_hcm a
where 1 = 1
  and partition_0 || partition_1 || partition_2::int =
      (select max(partition_0 || partition_1 || partition_2::int) from dl_bi.customer_engagement__c_hcm)