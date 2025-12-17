{{ config(materialized='table') }}

select *
from (select id,
             title,
             createddate,
             accountid,
             lead_source_marketing__c,
             name,
             isactive__c,
             phone,
             createdbyadminsync__c,
             a.isprimaryisamasteradmin__c,
             a.lead_contact_status__c,
             lead_source_most_recentt__c                                     as lead_source_most_recent__c,
             isdeleted,
             email,
             a.account_has_buyer__c,
             a.contact_has_buyer__c,
             a.call_priority_score_group__c,
             a.call_priority_score__c,
             a.contact_priority_score__c,
             a.contact_priority_score_bucket__c,
             a.priority_contact__c,
             a.recommend_product_1__c,
             a.recommend_product_2__c,
             a.recommend_product_list__c,
             sysdate                                                         as mv_refresh_date,
             (partition_0 || '-' || partition_1 || '-' || partition_2)::date as partition_date,
             row_number() over (partition by id order by createddate desc)      rn
      from dl_bi.contact a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date) from dl_bi.contact)
        and a.isdeleted = 'false')
where rn = 1
