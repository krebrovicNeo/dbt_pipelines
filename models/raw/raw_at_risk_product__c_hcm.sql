{{ config(materialized='table') }}

select *
from (select id,
             createddate,
             account_name__c,
             stage__c,
             status__c,
             churn_date__c,
             at_risk_reason__c,
             churn_reason__c,
             explanation__c,
             additional_notes__c,
             out_of_period_churn_date__c,
             competitor_chosen_churn__c,
             competitor_chosen__c,
             churn_date_new__c,
             status_for_churn__c,
             subscription_end_date__c,
             product_grouping__c,
             at_risk_product_name__c,
             systemmodstamp,
             isdeleted,
             customer_engagement_associated_record__c,
             sysdate                                                         as mv_refresh_date,
             (partition_0 || '-' || partition_1 || '-' || partition_2)::date as partition_date,
             partition_3,
             row_number() over (partition by id order by systemmodstamp desc)   rn
      from dl_bi.at_risk_product__c_hcm a
      where 1 = 1
        and (partition_0 || '-' || partition_1 || '-' || partition_2)::date =
            (select max((partition_0 || '-' || partition_1 || '-' || partition_2)::date)
             from dl_bi.at_risk_product__c_hcm)
        and isdeleted = 'false')
where rn = 1