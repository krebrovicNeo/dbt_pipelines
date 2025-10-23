{{ config(materialized='table') }}

with account_hcm AS (SELECT x.*
                     FROM (SELECT DISTINCT a.name
                                         , a.id
                                         , a.date_became_customer__c
                                         , a.shippingstate
                                         , a.shippingpostalcode
                                         , a.total_employees__c
                                         , a.territory2__c
                                         , a.industry
                                         , a.parentid
                                         , a.brand_name_formula__c
                                         , a.hcm_legacy_id__c
                                         , ROW_NUMBER() OVER (partition by name order by a.hcm_legacy_id__c) as index
                           FROM {{ ref('account_raw') }} a
                           WHERE a.hcm_legacy_id__c IS NOT NULL) x
                     WHERE x.index = 1),
     churn_sheet_hcm AS (SELECT 'HCM'                                                                            as business_division
                              , NULLIF(e.month, ' ')::date                                                       as change_month_ending
                              , e."customer: name"::varchar(1000)                                                as saasoptics_customer_name
                              , a.name::varchar(1000)                                                            as account_name
                              , a.id::varchar(1000)                                                              as account_id
                              , a.hcm_legacy_id__c::varchar(1000)                                                as hcm_legacy_id
                              , a.date_became_customer__c::date                                                  as account_became_customer_date
                              , e."product code"::varchar(1000)                                                  as product_code
                              , NULL::varchar(1000)                                                              as item_code
                              , NULL::varchar(1000)                                                              as subscription_item_name
                              , NULL::date                                                                       as subscription_start_date
                              , NULLIF(e.month, ' ')::date                                                       as subscription_end_date
                              , e.type::varchar(1000)                                                            as change_type
                              , ipq.incentive_type                                                               as incentive_type
                              , NULLIF(REPLACE(replace("churn amount", '$', ''), ',', ''), '')::double precision as subscription_arr
                              , a.shippingstate::varchar(1000)                                                   as account_state
                              , a.shippingpostalcode::varchar(1000)                                              as account_postal_code
                              , a.total_employees__c                                                             as total_fte
                              , a.territory2__c::varchar(1000)                                                   as territory
                              , a.industry::varchar(1000)                                                        as industry
                              , a.parentid::varchar(1000)                                                        as parent_account_id
                              , e."churn reason revised"                                                         as churn_reason_revised
                              , e.playbook                                                                       as playbook

                              , cd.product_cancellation_date                                                     as product_cancellation_date
                              , cd.product_cancellation_month_ending                                             as product_cancellation_month_ending

                              , init_subs.first_subscription_start_date::date                                    as first_subscription_start_date
                              , CASE WHEN e."closed month flag" = 'TRUE' THEN 1 ELSE 0 END                       as closed_month_flag
                              , NULLIF(e."last refresh date", ' ')::date                                         as last_refresh_date
                              , row_number()
                                over (partition by a.id, e."product code",NULLIF(REPLACE(replace("churn amount", '$', ''), ',', ''), '')::double precision
                                    order by change_month_ending desc, init_subs.first_subscription_start_date)  as idx
                              , sysdate                                                                          as refresh_ts
                         FROM 
                             {{ ref('historyhcm_raw') }} e


                                  LEFT OUTER JOIN account_hcm a
                                                  ON e."customer: name" = a.name

                                  LEFT OUTER JOIN {{ ref('stg_apc_product_subscriptions') }} init_subs
                                                  ON a.id = init_subs.account_id

                                  LEFT OUTER JOIN {{ ref('stg_apc_incentive_per_quote') }} ipq ON ipq.idx = 1
                                  AND a.id = ipq.account_id
                                  AND e."product code" = ipq.product_code

                                  LEFT OUTER JOIN derived_datasets.v_account_product_with_cancellation_dates cd
                                                  ON init_subs.account_id = cd.account_id
                                                      AND e."product code" = cd.product_code
                         WHERE (e.exclude IS NULL or e.exclude = 0 or e.exclude = '')

     )
select *
from churn_sheet_hcm