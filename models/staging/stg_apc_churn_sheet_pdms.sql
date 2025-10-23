{{ config(materialized='view') }}


SELECT 'PDMS'                                  as                                                                                       business_division, -- #TODO: REVIEW - Should we use "Brand" or Product Division for business division? This was not discussed
       NULLIF(c.month, ' ')::date              as                                                                                       change_month_ending,
       --                          , c."customer: name" as churn_account_name
       a.name                                  as                                                                                       account_name,
       a.id                                    as                                                                                       account_id,
       a.date_became_customer__c               as                                                                                       account_became_customer_date,
       c.product                               as                                                                                       product_code,
       NULL::varchar(1000)                     as                                                                                       subscription_item_name,
       NULL::date                              as                                                                                       subscription_start_date,
       NULLIF(c.month, ' ')::date              as                                                                                       subscription_end_date,
       c.type                                  as                                                                                       change_type,
       ipq.incentive_type                      as                                                                                       incentive_type,
       c."churn amount"                        as                                                                                       subscription_arr,
       -- SUM makes sure we capture all ARR when there is no match on account, and account specific attributes are NULL. Without this we would undercount amount.
       a.shippingstate                         as                                                                                       account_state,
       a.shippingpostalcode                    as                                                                                       account_postal_code,
       a.numberofemployees                     as                                                                                       total_fte,
       a.territory2__c                         as                                                                                       territory,
       a.industry                              as                                                                                       industry,
       c."churn description power"             as                                                                                       churn_description_power,
       c."cancellation description power"      as                                                                                       cancellation_description_power,
       init_subs.first_subscription_start_date as                                                                                       first_subscription_start_date,
       CASE
           WHEN c."closed month flag" = 'TRUE' THEN 1
           ELSE 0
           END                                 as                                                                                       closed_month_flag,
       NULLIF("last refresh date", ' ')::date  as                                                                                       last_refresh_date,
       c."account number"                      as                                                                                       account_number,
       row_number()
       over (partition by a.id, c.product, c."churn amount" order by change_month_ending desc, init_subs.first_subscription_start_date) idx
        ,
       sysdate                                 as                                                                                       refresh_ts
FROM {{ ref('historypdms_raw') }} c
         LEFT OUTER JOIN  {{ ref('account_raw') }} a ON a.intacctid__c = c."account number"
         LEFT OUTER JOIN  {{ ref('stg_apc_first_product_subscriptions') }} init_subs ON init_subs.account_id = a.id
    AND init_subs.product_code = c.product
         LEFT OUTER JOIN  {{ ref('stg_apc_incentive_per_quote') }} ipq ON ipq.idx = 1
    AND a.id = ipq.account_id
    AND c.product = ipq.product_code
    WHERE (
    c.exclude IS NULL
        or c.exclude = 0
        or c.exclude = ''
    )
    AND abs(c."churn amount") > 1