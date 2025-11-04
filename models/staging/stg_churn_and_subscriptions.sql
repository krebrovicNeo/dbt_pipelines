{{ config(materialized='table') }}

WITH incentives_per_quote AS
         (
          SELECT *
          from  {{ ref('stg_apc_incentive_per_quote') }}
          ) ,
     product_subscriptions AS
         (
          select *
          from {{ ref('stg_apc_product_subscriptions') }}
         ),
     first_product_subscriptions AS
         (
        select *
          from {{ ref('stg_apc_first_product_subscriptions') }}
          ),
     churn_sheet_pdms AS (select *
                          from {{ ref('stg_apc_churn_sheet_pdms') }}
                          ),
     churn_sheet_pdms_dedup as (select change_month_ending,
                                       account_name,
                                       account_id,
                                       account_became_customer_date,
                                       product_code,
                                       subscription_item_name,
                                       subscription_start_date,
                                       subscription_end_date,
                                       change_type,
                                       incentive_type,
                                       sum(subscription_arr)::double precision as subscription_arr
                                        ,
                                       account_state,
                                       account_postal_code,
                                       total_fte,
                                       territory,
                                       industry,
                                       churn_description_power,
                                       cancellation_description_power,
                                       first_subscription_start_date,
                                       closed_month_flag,
                                       last_refresh_date
                                from churn_sheet_pdms p
                                where p.idx = 1
                                group by change_month_ending,
                                         account_name,
                                         account_id,
                                         account_became_customer_date,
                                         product_code,
                                         subscription_item_name,
                                         subscription_start_date,
                                         subscription_end_date,
                                         change_type,
                                         incentive_type,
                                         account_state,
                                         account_postal_code,
                                         total_fte,
                                         territory,
                                         industry,
                                         churn_description_power,
                                         cancellation_description_power,
                                         first_subscription_start_date,
                                         closed_month_flag,
                                         last_refresh_date),
     account_hcm AS (SELECT x.*
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
                           FROM  "bidb"."dbt_datasets"."account_raw" a
                           WHERE a.hcm_legacy_id__c IS NOT NULL) x
                     WHERE x.index = 1),
     churn_sheet_hcm AS (select *
                         from "bidb"."dbt_datasets"."stg_apc_churn_sheet_hcm"
         --  where account_id = '001Uh00000EOBT4IAP'
         -- and subscription_end_date = '2024-10-31'
     ),
     churn_sheet_hcm_dedup as
         (select change_month_ending,
                 saasoptics_customer_name,
                 account_name,
                 account_id,
                 hcm_legacy_id,
                 account_became_customer_date,
                 product_code,
                 item_code,
                 subscription_item_name,
                 subscription_start_date,
                 subscription_end_date,
                 change_type,
                 incentive_type,
                 sum(subscription_arr)::double precision              as subscription_arr,
                 account_state,
                 account_postal_code,
                 total_fte,
                 territory,
                 industry,
                 min(first_subscription_start_date) as first_subscription_start_date,
                 closed_month_flag,
                 last_refresh_date
          from churn_sheet_hcm
          where idx = 1
          group by change_month_ending,
                   saasoptics_customer_name,
                   account_name,
                   account_id,
                   hcm_legacy_id,
                   account_became_customer_date,
                   product_code,
                   item_code,
                   subscription_item_name,
                   subscription_start_date,
                   subscription_end_date,
                   change_type,
                   incentive_type,
                   account_state,
                   account_postal_code,
                   total_fte,
                   territory,
                   industry,
                   closed_month_flag,
                   last_refresh_date)
        ,
     churn_and_subscriptions AS (SELECT 'Power'                business_division,
                                        change_month_ending,
                                        NULL::varchar(1000) as saasoptics_customer_name,
                                        account_name,
                                        account_id,
                                        NULL::varchar(1000) as hcm_legacy_id,
                                        account_became_customer_date,
                                        product_code,
                                        NULL::varchar(1000) as subscription_item_code,
                                        subscription_item_name,
                                        subscription_start_date,
                                        subscription_end_date,
                                        change_type,
                                        incentive_type,
                                        subscription_arr::double precision,
                                        account_state,
                                        account_postal_code,
                                        total_fte,
                                        territory,
                                        industry,
                                        churn_description_power,
                                        cancellation_description_power,
                                        first_subscription_start_date,
                                        closed_month_flag,
                                        null::date          as contract_end_date,
                                        last_refresh_date
                                 FROM churn_sheet_pdms_dedup c
                                 UNION ALL
                                 SELECT 'HCM'               as business_division,
                                        change_month_ending,
                                        saasoptics_customer_name,
                                        account_name,
                                        account_id,
                                        hcm_legacy_id,
                                        account_became_customer_date,
                                        product_code,
                                        item_code           as subscription_item_code,
                                        subscription_item_name,
                                        subscription_start_date,
                                        subscription_end_date,
                                        change_type,
                                        incentive_type,
                                        subscription_arr::double precision,
                                        account_state,
                                        account_postal_code,
                                        total_fte,
                                        territory,
                                        industry,
                                        NULL::varchar(1000) as churn_description_power,
                                        NULL::varchar(1000) as cancellation_description_power,
                                        first_subscription_start_date,
                                        closed_month_flag,
                                        null::date          as contract_end_date,
                                        last_refresh_date
                                 FROM churn_sheet_hcm_dedup c
                                 UNION ALL
                                 SELECT s.business_division,
                                        month_ending,
                                        NULL::varchar(1000) as saasoptics_customer_name,
                                        account_name,
                                        account_id,
                                        NULL::varchar(1000) as hcm_legacy_id,
                                        account_became_customer_date,
                                        product_code,
                                        subscription_item_code,
                                        subscription_item_name,
                                        subscription_start_date,
                                        subscription_end_date,
                                        NULL::varchar(1000) as change_type,
                                        incentive_type,
                                        subscription_arr::double precision,
                                        account_state,
                                        account_postal_code,
                                        total_fte,
                                        territory,
                                        industry,
                                        NULL::varchar(1000) as churn_description_power,
                                        NULL::varchar(1000) as cancellation_description_power,
                                        first_subscription_start_date,
                                        NULL::int                as closed_month_flag,
                                        contract_end_date,
                                        null::date          as last_refresh_date
                                 FROM product_subscriptions s
                                 )
                                 select * from churn_and_subscriptions