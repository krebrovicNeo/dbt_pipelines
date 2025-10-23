{{ config(materialized='view') }}

with product_subscriptions AS (SELECT aps.business_division,
                                      --                                  aps.subscription_end_of_month_date as month_ending,
                                      last_day(date_add('day', 1, aps.subscription_end_date))::date as month_ending, -- TODO - REVIEW: Make sure this is accurate across both business divisions
                                      aps.account_name,
                                      aps.account_id,
--                                       aps.subscription_id,
                                      aps.account_became_customer_date,
                                      aps.product_code                                              as product_code,
                                      aps.item_code                                                 as subscription_item_code,
                                      aps.item_name::text                                           as subscription_item_name,
                                      aps.subscription_start_date,
                                      aps.subscription_end_date,
                                      aps.subscription_arr::numeric(18, 2)                          as subscription_arr,
                                      aps.state                                                     as account_state,
                                      aps.postal_code                                               as account_postal_code,
                                      aps.total_fte                                                 as total_fte,    -- this was employees till 2025-06-11
                                      aps.industry                                                  as territory,
                                      aps.industry                                                  as industry,
                                      aps.first_subscription_start_date,
                                      aps.contract_end_date,
                                      aps.product_family,
                                      ipq.incentive_type,
                                      ROW_NUMBER() OVER ()                                          as product_subscription_index
                                       ,
                                      sysdate                                                       as refresh_ts
                               FROM derived_datasets.account_product_subscriptions aps
                                        LEFT OUTER JOIN staging.tmp_apc_incentive_per_quote ipq ON ipq.idx = 1
                                   AND aps.account_id = ipq.account_id
                                   AND aps.item_code = ipq.item_code
                                   AND aps.subscription_end_date = ipq.subscription_end_date)
select *
from product_subscriptions