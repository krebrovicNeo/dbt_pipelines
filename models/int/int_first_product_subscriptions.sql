{{ config(materialized='ephemeral') }}

SELECT 
    s.account_id,
    s.product_code,
    MIN(s.subscription_start_date) as first_product_subscription_start_date
FROM {{ ref('account_product_subscriptions') }} s --derived_datasets.v_account_product_subscriptions_refactored s
GROUP BY 1,2