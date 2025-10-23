{{ config(materialized='view') }}

with     first_product_subscriptions AS (SELECT p.account_name,
                                            p.account_id,
                                            --                                                   , aps.account_became_customer_date
                                            --                                                   , aps.state
                                            --                                                   , aps.postal_code
                                            --                                                   , aps.employees
                                            --                                                   , aps.industry
                                            p.product_code,
                                            p.business_division,
                                            MIN(p.first_subscription_start_date) as first_subscription_start_date
                                     FROM {{ ref('stg_apc_product_subscriptions') }} p
                                     GROUP BY p.account_name,
                                              p.account_id,
                                              p.product_code,
                                              p.business_division)
select *
from first_product_subscriptions