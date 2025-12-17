{{ config(materialized='view') }}

WITH incentives AS (SELECT s.id                                                                        as subscription_id
                         , a.id                                                                        as account_id
                         , q.id                                                                        as quote_id
                         , a.name                                                                      as account_name
                         , a.date_became_customer__c                                                   as account_became_customer_date
                         , m.family                                                                    as product_family          -- this is brought from product now but check if it will be added, for HCM it's pulled from export: v_ngv_saasopticsitemmappingexport_productfamily_to_productcode
                         , m.item_code                                                                 as item_code               -- this is brought from product now but check if it will be added, for HCM it's pulled from export: v_ngv_saasopticsitemmappingexport_productfamily_to_productcode
                         , m.name                                                                      as item_name               -- this is brought from product now but check if it will be added, for HCM it's pulled from export: v_ngv_saasopticsitemmappingexport_productfamily_to_productcode
                         , case
                               when m.product_code = 'PowerTraining' then 'PowerPolicy'
                               else m.product_code end                                                 as product_code
                         , CASE
                               WHEN s.sbqq__terminateddate__c is not null AND current_date > s.sbqq__terminateddate__c
                                   THEN 'Terminated'
                               WHEN current_date < s.sbqq__startdate__c THEN 'Draft'
                               WHEN current_date > s.end_date__c THEN 'Expired'
                               ELSE 'Active' END                                                       as subscription_status

                         , s.sbqq__startdate__c                                                        as subscription_start_date -- the earliest startdate of subscription (multiyear deal)
                         , s.end_date__c                                                               as subscription_end_date
                         , SUM(s.final_year_arr__c)                                                    as subscription_arr
                         , SUM(s.arr_contracted__c)                                                    as subscription_arr_contracted
                         , SUM(s.line_arr__c)                                                          as subscription_arr_line
                         , c.enddate                                                                   as contract_end_date       -- New field from contract object
                         , a.hcm_legacy_id__c                                                          as account_legacy_id
                         , ql.product_name__c                                                          as product
                         , q.actual_subscription_term__c                                               as term
                         , o.incentive_type__c                                                         as incentive_type
                         , ROW_NUMBER()
                           OVER (PARTITION BY quote_id, item_code ORDER BY subscription_end_date DESC) AS rn
                         , sysdate                                                                     as refresh_ts
                    FROM {{ ref('raw_sbqq_subscription__c') }} s
--                     INNER JOIN churn_and_subscriptions cs
                             INNER JOIN {{ ref('raw_account') }} a ON s.sbqq__account__c = a.id
                             INNER JOIN {{ ref('raw_quoteline') }} ql
                                        ON ql.x18_digit_ql_id__c = s.sbqq__quoteline__c -- SBQQ__OriginalQuoteLine__c OR SBQQ__QuoteLine__c
                             LEFT JOIN {{ ref('raw_quote') }} q ON ql.sbqq__quote__c = q.id
                             INNER JOIN {{ ref('raw_opportunity') }} o ON o.SBQQ__PrimaryQuote__c = q.id
                             LEFT OUTER JOIN {{ ref('raw_product2') }} p
                                             ON s.sbqq__product__c = p.id
                             LEFT JOIN (select distinct item_code, name, family, product_code, business_division
                                        from {{ ref('raw_new_product2_mapping') }}) m
                                       on m.item_code = p.productcode
                             LEFT OUTER JOIN {{ ref('raw_contract') }} c -- new join
                                             ON s.sbqq__contract__c = c.id
                    WHERE o.incentive_type__c IS NOT NULL
                      AND o.incentive_type__c <> 'None'
                      AND s.final_year_arr__c > 0

                    GROUP BY s.id, a.id, q.id, a.name, a.date_became_customer__c, c.enddate,
                             s.sbqq__startdate__c, s.end_date__c, s.end_date__c,
                             ql.product_name__c, q.actual_subscription_term__c, a.hcm_legacy_id__c,
                             o.incentive_type__c, s.sbqq__terminateddate__c, m.family, m.item_code, m.name,
                             m.product_code, refresh_ts),
     incentives_per_quote AS (SELECT *
                                   , DENSE_RANK()
                                     OVER (PARTITION BY account_id ORDER BY subscription_start_date, quote_id, incentive_type ) AS idx
                              FROM incentives
                              WHERE rn = 1
                              GROUP BY subscription_id, account_id, quote_id, account_name,
                                       account_became_customer_date,
                                       product_family, item_code, item_name, product_code, subscription_status,
                                       subscription_start_date, subscription_end_date, subscription_arr,
                                       subscription_arr_contracted, subscription_arr_line, account_legacy_id,
                                       product, term, incentive_type, contract_end_date, rn, refresh_ts)
select *
from incentives_per_quote