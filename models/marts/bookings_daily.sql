{{ config(materialized='table') }}

WITH first_product_subscription AS (SELECT s.account_id,
                                           s.product_code,
                                           MIN(s.subscription_start_date) as first_product_subscription_start_date
                                    FROM derived_datasets.v_account_product_subscriptions_refactored s
                                    GROUP BY s.account_id,
                                             s.product_code),
     product_mappings AS (SELECT distinct item_code, -- Has duplicates. Only return unique item code values
                                          product_code,
                                          family,
                                          business_division
                          FROM {{ ref('new_product2_mapping_raw') }} ),
     active_subscriptions as
         (select distinct o.account_id, o.product_code, month_ending_date
          from derived_datasets.active_subscriptions_snapshot_monthly o),
     bookings AS (SELECT case
                             when
                                 pm.item_code in ('QC-001', 'QC-002')
                                 then a.brand_name_formula__c
                             else
                                 pm.business_division end                                         as business_division,
                         CASE
                             WHEN o.original_opportunity_owner_role__c ILIKE '%Customer Success Manager%' THEN 'HCM'
                             WHEN o.original_opportunity_owner_role__c ILIKE '%HCM%' THEN 'HCM'
                             WHEN o.original_opportunity_owner_role__c ILIKE '%Power%' THEN 'Power'
                             ELSE a.brand_name_formula__c END                                     as sales_division,
                         CASE
                             WHEN a.date_became_customer__c < o.closedate THEN 'Existing'
                             ELSE 'New' END                                                       as business_type,
                         DATE(o.closedate)                                                        as reporting_date,
                         a.id                                                                     as account_id,
                         a.name                                                                   as account_name,
                         a.industry                                                               as account_market,           -- Agency Type will be migrated to the industry field. Will be different values after 10/01/2024 for HCM
                         a.type                                                                   as account_type,
                         a.shippingstate                                                          as account_state,
                         a.shippingpostalcode                                                     as account_postal_code,
                         NULL::text                                                               as segmentation_tier         -- #TODO: REVIEW - Does not exist in PDMS
                          ,
                         null::text                                                               as agency_type               -- #TODO: REVIEW - Does not exist in PDMS
                          ,
                         o.id                                                                     as opportunity_id,
                         o.type                                                                   as opportunity_type,
                         CASE WHEN issqo__c = 1 THEN o.appointment_date__c ELSE o.createddate END as opportunity_created_date, -- #TODO: REVIEW - check logic with RK
                         o.purchasing_vehicle__c                                                  as purchasing_vehicle,
                         o.third_party_reseller__c                                                as third_party_reseller,
                         o.co_op_agreement__c                                                     as co_op_agreement,
                         a.numberofemployees                                                      as total_ftes,               -- #TODO: REVIEW - check logic with RK
                         a.date_became_customer__c                                                as became_customer_date,
                         pm.product_code                                                          as product_code,
                         li.productcode                                                           as product_code_raw,
                         p.name                                                                   as item_name,                -- #TODO: REVIEW
                         p.family                                                                 as product_family,
                         li.product_family__c                                                     as products_from_quote,
                         li.product_type__c                                                       as product_type,
                         x.first_accred_association_date,
                         x.first_accred_required_platform_date,
                         x.is_accred_affiliated,
                         x.is_partner_affiliated,
                         x.accred_affiliation_ps,
                         MIN(date(o.closedate))
                         OVER (PARTITION BY a.id, m.raw_productfamily_value)                      as first_product_purchase_date,
                         fsub.first_product_subscription_start_date,
                         DENSE_RANK() OVER (
                             PARTITION BY o.id,
                                 li.productcode
                             ORDER BY li.calculate_arr_formula__c DESC,
                                 li.enddate__c DESC
                             )                                                                    as index,
                         CASE
                             WHEN li.product_type__c = 'Services' THEN NULLIF(li.net_total__c, '')::numeric
                             ELSE 0
                             END                                                                  as services_bookings_amount
                          ,
                         li.commissionable_arr_rep__c                                             as arr_bookings_amount,
                         NULL::float8                                                             as arr_bookings_amount_finance,
                         CASE
                             WHEN li.product_type__c = 'Services' THEN NULLIF(li.net_total__c, '')::numeric
                             ELSE li.commissionable_arr_rep__c
                             END                                                                  as total_bookings_amount,
                         NULL::float8                                                             as total_bookings_amount_finance,
                         CASE
                             WHEN
                                 (li.is_core_product__c = 1 -- #TODO: REVIEW - is core product on line items
                                     OR li.product_family__c IN (
                                                                 'Standards',
                                                                 'Training',
                                                                 'Schedule',
                                                                 'GovernmentJobs'
                                         )
                                     )
                                     AND p.product_type__c = 'Recurring'
                                     AND li.calculate_arr_formula__c = 1
                                     AND (
                                     (
                                         fsub.first_product_subscription_start_date >=
                                         DATEADD('day', -90, o.closedate) -- #TODO: REVIEW - check this logic!
                                         )
                                         AND li.upgraded_subscription__c IS NULL
                                     )
                                     and COALESCE(
                                                 li.commissionable_arr_rep__c,
                                                 li.line_arr__c,
                                                 li.arr__c
                                         ) < 0 THEN -1
                             WHEN (
                                      li.is_core_product__c = 1 -- #TODO: REVIEW - is core product on line items
                                          OR li.product_family__c IN (
                                                                      'Standards',
                                                                      'Training',
                                                                      'Schedule',
                                                                      'GovernmentJobs'
                                          )
                                      )
                                 AND li.isdeleted = 0
                                 AND p.isdeleted = 0
                                 AND p.product_type__c = 'Recurring'
                                 AND (
                                      (li.commissionable_arr_rep__c > 0)
                                          OR (li.line_arr__c > 0)
                                          OR (li.arr__c > 0)
                                      )
                                 AND li.calculate_arr_formula__c = 1
                                 AND (
                                      (
                                          (
                                              fsub.first_product_subscription_start_date >=
                                              DATEADD('day', -90, o.closedate) -- #TODO: REVIEW - check this logic!
                                              )
                                              AND li.upgraded_subscription__c IS NULL
                                          )
                                          or act.account_id is null
                                      )
                                 AND ROW_NUMBER() OVER (
                                     PARTITION BY o.closedate,
                                         o.accountid,
                                         li.productcode
                                     ORDER BY li.commissionable_arr_rep__c DESC,
                                         li.line_arr__c DESC,
                                         li.arr__c DESC,
                                         o.closedate DESC
                                     ) = 1 -- only attribute 1 unit per product/account/date
                                 THEN 1
                             ELSE 0
                             END                                                                  as arr_bookings_units,

                         CASE WHEN li.is_core_product__c = 1 THEN 1 ELSE 0 END                    as is_core_product,          -- New revision 20250128
                         o.owner_name__c                                                          as owner_name,
                         o.name                                                                   as opportunity_name,
                         o.original_opportunity_owner_role__c                                     as original_opp_owner_role,
                         r.name                                                                   as opp_owner_role,
                         a.referral_partner_content_provider__c                                   as referral_partner_content_provider
                          ,
                         CASE
                             WHEN li.product_family__c IN
                                  (SELECT m.products_from_quote
                                   FROM mappings.v_ngv_opplineitem_product_to_partnerintegration m) -- #TODO: REVIEW - check mappings for accuracy
                                 THEN 1
                             ELSE 0 END                                                              is_partner_integration,
                         li.isunit__c                                                             as is_unit

                  --         CASE
--             WHEN m.product_code = 'Integrations' THEN 1
--             ELSE 0
--         END is_partner_integration -- TODO: REVIEW - New logic revision based on product2 mappings updates 10/03/2024
                  FROM {{ ref('raw_opportunitylineitem') }}  li
                           LEFT OUTER JOIN sfdc_silver.mv_opplineitem_productfamily_to_productgroup m -- create raw in dbt
                                           ON li.product_family__c = m.raw_productfamily_value
                           LEFT OUTER JOIN product_mappings pm ON li.productcode = pm.item_code
                           INNER JOIN {{ ref('opportunity_raw') }}  o ON li.opportunityid = o.id
                           INNER JOIN {{ ref('account_raw') }}  a ON o.accountid = a.id
                           LEFT OUTER JOIN derived_datasets.v_pdms_account_partnership_attributes x
                                           ON x.account_id = a.id
                           LEFT OUTER JOIN {{ ref('product2_raw') }}  p ON li.product2id = p.id AND p.isdeleted = 0
                           LEFT OUTER JOIN first_product_subscription fsub
                                           ON fsub.account_id = a.id
                                               AND
                                              case
                                                  when fsub.product_code = 'PowerSchedule' then 'Schedule'
                                                  else fsub.product_code end =
                                              case
                                                  when m.mapped_productfamily_value = 'PowerSchedule' then 'Schedule'
                                                  else m.mapped_productfamily_value end
                           left outer join {{ ref('user_raw') }}  u on u.id = o.ownerid
                           left outer join {{ ref('userrole_raw') }}  r on r.id = u.userroleid
                           left join active_subscriptions act
                                     on act.account_id = a.id
                                         and act.product_code = pm.product_code
                                         and act.month_ending_date =
                                             DATEADD(day, -1, DATE_TRUNC('month', DATEADD(month, -2, o.closedate)))

                  WHERE o.stagename = 'Closed Won'
                    AND o.isdeleted = 0
                    AND li.isdeleted = 0
                    AND (
                      o.type IS NULL
                          OR NOT (
                          o."type"::text IN ('Migration Target', 'Renewal')
                          )
                      ) -- New Logic
                    AND NOT (
                      (
                          o.contract_1st_year__c = 0
                              AND o.opp1styrrevenue_master__c = 0
                          ) -- ignore opp line items where the parent opportunity has no revenue associated with it; we found some anomalies like this.
                          OR (
                          o.contract_1st_year__c IS NULL
                              AND o.opp1styrrevenue_master__c IS NULL
                          )
                      )
                    AND (o.original_opportunity_owner_role__c IS NULL OR
                         o.original_opportunity_owner_role__c NOT ILIKE 'FNL%')
                    AND o.closedate >= '2024-01-01'
     ),
     finance_bookings AS (SELECT
                              case
                                     when
                                         pm.item_code in ('QC-001', 'QC-002')
                                         then a.brand_name_formula__c
                                     else
                                         pm.business_division end::varchar(16383)                        as business_division,
                                 CASE
                                     WHEN o.original_opportunity_owner_role__c ILIKE '%Customer Success Manager%'
                                         THEN 'HCM'
                                     WHEN o.original_opportunity_owner_role__c ILIKE '%HCM%' THEN 'HCM'
                                     WHEN o.original_opportunity_owner_role__c ILIKE '%Power%' THEN 'Power'
                                     ELSE a.brand_name_formula__c END                                     as sales_division,
                                 CASE
                                     WHEN a.date_became_customer__c < o.closedate THEN 'Existing'
                                     ELSE 'New' END                                                       as business_type,
                                 DATE(o.closedate)                                                        as reporting_date,
                                 a.id                                                                     as account_id,
                                 a.name                                                                   as account_name,
                                 a.industry                                                               as account_market,           -- Agency Type will be migrated to the industry field. Will be different values after 10/01/2024 for HCM
                                 a.type                                                                   as account_type,
                                 a.shippingstate                                                          as account_state,
                                 a.shippingpostalcode                                                     as account_postal_code,
                                 NULL::text                                                               as segmentation_tier         -- #TODO: REVIEW - Does not exist in PDMS
                                  ,
                                 null::text                                                               as agency_type               -- #TODO: REVIEW - Does not exist in PDMS
                                  ,
                                 o.id                                                                     as opportunity_id,
                                 o.type                                                                   as opportunity_type,
                                 CASE WHEN issqo__c = 1 THEN o.appointment_date__c ELSE o.createddate END as opportunity_created_date, -- #TODO: REVIEW - check logic with RK
                                 o.purchasing_vehicle__c                                                  as purchasing_vehicle,
                                 o.third_party_reseller__c                                                as third_party_reseller,
                                 o.co_op_agreement__c                                                     as co_op_agreement,
                                 a.numberofemployees                                                      as total_ftes,               -- #TODO: REVIEW - check logic with RK
                                 a.date_became_customer__c                                                as became_customer_date,
                                 pm.product_code                                                          as product_code,
                                 li.productcode                                                           as product_code_raw,
                                 p.name                                                                   as item_name,                -- #TODO: REVIEW
                                 p.family                                                                 as product_family,
                                 li.product_family__c                                                     as products_from_quote,
                                 li.product_type__c                                                       as product_type,

                                 x.first_accred_association_date,
                                 x.first_accred_required_platform_date,
                                 x.is_accred_affiliated,
                                 x.is_partner_affiliated,
                                 x.accred_affiliation_ps,


                                 MIN(date(o.closedate))
                                 OVER (PARTITION BY a.id, m.raw_productfamily_value)                      as first_product_purchase_date,
                                 fsub.first_product_subscription_start_date,


                                  DENSE_RANK() OVER (
                                  PARTITION BY o.id,
                                      li.productcode
                                  ORDER BY li.final_year_arr_rep__c DESC,
                                      li.enddate__c DESC
                                  )::bigint                                                 as index,

                                      CASE
                                          WHEN li.product_type__c = 'Services' THEN NULLIF(li.net_total__c, '')::float8
                                          ELSE 0
                                          END                                               as services_bookings_amount
                                  ,
                                      NULL::float8                                          as arr_bookings_amount
                                  ,
                                      li.final_year_arr_rep__c::float8                      as arr_bookings_amount_finance,
                                      NULL::float8                                          as total_bookings_amount,
                                      CASE
                                          WHEN li.product_type__c = 'Services' THEN NULLIF(li.net_total__c, '')::float8
                                          ELSE li.final_year_arr_rep__c::float8
                                      END                                                   as total_bookings_amount_finance,
                                      NULL::integer                                         as arr_bookings_units,
                                      CASE WHEN li.is_core_product__c = 1 THEN 1 ELSE 0 END as is_core_product -- New revision 20250128

                          ,o.owner_name__c                                                          as owner_name,
                          o.name                                                                   as opportunity_name,
                          o.original_opportunity_owner_role__c                                     as original_opp_owner_role,
                          r.name                                                                   as opp_owner_role,
                          a.referral_partner_content_provider__c                                   as referral_partner_content_provider
                           ,
                          CASE
                              WHEN li.product_family__c IN
                                   (SELECT m.products_from_quote
                                    FROM mappings.v_ngv_opplineitem_product_to_partnerintegration m)
                                  THEN 1
                              ELSE 0 END                                                              is_partner_integration,
                          li.isunit__c                                                             as is_unit


                          FROM sfdc_silver.mv_opportunitylineitem_merge li
                                   LEFT OUTER JOIN sfdc_silver.mv_opplineitem_productfamily_to_productgroup m
                                                   ON li.product_family__c = m.raw_productfamily_value
                                   LEFT OUTER JOIN product_mappings pm ON li.productcode = pm.item_code
                                   INNER JOIN {{ ref('opportunity_raw') }}  o ON li.opportunityid = o.id
                                   INNER JOIN {{ ref('account_raw') }}  a ON o.accountid = a.id
                                   LEFT OUTER JOIN derived_datasets.v_pdms_account_partnership_attributes x
                                                   ON x.account_id = a.id
                                   LEFT OUTER JOIN {{ ref('product2_raw') }}  p
                                                   ON li.product2id = p.id AND p.isdeleted = 0
                                   LEFT OUTER JOIN first_product_subscription fsub
                                                   ON fsub.account_id = a.id
                                                       AND
                                                      case
                                                          when fsub.product_code = 'PowerSchedule' then 'Schedule'
                                                          else fsub.product_code end =
                                                      case
                                                          when m.mapped_productfamily_value = 'PowerSchedule'
                                                              then 'Schedule'
                                                          else m.mapped_productfamily_value end
                                   left outer join {{ ref('user_raw') }}  u on u.id = o.ownerid
                                   left outer join {{ ref('userrole_raw') }}  r on r.id = u.userroleid
                          WHERE o.stagename = 'Closed Won'
                            AND o.isdeleted = 0
                            AND li.isdeleted = 0
                            AND li.final_year_arr_line_item__c = TRUE
                            AND COALESCE(o.owner_name__c, '') NOT ILIKE '%agency360%'
                            AND COALESCE(o.name, '') NOT ILIKE '%Quota Credit%'
                            AND COALESCE(li.renewed_subscription__c, '') = ''
                            AND (
                              UPPER(TRIM(o.type)) IN ('EXISTING BUSINESS', 'NEW BUSINESS')
                                  OR (
                                  UPPER(TRIM(o.type)) = 'RENEWAL + ADD-ON'
                                      AND COALESCE(UPPER(TRIM(li.revenue_metric__c)), '') <> 'RENEWAL'
                                  )
                              )
                            )
        ,
     bookings_hist AS (SELECT CASE
                                  WHEN h.business_division = 'PDMS'
                                      THEN 'Power'
                                  ELSE h.business_division END               business_division,
                              NULL                                        as sales_division,
                              business_type,
                              reporting_date,
                              isnull(am.account_id, h.account_id)         as account_id,
                              account_name,
                              account_market,
                              account_type,
                              account_state,
                              account_postal_code,
                              segmentation_tier,
                              agency_type,
                              isnull(om.opportunity_id, h.opportunity_id) as opportunity_id,
                              opportunity_type,
                              opportunity_created_date,
                              purchasing_vehicle,
                              third_party_reseller,
                              co_op_agreement,
                              total_ftes,
                              became_customer_date,
                              isnull(pm.product_code, h.product_code)     as product_code,
                              product_code_raw,
                              item_name,
                              pm.family                                   as product_family,
                              products_from_quote,
                              product_type,
                              first_accred_association_date,
                              first_accred_required_platform_date,
                              is_accred_affiliated,
                              NULL::boolean                               as is_partner_affiliated,
                              accred_affiliation_ps,
                              first_product_purchase_date,
                              first_product_subscription_start_date,
                              NULL::bigint                                AS index,
                              NULL::float8                                AS services_bookings_amount,
                              arr_bookings_amount,
                              NULL::float8                                AS arr_bookings_amount_finance,
                              total_bookings_amount,
                              NULL::float8                                AS total_bookings_amount_finance,
                              arr_bookings_units,
                              is_core_product,
                              owner_name,
                              h.opportunity_name                          as opportunity_name,
                              NULL::varchar                               as original_opp_owner_role,
                              NULL::varchar                               as opp_owner_role,
                              referral_partner_content_provider,
                              is_partner_integration,
                              NULL::float8                      as is_unit
                       FROM sfdc_silver.bookings_daily_hist h
                                left join product_mappings pm ON h.product_code_raw = pm.item_code -- create a macro in dbt
                                left join derived_datasets.v_account_mapping am
                                          on am.hcm_legacy_id__c = h.account_id
                                left join derived_datasets.v_opportunity_mapping om
                                          on om.hcm_legacy_id__c = h.opportunity_id)

, bookings_with_hist AS (
    SELECT *
      FROM bookings
      WHERE reporting_date >= '2024-01-01'
        AND index = 1
      UNION ALL
      SELECT *
      FROM bookings_hist
      WHERE reporting_date < '2024-01-01'
)

, final AS (
    SELECT * FROM bookings_with_hist
    UNION ALL
    SELECT * FROM finance_bookings
        WHERE index = 1
)

SELECT
    row_number() OVER ()                                         AS bookings_daily_id
     , business_division
     , sales_division
    -- attributes from combined
     , CASE
           WHEN acc.date_became_customer__c < DATEADD('day', -31, combined.reporting_date)
               THEN 'Existing' -- add 31-day window
           ELSE 'New'
    END                                                             as business_type
     , reporting_date
     , coalesce(acc.id, combined.account_id)                        as account_id
     , coalesce(acc.name, combined.account_name)                    as account_name
     , coalesce(acc.industry, combined.account_market)              as account_market
     , coalesce(acc.type, combined.account_type)                    as account_type
     , coalesce(acc.shippingstate, account_state)                   as account_state
     , coalesce(acc.shippingpostalcode, account_postal_code)        as account_postal_code
     , coalesce(acc.segmentation_tier__c, account_postal_code)      as segmentation_tier
     , agency_type
     , opportunity_id
     , coalesce(opp.type, combined.opportunity_type)                as opportunity_type
     , coalesce(opp.createddate, combined.opportunity_created_date) as opportunity_created_date
     , purchasing_vehicle
     , third_party_reseller
     , co_op_agreement
     , total_ftes
     , became_customer_date
     , product_code
     , product_code_raw
     , item_name
     , product_family
     , products_from_quote
     , product_type
     , first_accred_association_date
     , first_accred_required_platform_date
     , is_accred_affiliated
     , is_partner_affiliated
     , accred_affiliation_ps

     , first_product_purchase_date
     , first_product_subscription_start_date

     , index::bigint

     , services_bookings_amount::float8
     , arr_bookings_amount::float8
     , arr_bookings_amount_finance::float8
     , total_bookings_amount::float8
     , total_bookings_amount_finance::float8
     , arr_bookings_units
     , is_core_product
    , owner_name
    , opportunity_name
    , original_opp_owner_role
    , opp_owner_role
    , referral_partner_content_provider
    , is_partner_integration
    , is_unit

    , dc.week_end                                                  as reporting_week_end
    , dc.week_start                                                as reporting_week_start
    , dc.working_day_of_month
    , dc.working_day_of_quarter
    , dc.working_day_of_year
    , dc.comp_date
    , dc.comp_week_end
    , dc.comp_week_start
    , dc.current_vs_previous

FROM final combined
    INNER JOIN derived_datasets.v_dates dc ON combined.reporting_date = dc.dt
    LEFT JOIN {{ ref('account_raw') }}  acc on acc.id = combined.account_id
    left join {{ ref('opportunity_raw') }}  opp on opp.id = combined.opportunity_id
WHERE 1 = 1
  AND account_name <> 'NEOGOV (CA)'
  AND account_name NOT ilike '%test%'
