{{ config(materialized='view') }}


with account_active_history as
         (select distinct account_id,
                          month_ending_date
          from derived_datasets.active_subscriptions_snapshot_monthly a
          where a.product_code in ('AT',
                                   'BE',
                                   'EF',
                                   'HR',
                                   'IN',
                                   'LE',
                                   'ON',
                                   'PE',
                                   'PowerAction',
                                   'PowerEngage',
                                   'PowerFTO',
                                   'PowerReady',
                                   'PowerIA',
                                   'PowerLine',
                                   'PowerPolicy',
                                   'PowerRecall',
                                   'PowerTime',
                                   'PowerVitals',
                                   'PR',
                                   'Schedule',
                                   'TA',
                                   'Vetted')
            and a.item_name not in ('Candidate Text Messaging',
                                    'Candidate Text Messaging Subscription', 'Renewal ARR Uptick'))
        ,
     pipeline_daily      as
     (
SELECT LPAD(CAST(ROW_NUMBER() OVER () AS VARCHAR), 9, '0') ||
                 SUBSTRING(MD5(RANDOM()::VARCHAR), 0, 7)                  AS sales_funnel_daily_id,
                 dc.comp_date,
                 dc.comp_week_start,
                 dc.comp_week_end,
                 dc.current_vs_previous,
                 opportunity.sqo_date__c                                  AS reporting_date,

                 dc.week_end                                              AS reporting_week_end,
                 dc.week_start                                            AS reporting_week_start,

                 dc.working_day_of_month,
                 dc.working_day_of_quarter,

                 dc.working_day_of_year,

                 CASE
                     WHEN opportunity.original_opportunity_owner_role__c ilike '%power%' THEN 'Power'
                     WHEN opportunity.original_opportunity_owner_role__c ilike '%hcm%' THEN 'HCM'
                     WHEN account.brand_name_formula__c ilike '%power%' THEN 'Power'
                     WHEN account.brand_name_formula__c ilike '%hcm%' OR
                          account.brand_name_formula__c ilike '%neoed%' THEN 'HCM'
                     WHEN opportunity.original_opportunity_owner_role__c IS NOT NULL OR
                          account.brand_name_formula__c IS NOT NULL THEN 'All'
                     ELSE 'Missing'
                     END::VARCHAR(10)                                     AS business_division,
                 opportunity.accountid                                    AS account_id,
                 opportunity.id                                           AS opportunity_id,
                 NULL::text                                               AS appointment_id,
                 account.name                                             as account_name,
                 account.date_became_customer__c                          as became_customer_date,
                 CASE
                     WHEN account.date_became_customer__c < DATEADD('day', -31, reporting_date)
                         THEN 'Existing' -- add 31-day window
                     ELSE 'New'
                     END                                                  as business_type,
                 CASE
                     WHEN ah2.account_id is not null THEN 'Existing'
                     ELSE 'New'
                     END                                                  AS derived_business_type,
                 account.industry                                         as market_segment,
                 account.total_fte__c                                     as agency_ftes,
                 null::double precision                                   as account_fiscal_month,
                 account.shippingstate                                    as state,
                 account.shippingpostalcode                               as postal_code,
                 um.name                                                  as account_owner,
                 um.manager__c                                            as account_owner_manager,
                 account.istam__c                                         as account_is_tam,
                 account.type                                             as account_type,
                 account.brand_name_formula__c                            as brand_name,
                 account.segmentation_tier__c                             as segmentation_tier,
                 --- agg fields
                 NULL::text                                               AS demo_source,
                 opportunity.original_appointment_booked_by_role__c::text AS original_role,
                 opportunity.original_opportunity_owner_role__c::text     AS opportunity_owner_role,
                 opportunity.type                                         AS opportunity_type,
                 opportunity.main_product__c                              AS opportunity_main_product,
                 NULL::text                                               AS closed_won_products,
                 0                                                        AS leads_new_contacts,
                 0                                                        AS outbound_calls,
                 0                                                        AS correct_contact_calls,
                 0                                                        as accounts_created,
                 0                                                        AS demos_booked,
                 0                                                        AS pr_demos_booked,
                 0                                                        AS pr_demos_scheduled,
                 0                                                        as pr_demos_completed,
                 0                                                        AS sqo_count,
                 NULL::bigint                                             AS demos_booked_to_scheduled_days,
                 NULL::bigint                                             AS demos_booked_to_scheduled_days_original,
                 NULL::date                                               AS original_appointment_date,
                 0                                                        AS demos_completed,
                 coalesce(opportunity.original_appointment_owner__c::text,
                          created_by_user.name)                           AS booked_by,
                 owned_by_user.name                                       AS booked_for,
                 owned_by_user.isactive                                   AS booked_for_user_isactive,
                 opportunity.original_opportunity_owner_role__c           AS booked_for_role,
                 coalesce(opportunity.Original_Appointment_Booked_By_Role__c,
                          user_role.name)                                 AS booked_by_role,
                 NULL::date                                               AS user_created_date,
                 null::timestamp                                          as booked_for_user_created_date,
                 0                                                        AS demos_scheduled,


                 0                                                        AS opps_closed_won,
                 0                                                        AS opps_closed_lost,
                 owned_by_user.name                                       AS opp_owner,
                 null::text                                               as opp_contact,
                 null::text                                               as opp_contact_role,
                 0::double precision                                      AS arr_bookings_amount,
                 0::double precision                                      AS total_bookings_amount,
                 0                                                        AS arr_bookings_units,
                 NULL::text                                               AS lead_source_marketing,
                 NULL::text                                               AS lead_source_most_recent,
                 NULL::text                                               AS first_touch_campaign,
                 NULL::text                                               AS last_touch_campaign,
                 NULL::text                                               AS activity_id,
                 opportunity.contactid                                    AS contact_id,

                 0                                                        AS call_with_sequence,
                 null::text                                               AS call_sequence_name,
                 null::text                                               AS call_disposition,
                 null::text                                               AS task_id,
                 null::text                                               AS ssr,

                 SUM(CASE
                         WHEN opportunity.stagename = 'Closed Won'
                             THEN opportunity.Commissionable_Arr__C::float8 +
                                  opportunity.Expected_Revenue_Services__C::float8
                         ELSE
                             GREATEST(opportunity.expected_revenue_arr__c::float8,
                                      opportunity.commissionable_arr__c::float8) +
                             opportunity.expected_revenue_services__c::float8
                     END)                                                 AS pipeline_amount_created,


                 SUM(CASE
                         WHEN opportunity.stagename = 'Closed Won'
                             THEN opportunity.Commissionable_Arr__C::float8
                         ELSE
                             GREATEST(opportunity.expected_revenue_arr__c::float8,
                                      opportunity.commissionable_arr__c::float8)
                     END)                                                 AS pipeline_arr_created,
--                                COALESCE(SUM(CASE
--                                                 WHEN opportunity.units__c::float8 > 0
--                                                     THEN opportunity.commissionable_arr__c::float8
--                                    END), 0::float8) +
--                                COALESCE(MAX(opportunity.expected_revenue_services__c::float8),
--                                         0::float8)
                 SUM(CASE
                         WHEN opportunity.units__c::float8 > 0 AND opportunity.stagename = 'Closed Won'
                             THEN opportunity.commissionable_arr__c::float8
                         WHEN opportunity.units__c::float8 > 0
                             THEN GREATEST(opportunity.expected_revenue_arr__c::float8,
                                           opportunity.commissionable_arr__c::float8) +
                                  opportunity.expected_revenue_services__c::float8
                         ELSE 0 END)                                      AS pipeline_amount_core_created,
--                                SUM(CASE
--                                        WHEN opportunity.units__c::float8 > 0 THEN opportunity.commissionable_arr__c::float8
--                                    END)
                 SUM(CASE
                         WHEN opportunity.units__c::float8 > 0 AND opportunity.stagename = 'Closed Won'
                             THEN opportunity.commissionable_arr__c::float8
                         WHEN opportunity.units__c::float8 > 0
                             THEN GREATEST(opportunity.expected_revenue_arr__c::float8,
                                           opportunity.commissionable_arr__c::float8)
                         ELSE 0 END)                                      AS pipeline_arr_core_created,
                 COUNT(DISTINCT
                       CASE
                           WHEN (opportunity.units__c::float8 > 0 AND
                                 opportunity.commissionable_arr__c::float8 > 0)
                               THEN opportunity.id
                           END)::int                                      AS pipeline_core_opp_cnt,
                 COUNT(DISTINCT CASE
                                    WHEN GREATEST(opportunity.expected_revenue_arr__c::float8,
                                                  opportunity.commissionable_arr__c::float8) > 0
                                        THEN opportunity.id
                     END)::int                                            AS pipeline_opp_cnt,
                 SUM(opportunity.units__c)::int                           AS pipeline_units
                  ,
                 null::date                                               as first_accred_association_date,
                 null::date                                               as first_accred_required_platform_date,
                 null::integer                                            as is_accred_affiliated,
                 null::boolean                                            as is_partner_affiliated,
                 null::text                                               as accred_affiliation_ps,
                 null::text                                               as demo_source_type,
                 -- end of sfwh data --------------
                 null::timestamp                                          as call_start_time,
                 null::text                                               as contact_title,
                 null::boolean                                            as contact_is_buyer,
                 null::integer                                            as contact_is_primary_buyer,
                 null::integer                                            as contact_is_secondary_buyer


          FROM {{ ref('raw_opportunity') }} opportunity
                   LEFT JOIN {{ ref('raw_account') }} account ON account.id = opportunity.accountid
                   LEFT JOIN {{ ref('raw_user') }} owned_by_user
                             ON opportunity.ownerid = owned_by_user.id
                   left join {{ ref('raw_user') }} um on account.ownerid::text = um.id::text
                   LEFT JOIN {{ ref('raw_user') }} original_opportunity_owner_user
                             ON opportunity.Original_Opportunity_Owner_Role__c =
                                original_opportunity_owner_user.id
                   LEFT JOIN {{ ref('raw_user') }} created_by_user
                             ON opportunity.createdbyid = created_by_user.id
                   LEFT JOIN {{ ref('raw_userrole') }} user_role
                             ON user_role.id = created_by_user.userroleid
                   LEFT JOIN {{ ref('stg_main_core_product_mapping') }} core_product_map
                             ON opportunity.main_product__c = core_product_map.main_core_product__c
                   left join derived_datasets.v_dates dc on opportunity.sqo_date__c = dc.dt
                   left join account_active_history ah2
                             on ah2.account_id = opportunity.accountid
                                 and DATEADD(
                                             day,
                                             -1,
                                             DATE_TRUNC('month', DATEADD(month, -1, opportunity.sqo_date__c))
                                     ) = ah2.month_ending_date

          WHERE opportunity.type IN ('New Business', 'Existing Business', 'Renewal + Addon')
            AND opportunity.sqo_date__c is not null

          GROUP BY opportunity.sqo_date__c, opportunity.accountid, business_division,
                   opportunity.id, opportunity.amount, opportunity.main_product__c, opportunity.name,
                   opportunity.original_opportunity_owner_role__c, opportunity.type,
                   opportunity.contactid, opportunity.hcm_legacy_id__c,
                   opportunity.Original_Appointment_Booked_By_Role__c,
                   opportunity.original_appointment_owner__c,
                   created_by_user.name, user_role.name, owned_by_user.isactive, owned_by_user.name,
                   account.name, ah2.account_id,
                   dc.comp_date, dc.comp_week_start, dc.comp_week_end, dc.current_vs_previous, dc.week_end,
                   dc.week_start, dc.working_day_of_month, dc.working_day_of_quarter, dc.working_day_of_year,
                   account.date_became_customer__c, account.industry, account.total_fte__c, account.shippingstate,
                   account.shippingpostalcode, um.name, um.manager__c, account.istam__c,
                   account.type, account.brand_name_formula__c, account.segmentation_tier__c
     )
    SELECT *
     FROM pipeline_daily