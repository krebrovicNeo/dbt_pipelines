{{ config(materialized='table') }}

WITH demos_booked AS (select * from {{ ref('stg_afd_demos_booked') }}),
     demo_progression AS (select * from {{ ref('stg_afd_demo_progression') }}),
     main_core_product_mapping AS (SELECT *
                                   FROM sfdc_silver.mv_merge_opportunity_maincoreproduct_to_productcode opp0 --#TODO: REVIEW - Check this mappings to most current core products
                                   WHERE opp0.mapped_main_core_product_value <> 'unmapped'
                                     AND opp0.mapped_main_core_product_value not in ('GJ', 'PowerStandards')),
     bookings AS (select * from {{ ref('stg_afd_bookings') }}),
     closed_won_opps AS (select * from {{ ref('stg_afd_closed_won_opps') }}),
     closed_lost_opps AS (select * from {{ ref('stg_afd_closed_lost_opps') }}),
     correct_calls AS (select * from {{ ref('stg_afd_correct_calls') }}),

     account_active_history as
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

     pipeline_daily as
         (select * from  {{ref('stg_afd_pipeline_daily') }} ),
     unioned_cte AS (SELECT *
                     FROM demos_booked
                     UNION ALL
                     SELECT *
                     FROM demo_progression
                     UNION ALL
                     SELECT *
                     FROM bookings
                     UNION ALL
                     SELECT *
                     FROM closed_won_opps
                     UNION ALL
                     SELECT *
                     FROM closed_lost_opps
                     UNION ALL
                     SELECT *
                     FROM correct_calls),
     aggregated_cte AS (SELECT reporting_date,
                               business_division::varchar(10)               AS business_division,
                               account_id                                   AS account_id,
                               opportunity_id                               AS opportunity_id,
                               appointment_id,
                               demo_source::text                            AS demo_source,
                               original_role,
                               opportunity_owner_role,
                               opportunity_type,
                               opportunity_main_product,
                               f_uniquify(
                                       pg_catalog.listagg(
                                               closed_won_products::text,
                                               ','::text::text
                                       )
                               )                                            AS closed_won_products,
                               SUM(leads_new_contacts)                      AS leads_new_contacts,
                               SUM(outbound_calls)                          AS outbound_calls,
                               SUM(correct_contact_calls)                   AS correct_contact_calls,
--                                SUM(correct_contact_conversation_calls) AS correct_contact_conversation,
                               SUM(demos_booked)                            AS demos_booked,
                               SUM(sqo_count)                               AS sqo_count,
                               SUM(demos_scheduled)                         AS demos_scheduled,
                               SUM(demos_completed)                         AS demos_completed,
                               booked_by,
                               booked_for,
                               booked_for_user_isactive,
                               booked_for_role,
                               booked_by_role,
                               user_created_date,
                               avg(demos_booked_to_scheduled_days)          AS demos_booked_to_scheduled_days,
                               avg(demos_booked_to_scheduled_days_original) AS demos_booked_to_scheduled_days_original,
                               original_appointment_date,
                               SUM(opps_closed_won)                         AS opps_closed_won,
                               SUM(opps_closed_lost)                        AS opps_closed_lost,
                               opp_owner,
                               SUM(arr_bookings_amount)                     AS arr_bookings_amount,
                               SUM(total_bookings_amount)                   AS total_bookings_amount,
                               SUM(arr_bookings_units)                      AS arr_bookings_units,
                               lead_source_marketing,
                               lead_source_most_recent,
                               first_touch_campaign,
                               last_touch_campaign,
                               activity_type,
                               activity_date,
                               lead_rating,
                               activity_id,
                               subject,
                               contact_id,
                               call_with_sequence,
                               call_sequence_name,
                               call_disposition,
                               task_id,
                               ssr,
                               SUM(pipeline_amount_created)                 AS pipeline_amount_created,
                               SUM(pipeline_arr_created)                    AS pipeline_arr_created,
                               SUM(pipeline_amount_core_created)            AS pipeline_amount_core_created,
                               SUM(pipeline_arr_core_created)               AS pipeline_arr_core_created,
                               COUNT(pipeline_core_opp_cnt)                 AS pipeline_core_opp_cnt,
                               COUNT(pipeline_opp_cnt)                      AS pipeline_opp_cnt,
                               SUM(pipeline_units)                          AS pipeline_units,
                               hcm_legacy_id__c,
                               CASE
                                   WHEN reporting_date > max(
                                                         CASE
                                                             WHEN sqo_count > 0 THEN reporting_date
                                                             ELSE '1900-01-01'::date
                                                             END
                                                            ) OVER (PARTITION BY account_id) --sqo_date
                                       AND opps_closed_won > 0 then max(
                                                                    CASE
                                                                        WHEN sqo_count > 0
                                                                            THEN coalesce(last_touch_campaign, first_touch_campaign)
                                                                        ELSE NULL
                                                                        END
                                                                       ) OVER (PARTITION BY account_id)
                                   ELSE NULL
                                   END                                      AS booking_first_touch,
                               CASE
                                   WHEN reporting_date > max(
                                                         CASE
                                                             WHEN sqo_count > 0 THEN reporting_date
                                                             ELSE '1900-01-01'::date
                                                             END
                                                            ) OVER (PARTITION BY account_id) --sqo_date
                                       AND activity_id IS NOT NULL
                                       AND opps_closed_won > 0 THEN max(
                                                                    CASE
                                                                        WHEN sqo_count > 0
                                                                            THEN coalesce(last_touch_campaign, first_touch_campaign)
                                                                        ELSE NULL
                                                                        END
                                                                       ) OVER (PARTITION BY account_id)
                                   ELSE NULL
                                   END                                      AS booking_last_touch_with_activity,
                               SUM(sqo_count) OVER (
                                   PARTITION BY account_id
                                   ORDER BY reporting_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                   )                                        AS sqo_group
                        FROM unioned_cte
                        GROUP BY reporting_date,
                                 business_division::varchar(10),
                                 account_id,
                                 demo_source,
                                 original_role,
                                 opportunity_owner_role,
                                 booked_by,
                                 booked_for,
                                 booked_for_user_isactive,
                                 booked_for_role,
                                 booked_by_role,
                                 user_created_date,
                                 opp_owner,
                                 opportunity_type,
                                 opportunity_main_product,
                                 lead_source_marketing,
                                 lead_source_most_recent,
                                 original_appointment_date,
                                 first_touch_campaign,
                                 last_touch_campaign,
                                 activity_type,
                                 activity_date,
                                 lead_rating,
                                 activity_id,
                                 contact_id,
                                 call_with_sequence,
                                 call_sequence_name,
                                 call_disposition,
                                 task_id,
                                 ssr, hcm_legacy_id__c,
                                 subject,
                                 sqo_count,
                                 opps_closed_won,
                                 opportunity_id,
                                 appointment_id),
     joined_accounts AS (SELECT reporting_date,
                                business_division::varchar(10),
                             /* account fields */
                                a.id                              AS account_id,
                                opportunity_id,
                                appointment_id,
                                a.name                            AS account_name,
                                a.date_became_customer__c         AS became_customer_date,
                                a.industry                        AS market_segment,
                                a.total_fte__c                    AS agency_ftes,
                                NULL::int                         AS account_fiscal_month,
                                a.shippingstate                   AS state,
                                a.shippingpostalcode              AS postal_code,
                                u.name                            AS account_owner,
                                u.manager__c                      as account_owner_manager,
                                a.istam__c                        AS account_is_tam,
                                a.type                            AS account_type,
                                a.brand_name_formula__c           AS brand_name,
                                a.segmentation_tier__c            AS segmentation_tier,
                             /* aggregated fields */
                                demo_source,
                                original_role,
                                opportunity_owner_role,
                                opportunity_type,
                                opportunity_main_product,
                                closed_won_products,
                                leads_new_contacts,
                                outbound_calls,
                                correct_contact_calls,
--                                 correct_contact_conversation,
                                CASE
                                    WHEN a.createddate::date = reporting_date
                                        AND a.isdeleted = false THEN 1
                                    ELSE 0
                                    END                           AS accounts_created,
                                demos_booked,
                                NULL::bigint                      AS pr_demos_booked,
                                NULL::bigint                      AS pr_demos_scheduled,
                                NULL::bigint                      AS pr_demos_completed,
                                sqo_count,
                                demos_booked_to_scheduled_days,
                                demos_booked_to_scheduled_days_original,
                                original_appointment_date,
                                demos_completed,
                                booked_by,
                                booked_for,
                                booked_for_user_isactive,
                                booked_for_role,
                                booked_by_role,
                                user_created_date,
                                NULL::timestamp without time zone AS booked_for_user_created_date,
                                demos_scheduled,
                                opps_closed_won,
                                opps_closed_lost,
                                opp_owner,
                                NULL                              AS opp_contact,
                                NULL                              AS opp_contact_role,
                                arr_bookings_amount,
                                total_bookings_amount,
                                arr_bookings_units,
                                lead_source_marketing,
                                lead_source_most_recent,
                                first_touch_campaign,
                                last_touch_campaign,
                                activity_id,
                                contact_id,
                                call_with_sequence,
                                call_sequence_name,
                                call_disposition,
                                task_id,
                                ssr,
                             /* Pipeline Daily */
                                pipeline_amount_created,
                                pipeline_arr_created,
                                pipeline_amount_core_created,
                                pipeline_arr_core_created,
                                pipeline_core_opp_cnt,
                                pipeline_opp_cnt,
                                pipeline_units,
                             /* Partner attributes */
                                x.first_accred_association_date,
                                x.first_accred_required_platform_date,
                                x.is_accred_affiliated,
                                x.is_partner_affiliated,
                                x.accred_affiliation_ps,
                                agg.hcm_legacy_id__c
                         FROM aggregated_cte agg
                                  LEFT JOIN {{ ref('raw_account') }} a ON a.id::text = agg.account_id::text
                                  LEFT JOIN derived_datasets_merge.v_pdms_account_partnership_attributes_merge x
                                            ON a.id = x.account_id
                                  LEFT OUTER JOIN {{ ref('raw_user') }} u ON a.ownerid = u.id),
     afd_history AS (SELECT reporting_week_end,
                            reporting_week_start,
                            working_day_of_month,
                            working_day_of_quarter,
                            working_day_of_year,
                            business_type,
                            reporting_date,
                            CASE
                                WHEN h.business_division = 'PDMS' THEN 'Power'
                                ELSE h.business_division
                                END::varchar(10)                        AS business_division,
                            isnull(am.account_id, h.account_id)         as account_id,
                            isnull(om.opportunity_id, h.opportunity_id) as opportunity_id,
                            appointment_id,
                            account_name,
                            became_customer_date,
                            market_segment,
                            agency_ftes,
                            account_fiscal_month,
                            state,
                            postal_code,
                            account_owner,
                            null                                        as account_owner_manager,
                            account_is_tam,
                            account_type,
                            NULL::varchar(1000)                         as brand,
                            segmentation_tier,
                            demo_source,
                            original_role,
                            opportunity_owner_role,
                            opportunity_type,
                            opportunity_main_product,
                            closed_won_products,
                            leads_new_contacts,
                            outbound_calls,
                            correct_contact_calls,
--                             correct_contact_conversation,
                            accounts_created,
                            demos_booked,
                            pr_demos_booked,
                            pr_demos_scheduled,
                            pr_demos_completed,
                            sqo_count,
                            demos_booked_to_scheduled_days,
                            NULL::bigint                                AS demos_booked_to_scheduled_days_original,
                            original_appointment_date,
                            demos_completed,
                            booked_by,
                            booked_for,
                            NULL::boolean                               AS booked_for_user_isactive,
                            booked_for_role,
                            original_role                               AS booked_by_role,
                            user_created_date,
                            booked_for_user_created_date,
                            demos_scheduled,
                            opps_closed_won,
                            opps_closed_lost,
                            opp_owner,
                            opp_contact,
                            opp_contact_role,
                            arr_bookings_amount,
                            total_bookings_amount,
                            arr_bookings_units,
                            lead_source_marketing,
                            lead_source_most_recent,
                            first_touch_campaign,
                            last_touch_campaign,
                            activity_id,
                            contact_id,
                            call_with_sequence,
                            call_sequence_name,
                            call_disposition,
                            task_id,
                            null::text                                  as ssr,
                            NULL::float8                                AS pipeline_amount_created,
                            NULL::float8                                AS pipeline_arr_created,
                            NULL::float8                                AS pipeline_amount_core_created,
                            NULL::float8                                AS pipeline_arr_core_created,
                            NULL::int                                   AS pipeline_core_opp_cnt,
                            NULL::int                                   AS pipeline_opp_cnt,
                            NULL::int                                   AS pipeline_units,
                            first_accred_association_date,
                            first_accred_required_platform_date,
                            is_accred_affiliated,
                            is_partner_affiliated,
                            accred_affiliation_ps,
                            demo_source_type,
                            null::text                                  as hcm_legacy_id__c
                     FROM sfdc_silver.account_funnel_daily_hist_20251022 h
                              -- sfdc_silver.account_funnel_daily_hist_202500630 h
                              left join derived_datasets.v_account_mapping am
                                        on am.hcm_legacy_id__c = h.account_id
                              left join derived_datasets.v_opportunity_mapping om
                                        on om.hcm_legacy_id__c = h.opportunity_id),
     sales_funnel_base as
         (SELECT dc.week_end               AS reporting_week_end,
                 dc.week_start             AS reporting_week_start,
                 dc.working_day_of_month,
                 dc.working_day_of_quarter,
                 dc.working_day_of_year,
                 CASE
                     WHEN joined_accounts.became_customer_date < reporting_date THEN 'Existing'
                     ELSE 'New'
                     END                   AS business_type,
                 joined_accounts.*,
                 m.mapped_demosource_value AS demo_source_type
          FROM joined_accounts
                   LEFT JOIN derived_datasets.v_dates dc ON reporting_date = dc.dt
                   LEFT JOIN mappings.combined_events_demosource_to_inboundoutbound m -- #TODO: REVIEW - Need to point to the new source
                             ON joined_accounts.demo_source = m.demo_source__c -- where account_name <> 'NEOGOV (CA)'
          WHERE joined_accounts.reporting_date >= '2023-01-01')
        ,
     sales_funnel_with_history AS (


         -- take all measures >= 2024/10
         SELECT *
         from sales_funnel_base base
         WHERE base.reporting_date >= '2024-10-01'


         UNION ALL

         -- same thing for history, take all measures before 2024/01
         SELECT *
         FROM afd_history hist
         WHERE hist.reporting_date < '2023-01-01'


         UNION ALL

         -- between 2024/01 and 2024/10 take closed lost without legacy id (migrated Power->Hcm + Power)
         SELECT *
         from sales_funnel_base base
         WHERE base.reporting_date between '2023-01-01' and '2024-09-30'
           and opps_closed_lost >= 1
           and hcm_legacy_id__c is null


         UNION ALL

         -- bring hcm from history between 2024/01 and 2024/10
         SELECT *
         FROM afd_history hist
         WHERE hist.reporting_date between '2023-01-01' and '2024-09-30'
           and opps_closed_lost >= 1
           and business_division = 'HCM'


         union all


         -- everything not closed lost between 2024/01 and 2024/09
         SELECT *
         FROM afd_history hist
         WHERE hist.reporting_date between '2023-01-01' and '2024-09-30'
           and opps_closed_lost = 0),
     contact_is_buyer as (select contact__c,
                                 case
                                     when sum(case when decision_maker_status__c = 'Primary' then 1 else 0 end) > 0
                                         then 1
                                     else 0 end contact_is_primary_buyer,
                                 case
                                     when sum(case when decision_maker_status__c = 'Secondary' then 1 else 0 end) > 0
                                         then 1
                                     else 0 end contact_is_secondary_buyer
                          from sfdc_silver.mv_buyer_role
                          where 1 = 1
                            and account__c is not null
                          group by contact__c)

SELECT
    /* Creating an 16 digit index for the entire dataset */
    LPAD(CAST(ROW_NUMBER() OVER () AS VARCHAR), 9, '0') ||
    SUBSTRING(MD5(RANDOM()::VARCHAR), 0, 7)                          AS sales_funnel_daily_id,
    dc.comp_date,
    dc.comp_week_start,
    dc.comp_week_end,
    dc.current_vs_previous,
    reporting_date,
    -- sfwh data ---------------------


    sfwh.reporting_week_end,
    sfwh.reporting_week_start,
    sfwh.working_day_of_month,
    sfwh.working_day_of_quarter,
    sfwh.working_day_of_year,
    sfwh.business_division::varchar(10),
    /* account fields */

    sfwh.account_id,
    sfwh.opportunity_id,
    sfwh.appointment_id,
    sfwh.account_name,
    coalesce(acc.date_became_customer__c, sfwh.became_customer_date) as became_customer_date,
    CASE
        WHEN sfwh.became_customer_date < DATEADD('day', -31, reporting_date) THEN 'Existing' -- add 31-day window
        ELSE 'New'
        END                                                          as business_type,

    CASE
        WHEN ah2.account_id is not null THEN 'Existing'
        ELSE 'New'
        END                                                          AS derived_business_type,

    coalesce(acc.industry, sfwh.market_segment)                      as market_segment,
    coalesce(acc.total_fte__c, sfwh.agency_ftes)                     as agency_ftes,
    sfwh.account_fiscal_month,
    coalesce(acc.shippingstate, sfwh.state)                          as state,
    coalesce(acc.shippingpostalcode, sfwh.postal_code)               as postal_code,
    coalesce(um.name, sfwh.account_owner)                            as account_owner,
    coalesce(um.manager__c, sfwh.account_owner_manager)              as account_owner_manager,
    coalesce(acc.istam__c, sfwh.account_is_tam)                      as account_is_tam,
    coalesce(acc.type, sfwh.account_type)                            as account_type,
    coalesce(acc.brand_name_formula__c, sfwh.brand_name)             as brand_name,
    coalesce(acc.segmentation_tier__c, sfwh.segmentation_tier)       as segmentation_tier,
    /* aggregated fields */

    sfwh.demo_source,
    sfwh.original_role,
    sfwh.opportunity_owner_role,
    coalesce(opp.type, sfwh.opportunity_type)                        as opportunity_type,
    coalesce(opp.main_product__c, sfwh.opportunity_main_product)     as opportunity_main_product,
    sfwh.closed_won_products,
    sfwh.leads_new_contacts,
    sfwh.outbound_calls,
    sfwh.correct_contact_calls,
    sfwh.accounts_created,
    sfwh.demos_booked,
    sfwh.pr_demos_booked,
    sfwh.pr_demos_scheduled,
    sfwh.pr_demos_completed,
    sfwh.sqo_count,
    sfwh.demos_booked_to_scheduled_days,
    sfwh.demos_booked_to_scheduled_days_original,
    sfwh.original_appointment_date,
    sfwh.demos_completed,
    sfwh.booked_by,
    sfwh.booked_for,
    sfwh.booked_for_user_isactive,
    sfwh.booked_for_role,
    sfwh.booked_by_role,
    sfwh.user_created_date,
    sfwh.booked_for_user_created_date,

    sfwh.demos_scheduled,
    sfwh.opps_closed_won,
    sfwh.opps_closed_lost,
    sfwh.opp_owner,
    sfwh.opp_contact,
    sfwh.opp_contact_role,
    sfwh.arr_bookings_amount,
    sfwh.total_bookings_amount,
    sfwh.arr_bookings_units,
    sfwh.lead_source_marketing,
    sfwh.lead_source_most_recent,
    sfwh.first_touch_campaign,
    sfwh.last_touch_campaign,
    sfwh.activity_id,
    sfwh.contact_id,
    sfwh.call_with_sequence,
    sfwh.call_sequence_name,
    sfwh.call_disposition,
    sfwh.task_id,
    sfwh.ssr,
    sfwh.pipeline_amount_created,
    sfwh.pipeline_arr_created,
    sfwh.pipeline_amount_core_created,
    sfwh.pipeline_arr_core_created,
    sfwh.pipeline_core_opp_cnt,
    sfwh.pipeline_opp_cnt,
    sfwh.pipeline_units,

    sfwh.first_accred_association_date,
    sfwh.first_accred_required_platform_date,
    sfwh.is_accred_affiliated,
    sfwh.is_partner_affiliated,
    sfwh.accred_affiliation_ps,
    sfwh.demo_source_type,
    -- end of sfwh data --------------
    tm.call_start_time__c                                            as call_start_time,
    cm.title                                                         as contact_title,
    isnull(cm.contact_has_buyer__c, false)                           as contact_is_buyer,
    isnull(c.contact_is_primary_buyer, 0)                            as contact_is_primary_buyer,
    isnull(c.contact_is_secondary_buyer, 0)                          as contact_is_secondary_buyer,
    opp.last_stage_before_closed


FROM sales_funnel_with_history sfwh
         left join {{ ref('raw_account') }} acc on acc.id = sfwh.account_id
         left join {{ ref('raw_user') }} um on acc.ownerid::text = um.id::text
         left join derived_datasets.opportunity opp on opp.id = sfwh.opportunity_id
         left join derived_datasets.v_dates dc on reporting_date = dc.dt
         left join {{ ref('raw_contact') }} cm on cm.id = sfwh.contact_id
         left join {{ ref('raw_task') }} tm on tm.id = sfwh.task_id
         left join contact_is_buyer c on c.contact__c = sfwh.contact_id
         left join account_active_history ah2
                   on ah2.account_id = sfwh.account_id
                       and DATEADD(
                                   day,
                                   -1,
                                   DATE_TRUNC('month', DATEADD(month, -1, sfwh.reporting_date))
                           ) = ah2.month_ending_date

union all

select pd.*,
       null::text as last_stage_before_closed
from pipeline_daily pd