{{ config(materialized='view') }}

SELECT o.closedate::date                                          AS d,
                                 CASE
                                     WHEN o.original_opportunity_owner_role__c ilike '%power%' THEN 'Power'
                                     WHEN o.original_opportunity_owner_role__c ilike '%hcm%' THEN 'HCM'
                                     WHEN a.brand_name_formula__c ilike '%power%' THEN 'Power'
                                     WHEN a.brand_name_formula__c ilike '%hcm%' OR
                                          a.brand_name_formula__c ilike '%neoed%' THEN 'HCM'
                                     WHEN o.original_opportunity_owner_role__c IS NOT NULL OR
                                          a.brand_name_formula__c IS NOT NULL THEN 'All'
                                     ELSE 'Missing'
                                     END::varchar(10)                                       AS business_division,
                                 o.accountid                                                AS account_id,
                                 o.id                                                       AS opportunity_id,
                                 NULL::text                                                 AS appointment_id,
                                 NULL::text                                                 AS demo_source,
                                 o.original_appointment_booked_by_role__c::text             AS original_role,
                                 o.original_opportunity_owner_role__c::text                 AS opportunity_owner_role,
                                 o.type                                                     AS opportunity_type,
                                 o.main_product__c                                          AS opportunity_main_product,
                                 NULL::text                                                 AS closed_won_products,
                                 0                                                          AS leads_new_contacts,
                                 0                                                          AS outbound_calls,
                                 0                                                          AS correct_contact_calls,
                                 0                                                          AS demos_booked,
                                 0                                                          AS sqo_count,
                                 0                                                          AS demos_scheduled,
                                 0                                                          AS demos_completed,
                                 coalesce(original_appointment_owner__c::text, uc.name)     AS booked_by,
                                 uf.name                                                    AS booked_for,
                                 uf.isactive                                                AS booked_for_user_isactive,
                                 o.original_opportunity_owner_role__c                       AS booked_for_role,
                                 coalesce(o.original_appointment_booked_by_role__c, r.name) AS booked_by_role,
                                 NULL::date                                                 AS user_created_date,
                                 NULL::bigint                                               AS demos_booked_to_scheduled_days,
                                 NULL::bigint                                               AS demos_booked_to_scheduled_days_original,
                                 NULL::date                                                 as original_appointment_date,
                                 0                                                          AS opps_closed_won,
                                 count(distinct o.id)                                       AS opps_closed_lost,
                                 uf.name                                                    AS opp_owner,
                                 0                                                          AS arr_bookings_amount,
                                 0                                                          AS total_bookings_amount,
                                 0                                                          AS arr_bookings_units,
                                 NULL::text                                                 AS lead_source_marketing,
                                 NULL::text                                                 AS lead_source_most_recent,
                                 NULL::text                                                 AS first_touch_campaign,
                                 NULL::text                                                 AS last_touch_campaign,
                                 NULL::text                                                 AS activity_type,
                                 NULL::date                                                 AS activity_date,
                                 NULL::text                                                 AS lead_rating,
                                 NULL::text                                                 AS activity_id,
                                 NULL::text                                                 AS subject,
                                 o.contactid                                                AS contact_id,
                                 0                                                          as call_with_sequence,
                                 NULL::text                                                 as call_sequence_name,
                                 null::text                                                 as call_disposition,
                                 null::text                                                 as task_id,
                                 null::text                                                 as ssr,
                                 NULL::float8                                               AS pipeline_amount_created,
                                 NULL::float8                                               AS pipeline_arr_created,
                                 NULL::float8                                               AS pipeline_amount_core_created,
                                 NULL::float8                                               AS pipeline_arr_core_created,
                                 NULL::int                                                  AS pipeline_core_opp_cnt,
                                 NULL::int                                                  AS pipeline_opp_cnt,
                                 NULL::int                                                  AS pipeline_units,
                                 o.hcm_legacy_id__c
                          FROM derived_datasets.opportunity o
                                   LEFT JOIN {{ ref('raw_account') }} a ON a.id = o.accountid
                                   LEFT JOIN {{ ref('raw_user') }} uf
                                             ON o.ownerid = uf.id
                                   INNER JOIN {{ ref('stg_main_core_product_mapping') }} mcp
                                              ON o.main_product__c = mcp.main_core_product__c
                                   LEFT JOIN {{ ref('raw_user') }} uc
                                             ON o.createdbyid = uc.id
                                   left join {{ ref('raw_account') }} r
                                             on r.id = uc.userroleid

                          WHERE 1 = 1
                            AND o.stagename::text = 'Closed Lost'::text
                            AND NOT (
                              o."type"::text IN ('Migration Target', 'Renewal')
                              )
                            AND o.isopportunitysqo__c = true

                            AND (
                              o.closedate >= '2023-01-01'
                                  AND o.saasoptic_grouping_id__c IS NULL
                              )
                            AND (o.SBQQ__AmendedContract__c IS NULL OR
                                 (o.SBQQ__AmendedContract__c IS NOT NULL AND o.units__c > 0))

                            AND coalesce(o.reason_lost__c, 'na') not in
                                ('Renewal - combined w/another opp', 'Moved to Amendment')

                            and coalesce(o.reason_lost_description__c, 'na') not ilike '%duplicate%'

                            and last_stage_before_closed IS not NULL
                          GROUP BY d,
                                   account_id,
                                   opportunity_type,
                                   opp_owner,
                                   original_role,
                                   opportunity_owner_role,
                                   opportunity_id,
                                   uf.name, uf.isactive, uc.name, o.contactid,
                                   original_opportunity_owner_role__c,
                                   original_appointment_booked_by_role__c,
                                   original_appointment_owner__c, o.hcm_legacy_id__c,
                                   a.brand_name_formula__c, r.name,
                                   opportunity_main_product