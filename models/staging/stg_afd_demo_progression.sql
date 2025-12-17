{{ config(materialized='view') }}

SELECT a.appointment_date__c::date                AS d,
                                 CASE
                                     WHEN o.original_opportunity_owner_role__c ilike '%power%' THEN 'Power'
                                     WHEN o.original_opportunity_owner_role__c ilike '%hcm%' THEN 'HCM'
                                     WHEN acc.brand_name_formula__c ilike '%power%' THEN 'Power'
                                     WHEN (acc.brand_name_formula__c ilike '%hcm%' OR
                                           acc.brand_name_formula__c ilike '%neoed%') THEN 'HCM'
                                     WHEN o.original_opportunity_owner_role__c IS NOT NULL OR
                                          acc.brand_name_formula__c IS NOT NULL THEN 'All'
                                     ELSE 'Missing'
                                     END::varchar(10)                       AS business_division,
                                 a.account__c                               AS account_id,
                                 a.opportunity__c                           AS opportunity_id,
                                 a.id                                       AS appointment_id,
                                 a.source__c                                AS demo_source,
                                 a.Original_Booked_By_Role__c::text         AS original_role,
                                 o.original_opportunity_owner_role__c::text AS opportunity_owner_role,
                                 NULL::text                                 AS opportunity_type,
                                 NULL::text                                 AS opportunity_main_product,
                                 NULL::text                                 AS closed_won_products,
                                 0                                          AS leads_new_contacts,
                                 0                                          AS outbound_calls,
                                 0                                          AS correct_contact_calls,
--                                  0                                              AS correct_contact_conversation_calls,
                                 0                                          AS demos_booked,
                              /* SQOs */
                                 COUNT(
                                         DISTINCT CASE
                                                      WHEN a.appointment_date__c IS NOT NULL
                                                          AND a.status__c = 'Completed'
                                                          AND a.issqo__c = true THEN a.id
                                                      ELSE NULL
                                     END
                                 )                                          AS sqo_count,
                              /* DEMOS SCHEDULED */
                                 COUNT(
                                         DISTINCT CASE
                                                      WHEN a.appointment_date__c IS NOT NULL THEN a.id
                                                      else null
                                     END
                                 )                                          AS demo_scheduled,
                              /* DEMOS COMPLETED */
                                 COUNT(
                                         DISTINCT CASE
                                                      WHEN a.appointment_date__c IS NOT NULL
                                                          AND a.status__c = 'Completed' THEN a.id
                                                      ELSE NULL
                                     END
                                 )                                          AS demos_completed,
                                 u.name                                     AS booked_by,
                                 uf.name                                    AS booked_for,
                                 uf.isactive                                AS booked_for_user_isactive,
                                 a.booked_for_role__c                       AS booked_for_role,
                                 a.original_booked_by_role__c               AS booked_by_role,
                                 u.createddate                              AS user_created_date,
                                 SUM(
                                         date_diff('day', a.createddate, a.appointment_date__c)
                                 )                                          AS demos_booked_to_scheduled_days,
                                 SUM(
                                         date_diff('day', a.createddate, a.original_appointment_date__c)
                                 )                                          AS demos_booked_to_scheduled_days_original,
                                 a.original_appointment_date__c             as original_appointment_date,
                                 0                                          AS opps_closed_won,
                                 0                                          AS opps_closed_lost,
                                 NULL::text                                 AS opp_owner,
                                 0                                          AS arr_bookings_amount,
                                 0                                          AS total_bookings_amount,
                                 0                                          AS arr_bookings_units,
                                 NULL::text                                 AS lead_source_marketing,
                                 NULL::text                                 AS lead_source_most_recent,
                                 NULL::text                                 AS first_touch_campaign,
                                 NULL::text                                 AS last_touch_campaign,
                                 NULL::text                                 AS activity_type,
                                 NULL::date                                 AS activity_date,
                                 NULL::text                                 AS lead_rating,
                                 NULL::text                                 AS activity_id,
                                 NULL::text                                 AS subject,
                                 o.contactid                                AS contact_id,
                                 0                                          as call_with_sequence,
                                 NULL::text                                 as call_sequence_name,
                                 null::text                                 as call_disposition,
                                 null::text                                 as task_id,
                                 a.ssr__c                                   as ssr,
                                 NULL::float8                               AS pipeline_amount_created,
                                 NULL::float8                               AS pipeline_arr_created,
                                 NULL::float8                               AS pipeline_amount_core_created,
                                 NULL::float8                               AS pipeline_arr_core_created,
                                 NULL::int                                  AS pipeline_core_opp_cnt,
                                 NULL::int                                  AS pipeline_opp_cnt,
                                 NULL::int                                  AS pipeline_units,
                                 o.hcm_legacy_id__c
                          FROM {{ ref('raw_appointment') }} a
                                   LEFT JOIN {{ ref('raw_account') }} acc ON acc.id = a.account_id__c
                                   LEFT JOIN {{ ref('raw_opportunity') }} o ON o.id = a.opportunity__c
                                   LEFT JOIN {{ ref('raw_user') }} u ON a.ownerid::text = u.id::text
                                   LEFT JOIN {{ ref('raw_user') }} uf ON a.booked_for__c = uf.id
                          WHERE 1 = 1
                            AND (
                              o.isdeleted = false
                                  or isnull(o.isdeleted)
                              )
                            AND (a.appointment_date__c >= '2024-01-01' AND o.saasoptic_grouping_id__c IS NULL)
                            AND a.core_product_appointment__c = 'true'
                            AND a.isdeleted = 'false'
                          GROUP BY d,
                                   account_id,
                                   demo_source,
                                   original_role,
                                   opportunity_owner_role,
                                   booked_by,
                                   user_created_date,
                                   opportunity_id,
                                   appointment_id,
                                   a.issqo__c,
                                   u.name, o.contactid,
                                   uf.name, uf.isactive,
                                   booked_by_role,
                                   booked_for_role,
                                   o.original_opportunity_owner_role__c,
                                   a.original_appointment_date__c,
                                   acc.brand_name_formula__c,
                                   opportunity_main_product,
                                   o.hcm_legacy_id__c,
                                   a.ssr__c