{{ config(materialized='ephemeral') }}

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
                                coalesce(o.Original_Appointment_Booked_By_Role__c, r.name) AS booked_by_role,
                                NULL::date                                                 AS user_created_date,
                                NULL::bigint                                               AS demos_booked_to_scheduled_days,
                                NULL::bigint                                               AS demos_booked_to_scheduled_days_original,
                                NULL::date                                                 as original_appointment_date,
                                COUNT(
                                        DISTINCT CASE
                                                     WHEN oli_subs.arr_bookings_units > 0 and is_core_product = 1
                                                         THEN oli_subs.opportunity_id
                                                     ELSE NULL
                                    END
                                )                                                          AS opps_closed_won,
                                0                                                          AS opps_closed_lost,
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
                         FROM {{ ref('raw_opportunity') }} o
                                  LEFT JOIN {{ ref('raw_account') }} a ON a.id = o.accountid
                                  LEFT JOIN {{ ref('raw_user') }} uf ON o.ownerid = uf.id
                                  LEFT JOIN derived_datasets.v_bookings_daily_refactored oli_subs
                                            ON o.id = oli_subs.opportunity_id
                                  LEFT JOIN {{ ref('raw_user') }} u
                                            ON o.Original_Opportunity_Owner_Role__c = u.id
                                  LEFT JOIN {{ ref('raw_user') }} uc
                                            ON o.createdbyid = uc.id
                                  LEFT JOIN {{ ref('raw_userrole') }} r
                                            ON r.id = uc.userroleid
                         WHERE 1 = 1
                           AND o.stagename::text = 'Closed Won'::text
                           AND NOT (
                             o."type"::text IN ('Migration Target', 'Renewal')
                             )
                           AND (o.closedate >= '2024-01-01'
                             )
                         GROUP BY d,
                                  uf.name, u.name, r.name, uc.name, uf.isactive,
                                  o.original_opportunity_owner_role__c,
                                  o.original_appointment_booked_by_role__c,
                                  o.original_appointment_owner__c,
                                  o.accountid,
                                  o.ownerid,
                                  o.type, o.hcm_legacy_id__c,
                                  o.id, o.contactid,
                                  a.brand_name_formula__c,
                                  o.main_product__c