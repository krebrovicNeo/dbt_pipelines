{{ config(materialized='view') }}

SELECT t.activitydate::date                   AS reporting_date,
                              CASE
                                  WHEN Original_Lead_Gen_Role__c ILIKE '%Customer Success Manager%' THEN 'HCM'
                                  WHEN Original_Lead_Gen_Role__c ILIKE '%HCM%' THEN 'HCM'
                                  WHEN Original_Lead_Gen_Role__c ILIKE '%Power%' THEN 'Power'
                                  ELSE a.brand_name_formula__c
                                  END::varchar(10)                   AS business_division,
                              t.accountid                            AS account_id,
                              NULL::text                             AS opportunity_id,
                              NULL::text                             AS appointment_id,
                              NULL::text                             AS demo_source,
                              t.original_lead_gen_role__c::text      AS original_role,
                              NULL::text                             AS opportunity_owner_role,
                              NULL::text                             AS opportunity_type,
                              NULL::text                             AS opportunity_main_product,
                              NULL::text                             AS closed_won_products,
                              0                                      AS leads_new_contacts,
                              COUNT(DISTINCT t.id)                   AS outbound_calls,
                              COUNT(
                                      DISTINCT CASE
--                                                    WHEN (t.calldisposition = 'Correct Contact') THEN t.id
                                                   WHEN (correct_contact__c = true) THEN t.id
                                                   ELSE NULL
                                  END
                              )                                      AS correct_contact_calls,
--                               COUNT(
--                                       DISTINCT CASE
--                                                    WHEN (
-- --                                                                t.calldisposition = 'Correct Contact'
--                                                             correct_contact__c = true
--                                                            AND t.calldurationinseconds >= 60
--                                                        ) THEN t.id
--                                                    ELSE NULL
--                                   END
--                                   )                             AS correct_contact_conversation_calls,
                              0                                      AS demos_booked,
                              0                                      AS sqo_count,
                              0                                      AS demos_scheduled,
                              0                                      AS demos_completed,
                              uf.name                                AS booked_by,
                              NULL::text                             AS booked_for,
                              NULL::boolean                          AS booked_for_user_isactive,
                              NULL::text                             AS booked_for_role,
                              t.original_lead_gen_role__c::text      AS booked_by_role,
                              NULL::date                             AS user_created_date,
                              NULL::bigint                           AS demos_booked_to_scheduled_days,
                              NULL::bigint                           AS demos_booked_to_scheduled_days_original,
                              NULL::date                             as original_appointment_date,
                              0                                      AS opps_closed_won,
                              0                                      AS opps_closed_lost,
                              NULL::text                             AS opp_owner,
                              0                                      AS arr_bookings_amount,
                              0                                      AS total_bookings_amount,
                              0                                      AS arr_bookings_units,
                              NULL::text                             AS lead_source_marketing,
                              NULL::text                             AS lead_source_most_recent,
                              NULL::text                             AS first_touch_campaign,
                              NULL::text                             AS last_touch_campaign,
                              NULL::text                             AS activity_type,
                              NULL::date                             AS activity_date,
                              NULL::text                             AS lead_rating,
                              NULL::text                             AS activity_id,
                              NULL::text                             AS subject,
                              t.whoid                                AS contact_id,
                              case
                                  when sequence_id__c is not null and sequence_id__c <> '' then 1
                                  else 0 end                         as call_with_sequence,
                              t.outreach_attributed_sequence_name__c as call_sequence_name,
                              t.calldisposition                      as call_disposition,
                              t.id                                   as task_id,
                              null::text                             as ssr,
                              NULL::float8                           AS pipeline_amount_created,
                              NULL::float8                           AS pipeline_arr_created,
                              NULL::float8                           AS pipeline_amount_core_created,
                              NULL::float8                           AS pipeline_arr_core_created,
                              NULL::int                              AS pipeline_core_opp_cnt,
                              NULL::int                              AS pipeline_opp_cnt,
                              NULL::int                              AS pipeline_units,
                              null::text                             as hcm_legacy_id__c
                       FROM {{ ref('raw_task') }} t
                                LEFT JOIN {{ ref('raw_account') }} a
                                          ON a.id = t.accountid
                                LEFT JOIN {{ ref('raw_user') }} uf
                                          ON t.ownerid = uf.id
                                LEFT JOIN {{ ref('raw_userrole') }} r ON r.id = uf.userroleid
                       WHERE 1 = 1
                         AND (t.subject ilike '%call:%' or t.calltype = 'Outbound')
                         AND t.activitydate IS NOT NULL
                         AND t.activitydate >= '2024-01-01'
                       GROUP BY reporting_date,
                                account_id,
                                original_role,
                                uf.name, t.whoid, sequence_id__c, t.id,
                                t.original_lead_gen_role__c, t.calldisposition,
                                a.brand_name_formula__c, t.outreach_attributed_sequence_name__c,
                                r.name