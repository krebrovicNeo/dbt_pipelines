{{ config(materialized='view') }}

SELECT b.reporting_date::date                                     AS d,
                         b.business_division::varchar(10)                           AS business_division,
                         coalesce(o.accountid, b.account_id)                        AS account_id,
                         coalesce(o.id, b.opportunity_id)                           AS opportunity_id,
                         NULL::text                                                 AS appointment_id,
                         NULL::text                                                 AS demo_source,
                         o.original_appointment_booked_by_role__c::text             AS original_role,
                         o.original_opportunity_owner_role__c::text                 AS opportunity_owner_role,
                         coalesce(o.type, b.opportunity_type)                       AS opportunity_type,
                         o.main_product__c                                          AS opportunity_main_product,
                         NULL::text                                                 AS closed_won_products,
                         0                                                          AS leads_new_contacts,
                         0                                                          AS outbound_calls,
                         0                                                          AS correct_contact_calls,
                         0                                                          AS demos_booked,
                         0                                                          AS sqo_count,
                         0                                                          AS demos_scheduled,
                         0                                                          AS demos_completed,
                         coalesce(original_appointment_owner__c::text, u.name)      AS booked_by,
                         uf.name                                                    AS booked_for, 
                         uf.isactive                                                AS booked_for_active,
                         o.original_opportunity_owner_role__c                       AS booked_for_role,
                         coalesce(o.Original_Appointment_Booked_By_Role__c, r.name) AS booked_by_role,
                         NULL::date                                                 AS user_created_date,
                         NULL::bigint                                               AS demos_booked_to_scheduled_days,
                         NULL::bigint                                               AS demos_booked_to_scheduled_days_original,
                         NULL::date                                                 as original_appointment_date,
                         0                                                          AS opps_closed_won,
                         0                                                          AS opps_closed_lost,
                         b.owner_name                                               AS opp_owner,
                         b.arr_bookings_amount::double precision                    AS arr_bookings_amount,
                         b.total_bookings_amount::double precision                  AS total_bookings_amount,
                         CASE
                             WHEN b.is_core_product = 1 THEN
                                 b.arr_bookings_units
                             ELSE 0 END                                             AS arr_bookings_units, 
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
                  FROM derived_datasets.bookings_daily b
                           LEFT JOIN {{ref('raw_opportunity')}} o
                                     ON b.opportunity_id = o.id
                           LEFT JOIN {{ref('raw_user')}} u ON o.createdbyid = u.id
                           LEFT JOIN {{ref('raw_userrole')}} r
                                     ON u.userroleid = r.id
                           LEFT JOIN {{ref('raw_user')}} uf ON o.ownerid = uf.id