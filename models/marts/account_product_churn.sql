{{ config(materialized='table') }}

WITH incentives_per_quote AS
         (
          SELECT *
          from  {{ ref('stg_apc_incentive_per_quote') }}
          ) ,
     product_subscriptions AS
         (
          select *
          from {{ ref('stg_apc_product_subscriptions') }}
         ),
     first_product_subscriptions AS
         (
        select *
          from {{ ref('stg_apc_first_product_subscriptions') }}
          ),
     churn_sheet_pdms AS (select *
                          from {{ ref('stg_apc_churn_sheet_pdms') }}
                          ),
     churn_sheet_pdms_dedup as (select change_month_ending,
                                       account_name,
                                       account_id,
                                       account_became_customer_date,
                                       product_code,
                                       subscription_item_name,
                                       subscription_start_date,
                                       subscription_end_date,
                                       change_type,
                                       incentive_type,
                                       sum(subscription_arr)::int as subscription_arr
                                        ,
                                       account_state,
                                       account_postal_code,
                                       total_fte,
                                       territory,
                                       industry,
                                       churn_description_power,
                                       cancellation_description_power,
                                       first_subscription_start_date,
                                       closed_month_flag,
                                       last_refresh_date
                                from churn_sheet_pdms p
                                where p.idx = 1
                                group by change_month_ending,
                                         account_name,
                                         account_id,
                                         account_became_customer_date,
                                         product_code,
                                         subscription_item_name,
                                         subscription_start_date,
                                         subscription_end_date,
                                         change_type,
                                         incentive_type,
                                         account_state,
                                         account_postal_code,
                                         total_fte,
                                         territory,
                                         industry,
                                         churn_description_power,
                                         cancellation_description_power,
                                         first_subscription_start_date,
                                         closed_month_flag,
                                         last_refresh_date),
     account_hcm AS (SELECT x.*
                     FROM (SELECT DISTINCT a.name
                                         , a.id
                                         , a.date_became_customer__c
                                         , a.shippingstate
                                         , a.shippingpostalcode
                                         , a.total_employees__c
                                         , a.territory2__c
                                         , a.industry
                                         , a.parentid
                                         , a.brand_name_formula__c
                                         , a.hcm_legacy_id__c
                                         , ROW_NUMBER() OVER (partition by name order by a.hcm_legacy_id__c) as index
                           FROM  {{ ref('account_raw') }} a
                           WHERE a.hcm_legacy_id__c IS NOT NULL) x
                     WHERE x.index = 1),
     churn_sheet_hcm AS (select *
                         from {{ ref('stg_apc_churn_sheet_hcm') }}
         --  where account_id = '001Uh00000EOBT4IAP'
         -- and subscription_end_date = '2024-10-31'
     ),
     churn_sheet_hcm_dedup as
         (select change_month_ending,
                 saasoptics_customer_name,
                 account_name,
                 account_id,
                 hcm_legacy_id,
                 account_became_customer_date,
                 product_code,
                 item_code,
                 subscription_item_name,
                 subscription_start_date,
                 subscription_end_date,
                 change_type,
                 incentive_type,
                 sum(subscription_arr)::int              as subscription_arr,
                 account_state,
                 account_postal_code,
                 total_fte,
                 territory,
                 industry,
                 min(first_subscription_start_date) as first_subscription_start_date,
                 closed_month_flag,
                 last_refresh_date
          from churn_sheet_hcm
          where idx = 1
          group by change_month_ending,
                   saasoptics_customer_name,
                   account_name,
                   account_id,
                   hcm_legacy_id,
                   account_became_customer_date,
                   product_code,
                   item_code,
                   subscription_item_name,
                   subscription_start_date,
                   subscription_end_date,
                   change_type,
                   incentive_type,
                   account_state,
                   account_postal_code,
                   total_fte,
                   territory,
                   industry,
                   closed_month_flag,
                   last_refresh_date)
        ,
     churn_and_subscriptions AS (SELECT 'Power'                business_division,
                                        change_month_ending,
                                        NULL::varchar(1000) as saasoptics_customer_name,
                                        account_name,
                                        account_id,
                                        NULL::varchar(1000) as hcm_legacy_id,
                                        account_became_customer_date,
                                        product_code,
                                        NULL::varchar(1000) as subscription_item_code,
                                        subscription_item_name,
                                        subscription_start_date,
                                        subscription_end_date,
                                        change_type,
                                        incentive_type,
                                        subscription_arr::int,
                                        account_state,
                                        account_postal_code,
                                        total_fte,
                                        territory,
                                        industry,
                                        churn_description_power,
                                        cancellation_description_power,
                                        first_subscription_start_date,
                                        closed_month_flag,
                                        null::date          as contract_end_date,
                                        last_refresh_date
                                 FROM churn_sheet_pdms_dedup c
                                 UNION ALL
                                 SELECT 'HCM'               as business_division,
                                        change_month_ending,
                                        saasoptics_customer_name,
                                        account_name,
                                        account_id,
                                        hcm_legacy_id,
                                        account_became_customer_date,
                                        product_code,
                                        item_code           as subscription_item_code,
                                        subscription_item_name,
                                        subscription_start_date,
                                        subscription_end_date,
                                        change_type,
                                        incentive_type,
                                        subscription_arr::int,
                                        account_state,
                                        account_postal_code,
                                        total_fte,
                                        territory,
                                        industry,
                                        NULL::varchar(1000) as churn_description_power,
                                        NULL::varchar(1000) as cancellation_description_power,
                                        first_subscription_start_date,
                                        closed_month_flag,
                                        null::date          as contract_end_date,
                                        last_refresh_date
                                 FROM churn_sheet_hcm_dedup c
                                 UNION ALL
                                 SELECT s.business_division,
                                        month_ending,
                                        NULL::varchar(1000) as saasoptics_customer_name,
                                        account_name,
                                        account_id,
                                        NULL::varchar(1000) as hcm_legacy_id,
                                        account_became_customer_date,
                                        product_code,
                                        subscription_item_code,
                                        subscription_item_name,
                                        subscription_start_date,
                                        subscription_end_date,
                                        NULL::varchar(1000) as change_type,
                                        incentive_type,
                                        subscription_arr::int,
                                        account_state,
                                        account_postal_code,
                                        total_fte,
                                        territory,
                                        industry,
                                        NULL::varchar(1000) as churn_description_power,
                                        NULL::varchar(1000) as cancellation_description_power,
                                        first_subscription_start_date,
                                        NULL::int                as closed_month_flag,
                                        contract_end_date,
                                        null::date          as last_refresh_date
                                 FROM product_subscriptions s
                                 )
        ,
     churn_reasons_new AS (SELECT atrisk.createddate                           as at_risk_create_date
                                , h.account_id
                                , NULL::varchar(1000)                          as at_risk_stage
                                , NULL::varchar(1000)                          as at_risk_status
                                , atrisk.churn_date_hcm__c
                                , atrisk.churn_reason__c                                                       -- new field
                                , atrisk.additional_notes__c                                                   -- new field
                                , atrisk.out_of_period_churn_date__c                                           -- new field
                                , atrisk.competitor_chosen_for_churn__c                                        -- new field
                                , atrisk.churn_date_new__c                                                     -- new field
                                , atrisk.status_for_churn__c                                                   -- new field
                                , atrisk.subscription_end_date__c                                              -- new field
                                , h.product_code
                                , h.subscription_end_date
                                , ce.additional_information_reason_if_known__c as customer_engagement_comments -- strip out html tags
                                , NULL::date                                   as customer_engagemnet_date_aware_of_risk
                                , ROW_NUMBER()
                                  OVER (PARTITION BY h.account_id, map_atrisk.product_code, h.subscription_end_date
                                      ORDER BY atrisk.createddate DESC)        as atrisk_index
                                , 'churn_reasons_new'                          as source
                           FROM {{ ref('at_risk_product__c_raw') }} atrisk
                                    LEFT JOIN {{ ref('customer_engagement__c_raw') }} ce
                                              ON ce.id = atrisk.customer_engagement_associated_record__c
                                    LEFT JOIN {{ ref('new_product2_mapping_raw') }} map_atrisk -- quickfix 2024-12-04 (CHANGE TO ALIGN WITH PRODUCTS IN PRODUCT2 OBJECT)
                                              ON ISNULL(atrisk.product_name__c, atrisk.products__c) = map_atrisk.name -- Look into this churn reason attribution
                                    LEFT JOIN churn_and_subscriptions h
                                              ON atrisk.account_id__c = h.account_id
                                                  AND map_atrisk.product_code = h.product_code
                                                  AND h.change_type = 'Churn'

                           WHERE atrisk.isdeleted = 0
                             AND h.change_type IS NOT NULL),
     churn_reasons_legacy AS (SELECT at_risk.createddate                                           as at_risk_create_date
--            , atrisk.account_name__c
                                   , h.account_id
                                   , at_risk.stage__c
                                   , at_risk.status__c
                                   , at_risk.churn_date__c
                                   , COALESCE(at_risk.churn_reason__c, at_risk.at_risk_reason__c)  AS churn_reason__c              -- new field
                                   , COALESCE(at_risk.additional_notes__c, at_risk.explanation__c) AS additional_notes__c          -- new field
                                   , at_risk.out_of_period_churn_date__c                                                           -- new field
                                   , COALESCE(at_risk.competitor_chosen_churn__c,
                                              at_risk.competitor_chosen__c)                        AS competitor_chosen_churn__c   -- new field
                                   , at_risk.churn_date_new__c                                                                     -- new field

                                   , at_risk.status_for_churn__c                                                                   -- new field
                                   , at_risk.subscription_end_date__c                                                              -- new field

                                   , h.product_code
                                   , h.subscription_end_date
                                   , ISNULL(ce.additional_information_reason_if_known__c,
                                            regexp_replace(ce.comments__c, '<[^>]*>'))             as customer_engagement_comments -- strip out html tags
                                   , ce.date_aware_of_risk__c                                      as customer_engagemnet_date_aware_of_risk
                                   , ROW_NUMBER()
                                     OVER (PARTITION BY h.account_id, map_atrisk.mapped_atriskproductname_value, h.subscription_end_date
                                         ORDER BY at_risk.createddate DESC)                        as atrisk_index
                                   , 'churn_reasons_legacy'                                        as source
                              FROM {{ ref('customer_engagement__c_hcm') }} ce
                                       INNER JOIN {{ ref('at_risk_product__c_hcm') }} at_risk
                                                  ON ce.id = at_risk.customer_engagement_associated_record__c
                                       LEFT JOIN --mappings.v_ngv_atriskproduct_atriskproductname_to_productcode map_atrisk
                                  mappings.ngv_atriskproduct_atriskproductname_to_productcode map_atrisk -- quickfix 2024-05-17 (Inserted product code for Attract)
                                                 ON ISNULL(at_risk.product_grouping__c, at_risk.at_risk_product_name__c) =
                                                    map_atrisk.raw_atriskproductname_value
                                       LEFT JOIN churn_and_subscriptions h
                                                 ON ce.account__c = h.hcm_legacy_id
                                                     AND map_atrisk.mapped_atriskproductname_value = h.product_code
--                                                       AND atrisk.createddate <= date_add('year', 1, h.subscription_end_date)

                              WHERE at_risk.isdeleted = 0
                                AND h.change_type = 'Churn')
        ,
     churn_reasons AS (select distinct COALESCE(n.account_id, l.account_id)                             AS account_id,
                                       COALESCE(n.product_code, l.product_code)                         AS product_code,
                                       coalesce(n.at_risk_create_date, l.at_risk_create_date)           as at_risk_create_date,
                                       COALESCE(n.subscription_end_date, l.subscription_end_date)       AS subscription_end_date,
                                       coalesce(n.subscription_end_date__c, l.subscription_end_date__c) as subscription_end_date__c,
                                       l.stage__c                                                       AS at_risk_stage,
                                       l.status__c                                                      as at_risk_status,
                                       coalesce(n.status_for_churn__c, l.status_for_churn__c)           AS status_for_churn__c,
                                       coalesce(n.churn_date_hcm__c, l.churn_date__c)                   AS churn_date_hcm__c,
                                       COALESCE(n.churn_reason__c, l.churn_reason__c)                   AS churn_reason__c,
                                       COALESCE(n.additional_notes__c, l.additional_notes__c)           AS additional_notes__c,
                                       COALESCE(n.out_of_period_churn_date__c,
                                                l.out_of_period_churn_date__c)                          AS out_of_period_churn_date__c,
                                       coalesce(n.competitor_chosen_for_churn__c,
                                                l.competitor_chosen_churn__c)                           AS competitor_chosen_for_churn__c,
                                       COALESCE(n.churn_date_new__c, l.churn_date_new__c)               AS churn_date_new__c,
                                       coalesce(n.customer_engagement_comments,
                                                l.customer_engagement_comments)                         as customer_engagement_comments,
                                       coalesce(n.customer_engagemnet_date_aware_of_risk,
                                                l.customer_engagemnet_date_aware_of_risk)               as customer_engagemnet_date_aware_of_risk
                       FROM (select * from churn_reasons_new where atrisk_index = 1) n
                                full outer join
                                (select * from churn_reasons_legacy l where atrisk_index = 1) l
                                on n.account_id = l.account_id
                                    and n.product_code = l.product_code
                                    and n.subscription_end_date = l.subscription_end_date)
        ,
     nps AS (SELECT COUNT(*) as prod_survey_response_count,
                    SUM(
                            CASE
                                WHEN n.score >= 9 THEN 1
                                ELSE 0
                                END
                    )        as prod_survey_promoter_count,
                    SUM(
                            CASE
                                WHEN n.score <= 6 THEN 1
                                ELSE 0
                                END
                    )        as prod_survey_detractor_count,
                    SUM(
                            CASE
                                WHEN n.timestamp >= dateadd('month', -18, p.change_month_ending) THEN 1
                                ELSE 0
                                END
                    )        as prod_trailing_18mo_survey_response_count,
                    SUM(
                            CASE
                                WHEN n.score >= 9
                                    AND n.timestamp >= dateadd('month', -18, p.change_month_ending) THEN 1
                                ELSE 0
                                END
                    )        as prod_trailing_18mo_survey_promoter_count,
                    SUM(
                            CASE
                                WHEN n.score <= 6
                                    AND n.timestamp >= dateadd('month', -18, p.change_month_ending) THEN 1
                                ELSE 0
                                END
                    )        as prod_trailing_18mo_survey_detractor_count,
                    n.account_id,
                    n.product,
                    p.subscription_end_date,
                    ROW_NUMBER() over (
                        PARTITION BY n.account_id,
                            n.product
                        ORDER BY p.subscription_end_date DESC
                        )    as rn
             FROM derived_datasets.nps n
                      INNER JOIN churn_and_subscriptions p
                                 ON
                                     --n.business_division = 'PDMS'
                                     n.account_id = p.account_id
                                         AND n.timestamp <= p.subscription_end_date
                                         --                                   AND n.product IS NULL
                                         --                                   AND p.product_code = 'PowerPolicy'
                                         AND p.product_code = n.product
             WHERE n.score >= 0
               AND n.survey_type = 'NPS'
             GROUP BY n.account_id,
                      n.product,
                      p.subscription_end_date),
     docebo AS (SELECT t.account_id,
                       COUNT(*) as docebo_course_completions
                FROM derived_datasets.v_training_pdms t
                WHERE t.training_type = 'Course Completions'
                GROUP BY t.account_id),
     cases AS (SELECT COUNT(DISTINCT c.case_id)                                          as case_count,
                      COUNT(DISTINCT CASE
                                         WHEN left(priority, 2) = 'P0' OR left(priority, 2) = 'P1'
                                             THEN c.case_id END)                         as case_count_p1,               -- New field
                      COUNT(DISTINCT CASE
                                         WHEN overall_satisfaction <= 2 AND overall_satisfaction IS NOT NULL
                                             THEN c.case_id END)                         as case_survey_csat_lt3_count,  -- New field
                      COUNT(DISTINCT CASE WHEN c.recommendation >= 9 THEN c.case_id END) as case_survey_promoter_count,
                      COUNT(DISTINCT CASE
                                         WHEN c.recommendation >= 0 AND c.recommendation <= 6
                                             THEN c.case_id END)                         as case_survey_detractor_count,
                      COUNT(DISTINCT CASE WHEN c.recommendation >= 0 THEN c.case_id END) as case_survey_response_count,
                      COUNT(DISTINCT CASE
                                         WHEN (left(priority, 2) = 'P0' OR left(priority, 2) = 'P1') AND
                                              c.created_date >=
                                              dateadd('month', -18, p.change_month_ending)
                                             THEN c.case_id END)                         as case_trailing_18mo_count_p1, -- New field
                      COUNT(DISTINCT CASE
                                         WHEN
                                             (overall_satisfaction <= 2 AND overall_satisfaction IS NOT NULL) AND
                                             c.created_date >=
                                             dateadd('month', -18, p.change_month_ending)
                                             THEN c.case_id END)                         as case_survey_trailing_18mo_csat_lt3_count,
                      COUNT(DISTINCT CASE
                                         WHEN c.recommendation >= 9 AND c.created_date >=
                                                                        dateadd('month', -18, p.change_month_ending)
                                             THEN c.case_id END)                         as case_trailing_18mo_survey_promoter_count,
                      COUNT(DISTINCT CASE
                                         WHEN c.recommendation >= 0 AND c.recommendation <= 6 AND
                                              c.created_date >=
                                              dateadd('month', -18, p.change_month_ending)
                                             THEN c.case_id END)                         as case_trailing_18mo_survey_detractor_count,
                      COUNT(DISTINCT CASE
                                         WHEN c.recommendation >= 0 AND c.created_date >=
                                                                        dateadd('month', -18, p.change_month_ending)
                                             THEN c.case_id END)                         as case_trailing_18mo_survey_response_count,
                      c.account_id,
                      c.product_code,
                      p.subscription_end_date
               FROM derived_datasets.cs_cases c
                        INNER JOIN churn_and_subscriptions p
                                   ON c.account_id = p.account_id
                                       and c.product_code = p.product_code
                                       AND c.created_date <= p.subscription_end_date
               WHERE 1 = 1
               --and p.product_code = 'PowerPolicy'
               GROUP BY c.account_id,
                        p.subscription_end_date, c.product_code),
     implementations AS (SELECT i.implementation_id
                              , i.account_name                                                                                                as account_name
                              , i.implementation_create_date
                              , i.implementation_actual_go_live_date
                              , i.implementation_signoff_date

                              , i.implementation_kickoff_date -- new field
--        , i.implementation_start_date   -- new field

                              , i.implementation_status_point_in_time_raw
                              , i.implementation_stage_point_in_time_raw
                              , cs.subscription_end_date
                              , i.implementation_reporting_go_live_date
                              , i.account_id
                              , case
                                    when i.product_code = 'PowerReady' then 'PowerFTO'
                                    else i.product_code end                                                                                   as product_code
                              , i.reporting_week_end
                              , ROW_NUMBER()
                                OVER (PARTITION BY i.account_id, i.product_code, cs.subscription_end_date ORDER BY i.reporting_week_end DESC) as imp_index

                         FROM derived_datasets.implementations_weekly_hcm i
                                  INNER JOIN churn_and_subscriptions cs
                                             ON i.account_id = cs.account_id
                                                 AND case
                                                         when i.product_code = 'PowerReady' then 'PowerFTO'
                                                         else i.product_code end = cs.product_code
                                                 AND i.reporting_week_end <= cs.subscription_end_date
                         WHERE i.implementation_stage_point_in_time_raw NOT IN
                               ('Biddle Trial Ended', 'PE Trial Ended', 'Trial Ended', 'Trial Purchased',
                                'Contract Cancelled'))
        ,
     admin_turnover AS (SELECT h.subscription_end_date
                             , h.account_id
                             , h.product_code
                             , SUM(CASE WHEN status = 'Deactivated' THEN 1 ELSE 0 END) as admins_deactivated_12mo
                             , SUM(CASE WHEN status = 'Activated' THEN 1 ELSE 0 END)   as admins_activated_12mo
                             , MAX(activeadmincount)                                   as max_active_admins_12mo
                        FROM s3data.neogov_admin_turnover t
                                 INNER JOIN s3data.neogov_accounts na
                                            ON na.employerid = t.employerid
                                 LEFT OUTER JOIN sfdc_silver.mv_account_merge a
                                                 ON na.salesforceid = a.id

                                 INNER JOIN churn_and_subscriptions h
                                            ON a.id = h.account_id
                                                AND t.product = h.product_code

                        WHERE a.id = h.account_id
                          AND t.statusdate::date <= h.subscription_end_date
                          AND t.statusdate::date >= date_add('month', -12, h.subscription_end_date)
                        GROUP BY h.subscription_end_date
                               , h.account_id
                               , h.product_code),
     heartbeat_features as
         (select hb1.month_end,
                 hb1.account_id,
                 hb1.product_code,
                 hb1.heartbeat_feature_name::text,
                 hb1.heartbeat_feature_count_trailing_3_months::int,
                 hb1.heartbeat_feature_count_trailing_3_6_months::int,
                 hb1.heartbeat_feature_count_trailing_6_9_months::int,
                 hb1.heartbeat_feature_count_trailing_9_12_months::int,
                 hb1.heartbeat_feature_count_trailing_6_12_months::int,
                 hb1.heartbeat_feature_count_trailing_12_15_months::int,
                 hb1.heartbeat_feature_count_trailing_15_18_months::int,
                 hb1.heartbeat_feature_count_trailing_12_18_months::int,
                 hb1.heartbeat_feature_count_trailing_18_21_months::int,
                 hb1.heartbeat_feature_count_trailing_21_24_months::int,
                 hb1.heartbeat_feature_count_trailing_18_24_months::int,
                 hb1.hearbeat_feature_most_recent_month::date
          from derived_datasets.v_account_heartbeat_feature_attributes_by_product_hcm_refactored hb1
          union all
          select hb2.month_end,
                 hb2.account_id,
                 hb2.product_code,
                 hb2.heartbeat_feature_name::text,
                 hb2.heartbeat_feature_count_trailing_3_months::int,
                 hb2.heartbeat_feature_count_trailing_3_6_months::int,
                 hb2.heartbeat_feature_count_trailing_6_9_months::int,
                 hb2.heartbeat_feature_count_trailing_9_12_months::int,
                 hb2.heartbeat_feature_count_trailing_6_12_months::int,
                 hb2.heartbeat_feature_count_trailing_12_15_months::int,
                 hb2.heartbeat_feature_count_trailing_15_18_months::int,
                 hb2.heartbeat_feature_count_trailing_12_18_months::int,
                 hb2.heartbeat_feature_count_trailing_18_21_months::int,
                 hb2.heartbeat_feature_count_trailing_21_24_months::int,
                 hb2.heartbeat_feature_count_trailing_18_24_months::int,
                 hb2.hearbeat_feature_most_recent_month::date
          from derived_datasets.v_account_heartbeat_feature_attributes_by_product_power_refactored hb2)
          ,

     churn_attributes as (SELECT pc.*,
                                 a.parentid                                                               as parent_account_id,
                                 cases.case_survey_csat_lt3_count,                                                                -- New field
                                 cases.case_survey_promoter_count,
                                 cases.case_survey_detractor_count,
                                 cases.case_survey_response_count,
                                 cases.case_trailing_18mo_count_p1,                                                               -- New field
                                 cases.case_survey_trailing_18mo_csat_lt3_count,
                                 cases.case_trailing_18mo_survey_promoter_count,
                                 cases.case_trailing_18mo_survey_detractor_count,
                                 cases.case_trailing_18mo_survey_response_count,

                                 cases.case_count,

                                 cr.at_risk_create_date::date,
                                 cr.at_risk_stage,
                                 cr.at_risk_status                                                        as at_risk_status,
                                 ISNULL(cr.churn_date_new__c, cr.churn_date_hcm__c)                       as at_risk_churn_date,

                                 cr.out_of_period_churn_date__c                                           as at_risk_out_of_period_churn_date,
                                 cr.competitor_chosen_for_churn__c                                        as at_risk_competitor_chosen_churn,
                                 cr.status_for_churn__c::varchar(1000)                                    as at_risk_status_for_churn,
                                 cr.subscription_end_date__c                                              as at_risk_subscription_end_date,

                                 cr.churn_reason__c                                                       as at_risk_reason,
                                 cr.additional_notes__c                                                   as at_risk_explanation, -- updated logic to consider additional notes

                                 cr.customer_engagement_comments,
                                 cr.customer_engagemnet_date_aware_of_risk,

                                 docebo.docebo_course_completions,

                                 imp.implementation_id::varchar(1000),
                                 imp.implementation_create_date::date,
                                 imp.implementation_actual_go_live_date::date,
                                 imp.implementation_reporting_go_live_date::date,
                                 imp.implementation_signoff_date::date,
                                 imp.implementation_kickoff_date::date,
--         imp.implementation_start_date::date,
                                 imp.implementation_status_point_in_time_raw::varchar(1000)               as implementation_status,
                                 (CASE
                                      WHEN pc.product_code = 'GJ' THEN 'Live'
                                      ELSE imp.implementation_stage_point_in_time_raw END)::varchar(1000) as implementation_stage,

                                 x.first_accred_association_date,
                                 x.first_accred_required_platform_date,
                                 x.is_accred_affiliated,
                                 x.is_partner_affiliated,
                                 x.accred_affiliation_ps,

                                 admin_turnover.admins_activated_12mo::int,
                                 admin_turnover.admins_deactivated_12mo::int,
                                 admin_turnover.max_active_admins_12mo::int,

                                 hb.heartbeat_feature_name::text,
                                 hb.heartbeat_feature_count_trailing_3_months::int,
                                 hb.heartbeat_feature_count_trailing_3_6_months::int,
                                 hb.heartbeat_feature_count_trailing_6_9_months::int,
                                 hb.heartbeat_feature_count_trailing_9_12_months::int,
                                 hb.heartbeat_feature_count_trailing_6_12_months::int,
                                 hb.heartbeat_feature_count_trailing_12_15_months::int,
                                 hb.heartbeat_feature_count_trailing_15_18_months::int,
                                 hb.heartbeat_feature_count_trailing_12_18_months::int,
                                 hb.heartbeat_feature_count_trailing_18_21_months::int,
                                 hb.heartbeat_feature_count_trailing_21_24_months::int,
                                 hb.heartbeat_feature_count_trailing_18_24_months::int,
                                 hb.hearbeat_feature_most_recent_month::date,

                                 MAX(
                                 CASE
                                     WHEN pc.closed_month_flag = 1 THEN pc.change_month_ending
                                     END
                                    )
                                 OVER ()                                                                  as latest_closed_month_ending
                          FROM churn_and_subscriptions pc
                                   LEFT JOIN derived_datasets.v_account_partnership_attributes_refactored x -- TODO: REVIEW THIS DATASET
                                             ON pc.account_id = x.account_id
                                   LEFT JOIN sfdc_silver.mv_account_merge a ON pc.account_id = a.id
                                   LEFT OUTER JOIN churn_reasons cr ON pc.account_id = cr.account_id
                              AND pc.product_code = cr.product_code
                              AND pc.subscription_end_date = cr.subscription_end_date
                              --AND cr.atrisk_index = 1
                              AND pc.change_type = 'Churn'
                                   LEFT JOIN nps ON pc.account_id = nps.account_id
                              AND pc.subscription_end_date = nps.subscription_end_date
                              AND pc.product_code = nps.product
                                   LEFT OUTER JOIN docebo ON docebo.account_id = pc.account_id
                                   LEFT OUTER JOIN cases ON pc.account_id = cases.account_id
                              AND pc.subscription_end_date = cases.subscription_end_date
                              AND pc.product_code = cases.product_code
                                   LEFT OUTER JOIN implementations imp ON imp.imp_index = 1
                              AND imp.account_id = pc.account_id
                              AND imp.product_code = pc.product_code
                              AND imp.subscription_end_date = pc.subscription_end_date
                                   LEFT OUTER JOIN heartbeat_features hb
                                                   ON hb.account_id = pc.account_id
                                                       AND hb.product_code = pc.product_code
                                                       AND hb.month_end =
                                                           last_day(date_add('month', -1, pc.change_month_ending))
                              
                                   LEFT OUTER JOIN admin_turnover ON pc.account_id = admin_turnover.account_id
                              AND pc.product_code = admin_turnover.product_code
                              AND pc.subscription_end_date = admin_turnover.subscription_end_date)
        ,
     churn_final AS (SELECT CASE
                                WHEN business_division::varchar(10) = 'PDMS' THEN 'Power'
                                ELSE business_division::varchar(10) END AS business_division,
                            change_month_ending::date,
                            saasoptics_customer_name::varchar(1000), -- new column
                            account_name::varchar(1000),
                            account_id::varchar(50),
                            parent_account_id::varchar(50),
                            account_became_customer_date::date,
                            product_code::varchar(50),
                            

                            subscription_item_code::varchar(1000),   -- new column
                             change_type::varchar(50)

                            ,subscription_item_name::varchar(1000),
                            subscription_start_date::date,
                            subscription_end_date::date,
                           
                            incentive_type::varchar(1000),
                            0 as subscription_arr, --subscription_arr::int,
                            account_state::varchar(50),
                            account_postal_code::varchar(50),
                            total_fte::int,
                            territory::varchar(1000),
                            industry::varchar(1000),
                            churn_description_power::varchar(1000),
                            cancellation_description_power::varchar(1000),
                            first_subscription_start_date::date

                            ,at_risk_create_date::date,
                            at_risk_stage::varchar(1000),
                            at_risk_status::varchar(1000),
                            at_risk_churn_date::date,
                            at_risk_out_of_period_churn_date::date,
                            at_risk_competitor_chosen_churn::varchar(1000),
                            at_risk_status_for_churn::varchar(1000),
                            at_risk_subscription_end_date::date,

                            at_risk_reason::varchar(1000),
                            at_risk_explanation::varchar(1000),

                            customer_engagement_comments::text,
                            customer_engagemnet_date_aware_of_risk::date,

                            implementation_id::varchar(1000),
                            implementation_create_date::date,
                            implementation_actual_go_live_date::date,
                            implementation_reporting_go_live_date::date,
                            implementation_signoff_date::date,
                            implementation_kickoff_date::date,
--         implementation_start_date::date,
                            --      , NULL::date as implementation_start_date,
                            implementation_status::varchar(1000),
                            implementation_stage::varchar(1000)

                            ,case_survey_csat_lt3_count,              -- New field
                            case_survey_promoter_count,
                            case_survey_detractor_count,
                            case_survey_response_count,
                            case_trailing_18mo_count_p1,             -- New field
                            case_survey_trailing_18mo_csat_lt3_count,
                            case_trailing_18mo_survey_promoter_count,
                            case_trailing_18mo_survey_detractor_count,
                            case_trailing_18mo_survey_response_count,

                            case_count::int,
                            docebo_course_completions::int,
                            first_accred_association_date::date,
                            first_accred_required_platform_date::date,
                            is_accred_affiliated::int,
                            is_partner_affiliated::boolean,
                            accred_affiliation_ps::text,

                            admins_activated_12mo::int,
                            admins_deactivated_12mo::int,
                            max_active_admins_12mo::int,

                            heartbeat_feature_name::text,
                            heartbeat_feature_count_trailing_3_months::int,
                            heartbeat_feature_count_trailing_3_6_months::int,
                            heartbeat_feature_count_trailing_6_9_months::int,
                            heartbeat_feature_count_trailing_9_12_months::int,
                            heartbeat_feature_count_trailing_6_12_months::int,
                            heartbeat_feature_count_trailing_12_15_months::int,
                            heartbeat_feature_count_trailing_15_18_months::int,
                            heartbeat_feature_count_trailing_12_18_months::int,
                            heartbeat_feature_count_trailing_18_21_months::int,
                            heartbeat_feature_count_trailing_21_24_months::int,
                            heartbeat_feature_count_trailing_18_24_months::int,
                            hearbeat_feature_most_recent_month::date,

                            latest_closed_month_ending,
                            contract_end_date,
                            last_refresh_date 
                     FROM churn_attributes f),
     churn_hist AS (SELECT CASE
                               WHEN business_division::varchar(10) = 'PDMS' THEN 'Power'
                               ELSE business_division::varchar(10) END,
                           change_month_ending::date,
                           saasoptics_customer_name::varchar(1000),
                           account_name::varchar(1000),
                           ISNULL(am.account_id::varchar(50), ch.account_id::varchar(50)) as account_id,
                           parent_account_id::varchar(50),
                           account_became_customer_date::date,
                           product_code::varchar(50),
                           subscription_item_code::varchar(1000),
                           change_type::varchar(50)
                           
                           ,subscription_item_name::varchar(1000),
                           subscription_start_date::date,
                           subscription_end_date::date,
                           
                           NULL::varchar(1000)                                            as incentive_type
                           ,0 as subscription_arr --subscription_arr::int
                           ,account_state::varchar(50),
                           account_postal_code::varchar(50),
                           total_fte::int,
                           territory::varchar(1000),
                           industry::varchar(1000),
                           churn_description_power::varchar(1000),
                           cancellation_description_power::varchar(1000),
                           first_subscription_start_date::date

                           ,at_risk_create_date::date,
                           at_risk_stage::varchar(1000),
                           at_risk_status::varchar(1000),
                           at_risk_churn_date::date,
                           at_risk_out_of_period_churn_date::date,
                           at_risk_competitor_chosen_churn::varchar(1000),
                           at_risk_status_for_churn::varchar(1000),
                           at_risk_subscription_end_date::date,

                           at_risk_reason::varchar(1000),
                           at_risk_explanation::varchar(1000),

                           customer_engagement_comments::text,
                           customer_engagemnet_date_aware_of_risk::date,

                           implementation_id::varchar(1000),
                           implementation_create_date::date,
                           implementation_actual_go_live_date::date,
                           implementation_reporting_go_live_date::date,
                           implementation_signoff_date::date,
                           implementation_kickoff_date::date,
--         implementation_start_date::date,
                           --      , NULL::date as implementation_start_date,
                           implementation_status::varchar(1000),
                           implementation_stage::varchar(1000)

                            ,case_survey_csat_lt3_count,  -- New field
                           prod_survey_promoter_count::int                                as case_survey_promoter_count,
                           prod_survey_detractor_count::int                               as case_survey_detractor_count,
                           prod_survey_response_count::int                                as case_survey_response_count,
                           case_trailing_18mo_count_p1, -- New field
                           case_survey_trailing_18mo_csat_lt3_count,
                           prod_trailing_18mo_survey_promoter_count                       as case_trailing_18mo_survey_promoter_count,
                           prod_trailing_18mo_survey_detractor_count::int                 as case_trailing_18mo_survey_detractor_count,
                           prod_trailing_18mo_survey_response_count                       as case_trailing_18mo_survey_response_count,
                        
                           case_count::int,
                           null::int,                   --docebo_course_completions::int,
                           first_accred_association_date::date,
                           first_accred_required_platform_date::date,
                           is_accred_affiliated::int,
                           is_partner_affiliated::boolean,
                           accred_affiliation_ps::text,

                           admins_activated_12mo::int,
                           admins_deactivated_12mo::int,
                           max_active_admins_12mo::int,

                           heartbeat_feature_name::text,
                           heartbeat_feature_count_trailing_3_months::int,
                           heartbeat_feature_count_trailing_3_6_months::int,
                           heartbeat_feature_count_trailing_6_9_months::int,
                           heartbeat_feature_count_trailing_9_12_months::int,
                           heartbeat_feature_count_trailing_6_12_months::int,
                           heartbeat_feature_count_trailing_12_15_months::int,
                           heartbeat_feature_count_trailing_15_18_months::int,
                           heartbeat_feature_count_trailing_12_18_months::int,
                           heartbeat_feature_count_trailing_18_21_months::int,
                           heartbeat_feature_count_trailing_21_24_months::int,
                           heartbeat_feature_count_trailing_18_24_months::int,
                           hearbeat_feature_most_recent_month::date,

                           latest_closed_month_ending,
                           contract_end_date,
                           last_refresh_date 
                    FROM sfdc_silver.mv_account_product_churn_hist ch
                             LEFT JOIN derived_datasets.v_account_mapping am
                                       ON am.hcm_legacy_id__c = ch.account_id),
     churn_legacy as
         (select *
          from (select h.*,
                       row_number()
                       over (partition by account_id,product_code,change_month_ending order by change_month_ending desc) rn
                from churn_hist h
                where h.change_type = 'Churn'
                  and account_id is not null) churn_leg
          where rn = 1)
select combined.business_division
     , combined.change_month_ending
     , combined.saasoptics_customer_name
     , combined.account_name
     , combined.account_id
     , combined.parent_account_id
     , combined.account_became_customer_date
     , combined.product_code
     , combined.subscription_item_code
     , combined.change_type
     
     , combined.subscription_item_name
     , combined.subscription_start_date
     , combined.subscription_end_date
     
     , combined.incentive_type
     , combined.subscription_arr::int
     , combined.account_state
     , combined.account_postal_code
     , combined.total_fte
     , coalesce(a.industry, a.territory2__c)                          as territory
     , coalesce(a.industry, a.territory2__c)                          as industry
     , combined.churn_description_power
     , combined.cancellation_description_power
     , combined.first_subscription_start_date
     
     
     , coalesce(combined.at_risk_create_date, cl.at_risk_create_date) as at_risk_create_date
     , coalesce(combined.at_risk_stage, cl.at_risk_stage)             as at_risk_stage
     , coalesce(combined.at_risk_status, cl.at_risk_status)           as at_risk_status
     , coalesce(combined.at_risk_churn_date, cl.at_risk_churn_date)   as at_risk_churn_date
     , coalesce(combined.at_risk_out_of_period_churn_date,
                cl.at_risk_out_of_period_churn_date)                  as at_risk_out_of_period_churn_date
     , coalesce(combined.at_risk_competitor_chosen_churn,
                cl.at_risk_competitor_chosen_churn)                   as at_risk_competitor_chosen_churn
     , coalesce(combined.at_risk_status_for_churn,
                cl.at_risk_status_for_churn)                          as at_risk_status_for_churn
     , coalesce(combined.at_risk_subscription_end_date,
                cl.at_risk_subscription_end_date)                     as at_risk_subscription_end_date
     , coalesce(combined.at_risk_reason, cl.at_risk_reason)           as at_risk_reason
     , coalesce(combined.at_risk_explanation, cl.at_risk_explanation) as at_risk_explanation
     , coalesce(combined.customer_engagement_comments,
                cl.customer_engagement_comments)                      as customer_engagement_comments
     , coalesce(combined.customer_engagemnet_date_aware_of_risk,
                cl.customer_engagemnet_date_aware_of_risk)            as customer_engagemnet_date_aware_of_risk
     , combined.implementation_id
     , combined.implementation_create_date
     , combined.implementation_actual_go_live_date
     , combined.implementation_reporting_go_live_date
     , combined.implementation_signoff_date
     , combined.implementation_kickoff_date
     , combined.implementation_status
     , combined.implementation_stage

     , combined.case_survey_csat_lt3_count  -- New field
     , combined.case_survey_promoter_count
     , combined.case_survey_detractor_count
     , combined.case_survey_response_count
     , combined.case_trailing_18mo_count_p1 --New field
     , combined.case_survey_trailing_18mo_csat_lt3_count
     , combined.case_trailing_18mo_survey_promoter_count
     , combined.case_trailing_18mo_survey_detractor_count
     , combined.case_trailing_18mo_survey_response_count
     , combined.case_count
     , docebo_course_completions
     , combined.first_accred_association_date
     , combined.first_accred_required_platform_date
     , combined.is_accred_affiliated
     , combined.is_partner_affiliated
     , combined.accred_affiliation_ps
     , combined.admins_activated_12mo
     , combined.admins_deactivated_12mo
     , combined.max_active_admins_12mo
     , combined.heartbeat_feature_name
     , combined.heartbeat_feature_count_trailing_3_months
     , combined.heartbeat_feature_count_trailing_3_6_months
     , combined.heartbeat_feature_count_trailing_6_9_months
     , combined.heartbeat_feature_count_trailing_9_12_months
     , combined.heartbeat_feature_count_trailing_6_12_months
     , combined.heartbeat_feature_count_trailing_12_15_months
     , combined.heartbeat_feature_count_trailing_15_18_months
     , combined.heartbeat_feature_count_trailing_12_18_months
     , combined.heartbeat_feature_count_trailing_18_21_months
     , combined.heartbeat_feature_count_trailing_21_24_months
     , combined.heartbeat_feature_count_trailing_18_24_months
     , combined.hearbeat_feature_most_recent_month
     , combined.latest_closed_month_ending
     , combined.contract_end_date
     , combined.last_refresh_date 
from (SELECT distinct *
      FROM churn_final
      WHERE change_month_ending >= '2022-01-01'
      UNION ALL
      SELECT distinct *
      FROM churn_hist
      WHERE change_month_ending < '2022-01-01') combined
         left join sfdc_silver.mv_account_merge a
                   on a.id = combined.account_id
         left join churn_legacy cl
                   on cl.account_id = combined.account_id
                       and cl.product_code = combined.product_code
                       and cl.change_month_ending = combined.change_month_ending 
                       
                       