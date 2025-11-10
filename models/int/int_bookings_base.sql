{{ config(materialized='ephemeral') }}

with first_product_subscription as (
    select * from {{ ref('int_first_product_subscription') }}
),

select case
            when
                product_mappings.item_code in ('QC-001', 'QC-002')
                then account.brand_name_formula__c
            else
                product_mappings.business_division end                                 as business_division,


        CASE
            WHEN opportunity.original_opportunity_owner_role__c ILIKE '%Customer Success Manager%' 
                THEN 'HCM'
            WHEN opportunity.original_opportunity_owner_role__c ILIKE '%HCM%' 
                THEN 'HCM'
            WHEN opportunity.original_opportunity_owner_role__c ILIKE '%Power%' 
                THEN 'Power'
            ELSE account.brand_name_formula__c END                                     as sales_division,


        CASE
            WHEN account.date_became_customer__c < opportunity.closedate 
                THEN 'Existing'
            ELSE 'New' END                                                             as business_type,


        DATE(opportunity.closedate)                                                    as reporting_date,
        account.id                                                                     as account_id,
        account.name                                                                   as account_name,
        account.industry                                                               as account_market,           -- Agency Type will be migrated to the industry field. Will be different values after 10/01/2024 for HCM
        account.type                                                                   as account_type,
        account.shippingstate                                                          as account_state,
        account.shippingpostalcode                                                     as account_postal_code,
        NULL::text                                                                     as segmentation_tier,         -- #TODO: REVIEW - Does not exist in PDMS
        null::text                                                                     as agency_type,               -- #TODO: REVIEW - Does not exist in PDMS
        opportunity.id                                                                 as opportunity_id,
        opportunity.type                                                               as opportunity_type,


        CASE 
            WHEN issqo__c = 1 
                THEN opportunity.appointment_date__c 
            ELSE opportunity.createddate 
        END                                                                            as opportunity_created_date, -- #TODO: REVIEW - check logic with RK


        opportunity.purchasing_vehicle__c                                              as purchasing_vehicle,
        opportunity.third_party_reseller__c                                            as third_party_reseller,
        opportunity.co_op_agreement__c                                                 as co_op_agreement,
        account.numberofemployees                                                      as total_ftes,               -- #TODO: REVIEW - check logic with RK
        account.date_became_customer__c                                                as became_customer_date,
        product_mappings.product_code                                                  as product_code,
        opportunity_product.productcode                                                as product_code_raw,
        product.name                                                                   as item_name,                -- #TODO: REVIEW
        product.family                                                                 as product_family,
        opportunity_product.product_family__c                                          as products_from_quote,
        opportunity_product.product_type__c                                            as product_type,
        partnership_attributes.first_accred_association_date,
        partnership_attributes.first_accred_required_platform_date,
        partnership_attributes.is_accred_affiliated,
        partnership_attributes.is_partner_affiliated,
        partnership_attributes.accred_affiliation_ps,
        
        
        MIN(date(opportunity.closedate)) OVER (
            PARTITION BY account.id, productfamily_to_productgrouproduct.raw_productfamily_value
            )                                                                          as first_product_purchase_date,


        first_product_subscription.first_product_subscription_start_date,
        
        
        DENSE_RANK() OVER (
            PARTITION BY opportunity.id,
                opportunity_product.productcode
            ORDER BY opportunity_product.calculate_arr_formula__c DESC,
                opportunity_product.enddate__c DESC
            )                                                                          as index,

            
        CASE
            WHEN opportunity_product.product_type__c = 'Services' 
                THEN NULLIF(opportunity_product.net_total__c, '')::numeric
            ELSE 0
        END                                                                            as services_bookings_amount,


        opportunity_product.commissionable_arr_rep__c                                  as arr_bookings_amount,
        NULL::float8                                                                   as arr_bookings_amount_finance,


        CASE
            WHEN opportunity_product.product_type__c = 'Services' 
                THEN NULLIF(opportunity_product.net_total__c, '')::numeric
            ELSE opportunity_product.commissionable_arr_rep__c
        END                                                                            as total_bookings_amount,
        NULL::float8                                                                   as total_bookings_amount_finance,


        CASE
            WHEN
                (opportunity_product.is_core_product__c = 1 -- #TODO: REVIEW - is core product on line items
                    OR opportunity_product.product_family__c IN (
                                                'Standards',
                                                'Training',
                                                'Schedule',
                                                'GovernmentJobs'
                        )
                    )
                    AND product.product_type__c = 'Recurring'
                    AND opportunity_product.calculate_arr_formula__c = 1
                    AND (
                    (
                        first_product_subscription.first_product_subscription_start_date >=
                        DATEADD('day', -90, opportunity.closedate) -- #TODO: REVIEW - check this logic!
                        )
                        AND opportunity_product.upgraded_subscription__c IS NULL
                    )
                    and COALESCE(
                                opportunity_product.commissionable_arr_rep__c,
                                opportunity_product.line_arr__c,
                                opportunity_product.arr__c
                        ) < 0 THEN -1
            WHEN (
                    opportunity_product.is_core_product__c = 1 -- #TODO: REVIEW - is core product on line items
                        OR opportunity_product.product_family__c IN (
                                                    'Standards',
                                                    'Training',
                                                    'Schedule',
                                                    'GovernmentJobs'
                        )
                    )
                AND opportunity_product.isdeleted = 0
                AND product.isdeleted = 0
                AND product.product_type__c = 'Recurring'
                AND (
                    (opportunity_product.commissionable_arr_rep__c > 0)
                        OR (opportunity_product.line_arr__c > 0)
                        OR (opportunity_product.arr__c > 0)
                    )
                AND opportunity_product.calculate_arr_formula__c = 1
                AND (
                    (
                        (
                            first_product_subscription.first_product_subscription_start_date >=
                            DATEADD('day', -90, opportunity.closedate) -- #TODO: REVIEW - check this logic!
                            )
                            AND opportunity_product.upgraded_subscription__c IS NULL
                        )
                        or act.account_id is null
                    )
                AND ROW_NUMBER() OVER (
                    PARTITION BY opportunity.closedate,
                        opportunity.accountid,
                        opportunity_product.productcode
                    ORDER BY opportunity_product.commissionable_arr_rep__c DESC,
                        opportunity_product.line_arr__c DESC,
                        opportunity_product.arr__c DESC,
                        opportunity.closedate DESC
                    ) = 1 -- only attribute 1 unit per product/account/date
                THEN 1
            ELSE 0
            END                                                                            as arr_bookings_units,


        CASE 
            WHEN opportunity_product.is_core_product__c = 1 
                THEN 1 
            ELSE 0 
        END                                                                                as is_core_product,          -- New revision 20250128


        opportunity.owner_name__c                                                          as owner_name,
        opportunity.name                                                                   as opportunity_name,
        opportunity.original_opportunity_owner_role__c                                     as original_opp_owner_role,
        user_role.name                                                                     as opp_owner_role,
        account.referral_partner_content_provider__c                                       as referral_partner_content_provider,
        

        CASE
            WHEN opportunity_product.product_family__c IN
                (SELECT product_to_partnerintegration.products_from_quote
                FROM mappings.v_ngv_opplineitem_product_to_partnerintegration product_to_partnerintegration) -- #TODO: REVIEW - check mappings for accuracy
                THEN 1
            ELSE 0 END                                                                     as is_partner_integration,


        opportunity_product.isunit__c                                                      as is_unit

from {{ ref('raw_opportunitylineitem') }}  opportunity_product


left outer join sfdc_silveuser_role.mv_opplineitem_productfamily_to_productgroup productfamily_to_productgroup -- create raw in dbt
    on opportunity_product.product_family__c = productfamily_to_productgrouproduct.raw_productfamily_value


left outer join {{ ref('raw_new_product2_mappings') }} product_mappings
    on opportunity_product.productcode = product_mappings.item_code


inner join {{ ref('raw_opportunity') }}  opportunity 
    on opportunity_product.opportunityid = opportunity.id


inner join {{ ref('raw_account') }}  account 
    on opportunity.accountid = account.id


left outer join {{ ref('int_account_partnership_attributes') }} partnership_attributes 
    on partnership_attributes.account_id = account.id


left outer join {{ ref('raw_product2') }}  product 
    on opportunity_product.product2id = product.id 
        and product.isdeleted = 0


left outer join first_product_subscription
    on first_product_subscription.account_id = account.id
        and
        case
            when first_product_subscription.product_code = 'PowerSchedule' then 'Schedule'
            else first_product_subscription.product_code end =
        case
            when productfamily_to_productgrouproduct.mapped_productfamily_value = 'PowerSchedule' then 'Schedule'
            else productfamily_to_productgrouproduct.mapped_productfamily_value end


left outer join {{ ref('raw_user') }}  user 
    on useuser_role.id = opportunity.ownerid


left outer join {{ ref('raw_userrole') }}  user_role 
    on user_role.id = useuser_role.userroleid


left join {{ ref('int_active_subscriptions') }} active_subscriptions
    on active_subscriptions.account_id = account.id
        and active_subscriptions.product_code = product_mappings.product_code
        and active_subscriptions.month_ending_date = DATEADD(day, -1, DATE_TRUNC('month', DATEADD(month, -2, opportunity.closedate)))


where opportunity.stagename = 'Closed Won'
    and opportunity.isdeleted = 0
    and opportunity_product.isdeleted = 0
    and (
            opportunity.type is null
            or not (
            opportunity."type"::text in ('Migration Target', 'Renewal')
            )
        )
    and not (
        (
            -- ignore opp line items where the parent opportunity has no revenue associated with it; we found some anomalies like this.
            opportunity.contract_1st_year__c = 0
            and opportunity.opp1styrrevenue_master__c = 0
        ) 
        or (
        opportunity.contract_1st_year__c is null
            and opportunity.opp1styrrevenue_master__c is null
        )
    )
    and (opportunity.original_opportunity_owner_role__c is null or opportunity.original_opportunity_owner_role__c not ILIKE 'FNL%')
    and opportunity.closedate >= '2024-01-01'