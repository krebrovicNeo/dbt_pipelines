
{{ config(materialized='table') }}

select
    case
        when pm.item_code in ('QC-001', 'QC-002')
            then a.brand_name_formula__c
        else pm.business_division 
    end::varchar(16383)                                                 as business_division,


    case
        when o.original_opportunity_owner_role__c ILIKE '%Customer Success Manager%'
            then 'HCM'
        when o.original_opportunity_owner_role__c ILIKE '%HCM%' 
            then 'HCM'
        when o.original_opportunity_owner_role__c ILIKE '%Power%' 
            then 'Power'
        else a.brand_name_formula__c 
    end                                                                 as sales_division,

    
    case
        when a.date_became_customer__c < o.closedate 
            then 'Existing'
        else 'New' 
    end                                                                 as business_type,


    DATE(o.closedate)                                                   as reporting_date,
    a.id                                                                as account_id,
    a.name                                                              as account_name,
    a.industry                                                          as account_market,           -- Agency Type will be migrated to the industry field. Will be different values after 10/01/2024 for HCM
    a.type                                                              as account_type,
    a.shippingstate                                                     as account_state,
    a.shippingpostalcode                                                as account_postal_code,
    NULL::text                                                          as segmentation_tier         -- #TODO: REVIEW - Does not exist in PDMS
    ,
    null::text                                                          as agency_type               -- #TODO: REVIEW - Does not exist in PDMS
    ,
    o.id                                                                as opportunity_id,
    o.type                                                              as opportunity_type,


    case 
        when issqo__c = 1 
            then o.appointment_date__c 
        else o.createddate 
    end                                                                 as opportunity_created_date, -- #TODO: REVIEW - check logic with RK


    o.purchasing_vehicle__c                                             as purchasing_vehicle,
    o.third_party_reseller__c                                           as third_party_reseller,
    o.co_op_agreement__c                                                as co_op_agreement,
    a.numberofemployees                                                 as total_ftes,               -- #TODO: REVIEW - check logic with RK
    a.date_became_customer__c                                           as became_customer_date,
    pm.product_code                                                     as product_code,
    li.productcode                                                      as product_code_raw,
    p.name                                                              as item_name,                -- #TODO: REVIEW
    p.family                                                            as product_family,
    li.product_family__c                                                as products_from_quote,
    li.product_type__c                                                  as product_type,

    x.first_accred_association_date,
    x.first_accred_required_platform_date,
    x.is_accred_affiliated,
    x.is_partner_affiliated,
    x.accred_affiliation_ps,


    min(date(o.closedate))
        over (partition by a.id, m.raw_productfamily_value)             as first_product_purchase_date,
    
    
    fsub.first_product_subscription_start_date,


    DENSE_RANK() over (
        partition by o.id, li.productcode
        order by li.final_year_arr_rep__c desc, li.enddate__c desc
        )::bigint                                                       as index,


        case
            when li.product_type__c = 'Services' 
                then NULLIF(li.net_total__c, '')::float8
            else 0
        end                                                                 as services_bookings_amount,


    NULL::float8                                                            as arr_bookings_amount,
    li.final_year_arr_rep__c::float8                                        as arr_bookings_amount_finance,
    NULL::float8                                                            as total_bookings_amount,


    case
        when li.product_type__c = 'Services' 
            then NULLIF(li.net_total__c, '')::float8
        else li.final_year_arr_rep__c::float8
    end                                                                     as total_bookings_amount_finance,


    NULL::integer                                                           as arr_bookings_units,


    case 
        when li.is_core_product__c = 1 
            then 1 
        else 0 
    end                                                                     as is_core_product, -- New revision 20250128

    o.owner_name__c                                                         as owner_name,
    o.name                                                                  as opportunity_name,
    o.original_opportunity_owner_role__c                                    as original_opp_owner_role,
    r.name                                                                  as opp_owner_role,
    a.referral_partner_content_provider__c                                  as referral_partner_content_provider,


    case
        when li.product_family__c in
            (select m.products_from_quote
            from mappings.v_ngv_opplineitem_product_to_partnerintegration m)
            then 1
        else 0 
    end                                                                     as is_partner_integration,
    li.isunit__c                                                            as is_unit


from sfdc_silver.mv_opportunitylineitem_merge li

left outer join sfdc_silver.mv_opplineitem_productfamily_to_productgroup m
    on li.product_family__c = m.raw_productfamily_value


left outer join product_mappings pm 
    on li.productcode = pm.item_code


inner join {{ ref('opportunity_raw') }}  o 
    on li.opportunityid = o.id


inner join {{ ref('account_raw') }}  a 
    on o.accountid = a.id


left outer join derived_datasets.v_pdms_account_partnership_attributes x
    on x.account_id = a.id


left outer join {{ ref('product2_raw') }}  p
    on li.product2id = p.id 
        and p.isdeleted = 0


left outer join first_product_subscription fsub
    on fsub.account_id = a.id
        and case
                when fsub.product_code = 'PowerSchedule' 
                    then 'Schedule'
                else fsub.product_code 
            end =
            case
                when m.mapped_productfamily_value = 'PowerSchedule'
                    then 'Schedule'
                else m.mapped_productfamily_value 
            end


left outer join {{ ref('user_raw') }}  u 
    on u.id = o.ownerid


left outer join {{ ref('userrole_raw') }}  r on r.id = u.userroleid


where o.stagename = 'Closed Won'
    and o.isdeleted = 0
    and li.isdeleted = 0
    and li.final_year_arr_line_item__c = TRUE
    and COALESCE(o.owner_name__c, '') not ILIKE '%agency360%'
    and COALESCE(o.name, '') not ILIKE '%Quota Credit%'
    and COALESCE(li.renewed_subscription__c, '') = ''
    and (
        UPPER(TRIM(o.type)) in ('EXISTING BUSINESS', 'NEW BUSINESS')
            or (
            UPPER(TRIM(o.type)) = 'RENEWAL + ADD-ON'
                and COALESCE(UPPER(TRIM(li.revenue_metric__c)), '') <> 'RENEWAL'
            )
        )