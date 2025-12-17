{{ config(materialized='table') }}

SELECT *
                                   FROM sfdc_silver.mv_merge_opportunity_maincoreproduct_to_productcode opp0 --#TODO: REVIEW - Check this mappings to most current core products
                                   WHERE opp0.mapped_main_core_product_value <> 'unmapped'
                                     AND opp0.mapped_main_core_product_value not in ('GJ', 'PowerStandards')