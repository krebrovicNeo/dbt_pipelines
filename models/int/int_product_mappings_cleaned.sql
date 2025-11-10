{{ config(materialized='ephemeral') }}

select distinct
    item_code,
    product_code,
    family,
    business_division
from {{ ref('raw_new_product2_mapping') }}