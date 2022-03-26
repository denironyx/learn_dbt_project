-- Alias
{{ config(materialized='table', alias = 'customers_alias', schema = 'itamar') }}

SELECT * 
FROM {{ ref('hello_world') }}
WHERE customer_id < {{ var('cust_id') }}