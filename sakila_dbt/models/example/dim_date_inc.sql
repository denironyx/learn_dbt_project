{{ config(materialized='incremental', unique_key='date_dim_id') }}

SELECT 
*
FROM {{ ref('dim_date') }}
WHERE 1=1

{% if is_incremental() %}
AND date_key::timestamp > (SELECT MAX(date_key) - INTERVAL '3 DAY' from {{this}})
{% endif %}