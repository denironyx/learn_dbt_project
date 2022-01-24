{{ config(materialized='incremental', unique_key='payment_id') }}

SELECT *,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS date_time
FROM
stg.payment
WHERE 1 = 1

{% if is_incremental() %}
and payment_date::timestamp > (SELECT MAX(payment_date) FROM {{this}})
{% endif %}