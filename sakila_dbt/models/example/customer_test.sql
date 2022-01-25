SELECT * 
FROM {{ ref('hello_world') }}
WHERE customer_id < {{ var('cust_id') }}