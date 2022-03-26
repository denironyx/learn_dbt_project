{{ config(materialized='table', post_hook="insert into {{this}}(customer_id) VALUES (-1)")}}
SELECT
customer_id,
first_name,
last_name,
{{ concat_it('first_name', 'last_name') }} AS the_full_name
FROM
{{ ref('hello_world') }}
WHERE customer_id < {{ var('cust_id') }}