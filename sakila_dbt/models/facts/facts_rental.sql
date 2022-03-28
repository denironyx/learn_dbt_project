{{ config(materialized='incremental', unique_key='rental_id') }}
with rental_base as (
SELECT 
    *,
    EXTRACT(EPOCH from rental_date::timestamp) as rental_epoch,
    EXTRACT(EPOCH from return_date::timestamp) as return_epoch,
    EXTRACT(EPOCH from return_date::timestamp) - EXTRACT(EPOCH from rental_date::timestamp) as diff,
    (CASE WHEN
    return_date IS NOT NULL THEN 1 
    ELSE 0 
    END)  AS is_return,
    to_char(rental_date::timestamp, 'YYYYMMDD')::integer as date_key,
    '{{ run_started_at.strftime ("%Y %m %d %H:%M:%S") }}'::timestamp as dbt_time 
    FROM
    {{ source('stg', 'rental') }}

),

inventory as (
    SELECT * FROM {{ source('stg', 'inventory') }}
),
-- What is the difference between ref and source?
dim_film as (
    SELECT * FROM {{ ref('dim_film') }}
),

dim_store as (
    SELECT * FROM {{ ref('dim_store') }}
),

dim_staff as (
    SELECT * FROM {{ ref('dim_staff') }}
), 

dim_customer as (
    SELECT * FROM {{ ref('dim_customer') }}
),

rental_base_1 AS (
    SELECT
        rental_base.*, 
        inventory.store_id, 
        inventory.film_id
    FROM
    rental_base

    INNER JOIN inventory ON 1=1
    AND inventory.inventory_id = rental_base.inventory_id
),

rental_base_2 as (
    SELECT
        rental_base_1.*,
        (CASE WHEN
            dim_staff.staff_id IS NOT NULL THEN dim_staff.staff_id
         ELSE -1 
         END ) AS staff_id_rental_check,
        (CASE WHEN 
            dim_customer.customer_id IS NOT NULL THEN dim_customer.customer_id
         ELSE -1
         END ) AS customer_id_check,
        (CASE WHEN
            dim_film.film_id IS NOT NULL THEN dim_film.film_id 
         ELSE - 1
         END) AS film_id_check,
        (CASE WHEN
            dim_store.store_id IS NOT NULL THEN dim_store.store_id 
         ELSE - 1
         END) AS store_id_check
    FROM 
    rental_base_1

    LEFT JOIN dim_staff ON 1=1
    AND rental_base_1.staff_id = dim_staff.staff_id

    LEFT JOIN dim_customer ON 1=1
    AND rental_base_1.customer_id = dim_customer.customer_id

    LEFT JOIN dim_film ON 1=1
    AND rental_base_1.film_id = dim_film.film_id

    LEFT JOIN dim_store ON 1=1
    AND rental_base_1.store_id = dim_store.store_id
)

SELECT 
    rental_id,
    rental_date,
    date_key,
    inventory_id,
    customer_id_check AS customer_id,
    film_id_check AS film_id,
    store_id_check AS store_id,
    staff_id_rental_check AS staff_id_rental,
    return_date, 
    CASE WHEN return_date IS NOT NULL THEN diff/3600 ELSE NULL END rental_hours,
    is_return,
    last_update,
    dbt_time
FROM
rental_base_2
WHERE 1=1

{% IF is_incremental() %}
AND update_date::timestamp > (SELECT MAX(update_date) FROM {{this}})
{% endif %}

-- INTERVAL '10 minutes' 