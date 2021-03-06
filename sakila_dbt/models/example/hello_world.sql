SELECT
    customer.customer_id::int,
    customer.store_id::int,
    customer.first_name, 
    customer.last_name,
    concat(customer.first_name, ' ', customer.last_name) as full_name,
    substring(email from POSITION('@' in email)+1 for char_length(email)-POSITION('@' in email)+1) as email_address,
    customer.email,
    customer.active::int,
    customer.address_id::int,
    address.address,
    city.city_id::int,
    city.city,
    country.country_id,
    country.country,
    (case when customer.active = 0 then 'no' else 'yes' end)::varchar(100) as "active_description",
    customer.create_date::timestamp,
    customer.last_update::timestamp
FROM
    stg.customer as customer
    
    LEFT JOIN stg.address ON 1=1
    AND customer.address_id=address.address_id

    LEFT JOIN stg.city ON 1=1
    AND address.city_id = city.city_id

    LEFT JOIN stg.country ON 1=1
    AND country.country_id = city.country_id