with rental_base as (
SELECT 
    *,
    EXTRACT(EPOCH from rental_date::timestamp) as rental_epoch,
    EXTRACT(EPOCH from return_date::timestamp) as return_epoch,
    EXTRACT(EPOCH from return_date::timestamp)    

)