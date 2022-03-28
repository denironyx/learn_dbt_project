{{ config(post_hook='insert into {{this}}(film_id) VALUES (-1)') }}

WITH stg_film AS (
    SELECT 
    *,
    (CASE WHEN 
        length <= 75 THEN 'short'
    WHEN (length > 75 AND length <= 120) THEN 'medium'
    WHEN length > 120 THEN 'long'
    ELSE 'na' end) as length_desc,
    COALESCE(original_language_id, 0) AS original_language_id_zero,
    CASE WHEN
        POSITION('Trailers' IN special_features::varchar) > 0 THEN 1
        ELSE 0
    END as has_trailers,
    CASE WHEN
        POSITION('Commentaries' IN special_features::varchar) > 0 THEN 1
        ELSE 0
    END as has_commentaries,
    CASE WHEN
        POSITION('Deleted Scenes' IN special_features::varchar) > 0 THEN 1
        ELSE 0
    END as has_deleted_scenes,
    CASE WHEN
        POSITION('Behind the Scenes' IN special_features::varchar) > 0 THEN 1
        ELSE 0
    END as has_behind_the_scenes,
    '{{ run_started_at.strftime ("%Y %m %d %H:%M:%S") }}'::timestamp as dbt_time 
    FROM
    {{ source('stg', 'film') }}
),

language AS (
    SELECT * FROM 
    {{ source('stg', 'language') }}
),

category AS (
    SELECT * FROM 
    {{ source('stg', 'category') }}
),

film_category AS (
    SELECT * FROM 
    {{ source('stg', 'film_category') }}
)

, 
stg_film_1 AS (
    SELECT
        stg_film.*,
        language.name as lang_name
    FROM
    stg_film
    LEFT JOIN language on 1=1
    AND stg_film.language_id = language.language_id
)
,

stg_film_2 AS (
    SELECT 
        stg_film_1.*,
        category.category_id,
        category.name AS category_desc
    FROM
    stg_film_1

    LEFT JOIN film_category ON 1 = 1
    AND stg_film_1.film_id = film_category.film_id

    LEFT JOIN category ON 1=1
    AND film_category.category_id = category.category_id
)

SELECT
    film_id,
    title,
    description,
    release_year,
    language_id,
    lang_name,
    original_language_id_zero as original_language_id,
    rental_duration,
    rental_rate,
    length,
    length_desc,
    replacement_cost,
    rating,
    category_id,
    category_desc,
    special_features,
    has_trailers,
    has_commentaries,
    has_behind_the_scenes,
    has_deleted_scenes,
    last_update, 
    dbt_time
FROM
stg_film_2