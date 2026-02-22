{{ config(unique_key = ['id','created_date'],) }} WITH city_codes_cte AS (
    SELECT
        id AS insee_code,
        name AS city_name,
        created_date
    FROM
        {{ ref('consolidate_city') }}
    WHERE
        created_date = (
            SELECT
                max(created_date)
            FROM
                {{ ref('consolidate_city') }}
        )
        AND name IN ('nantes', 'strasbourg', 'toulouse')
)
SELECT
    source.id,
    source.code,
    source.name,
    source.city_name,
    source.address,
    source.longitude,
    source.latitude,
    source.status,
    source.created_date,
    source.capacity,
    coalesce(source.city_code, c.insee_code) AS city_code
FROM
    {{ ref('stg_station') }} AS source
    LEFT JOIN city_codes_cte AS c ON source.city_name = c.city_name {% if is_incremental() %}
WHERE
    source.created_date >= (
        SELECT
            coalesce(max(created_date), '1900-01-01')
        FROM
            {{ this }}
    ) {% endif %}