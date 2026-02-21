{{ config(
  unique_key = ['station_id', 'created_date'],
) }}
SELECT
  id AS station_id,
  bicycle_docks_available,
  bicycle_available,
  last_statement_date :: TIMESTAMP AS last_statement_date,
  created_date
FROM
  {{ ref('stg_station_statement') }} {% if is_incremental() %}
WHERE
  created_date >= (
    SELECT
      COALESCE(MAX(created_date), '1900-01-01')
    FROM
      {{ this }}
  ) {% endif %}