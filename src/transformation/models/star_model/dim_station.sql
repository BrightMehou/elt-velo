{{ config(unique_key = 'id',) }}
SELECT
  id,
  code,
  name,
  address,
  longitude,
  latitude,
  STATUS,
  capacity
FROM
  {{ ref('consolidate_station') }} {% if is_incremental() %}
WHERE
  created_date = (
    SELECT
      max(created_date)
    FROM
      {{ ref('consolidate_station') }}
  ) {% endif %}