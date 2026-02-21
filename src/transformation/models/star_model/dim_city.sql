{{ config(unique_key = 'id',) }}
SELECT
  id,
  name,
  nb_inhabitants
FROM
  {{ ref('consolidate_city') }} {% if is_incremental() %}
WHERE
  created_date = (
    SELECT
      MAX(created_date)
    FROM
      {{ ref('consolidate_city') }}
  ) {% endif %}