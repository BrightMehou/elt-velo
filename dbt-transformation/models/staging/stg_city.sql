SELECT
    (ROW ->> 'population') :: integer AS NB_INHABITANTS,
    ROW ->> 'code' AS ID,
    lower(ROW ->> 'nom') AS NAME,
    current_date AS CREATED_DATE
FROM
    {{ source('postgres', 'staging_raw') }},
    jsonb_array_elements(DATA) AS ROW
WHERE
    NOM = 'commune_data.json'
    AND DATE = current_date