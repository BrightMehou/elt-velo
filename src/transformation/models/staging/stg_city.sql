select
    code as id,
    lower(nom) as name,
    population as nb_inhabitants,
    current_date() as created_date
from
    read_json(
        's3://{{ var("BUCKET_NAME") }}/{{ run_started_at.strftime("%Y-%m-%d") }}/commune_data.json'
    )