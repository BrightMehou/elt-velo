select
    id,
    name,
    nb_inhabitants
from {{ ref('consolidate_city') }}
    WHERE created_date = (select MAX(created_date) from {{ ref('consolidate_city') }})