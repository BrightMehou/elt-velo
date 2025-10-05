{{
  config(
    unique_key = ['id','created_date'],
  )
}}
with city_codes_cte as (
        select
            id as insee_code,
            name as city_name,
            created_date
        from
            {{ ref('consolidate_city') }}
        where
            created_date = (
                select
                    max(created_date)
                from
                    {{ ref('consolidate_city') }}
            )
            and
            name in ('nantes', 'strasbourg', 'toulouse')
    )
select
   source.id,
   source.code,
   source.name,
   source.city_name,
   coalesce(source.city_code, c.insee_code) as city_code,
   source.address,
   source.longitude,
   source.latitude,
   source.status,
   source.created_date,
   source.capacity,
from
     {{ ref('stg_station') }} as source
left join city_codes_cte c on source.city_name = c.city_name

{% if is_incremental() %}

where source.created_date >= (select coalesce(max(created_date),'1900-01-01') from {{ this }} )

{% endif %}