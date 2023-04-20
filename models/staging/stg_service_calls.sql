{{
    config(
        materialized="view",
        partition_by={
            "field": "creation_datetime",
            "data_type": "timestamp",
            "granularity": "day",
        },
    )
}}
-- dbt_utils updated surrogate_key to generate_surrogate_key
select
    {{
        dbt_utils.generate_surrogate_key(
            ["creation_datetime", "ward_id", "Service_Request_Type"]
        )
    }} as request_id, 
    *
from {{ source("staging", "facts_2022_partitioned") }}
where
    ward_id is not null
    and creation_datetime is not null
    and service_request_type is not null
    
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 1000 {% endif %}