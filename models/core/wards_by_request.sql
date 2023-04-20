{{ config(materialized="table") }}

with
    tcount as (
        select ward_name ward, service_request_type, count(1) type_count
        from {{ ref("stg_service_calls") }}
        group by ward_name, service_request_type
        having ward_name is not null
    ),
    t_rank as (
        select
            ward,
            service_request_type,
            type_count,
            row_number() over (partition by service_request_type order by type_count desc) type_rank,
            type_count / sum(type_count) over (partition by service_request_type) percentage
        from tcount
        where type_count > 100
    )
select service_request_type, ward, type_count, type_rank, round(percentage, 3) percentage
from t_rank
where type_rank < 4
order by service_request_type, type_rank
