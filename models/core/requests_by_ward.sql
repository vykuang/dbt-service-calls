{{ config(materialized="table") }}

with
    w_count as (
        select ward_name, service_request_type, count(1) as ward_count,
        from {{ ref("stg_service_calls") }}
        where ward_name is not null
        group by ward_name, service_request_type
    ),
    w_rank as (
        select
            ward_name,
            service_request_type,
            ward_count,
            row_number() over (
                partition by ward_name order by ward_count desc
            ) ward_rank,
            ward_count / sum(ward_count) over (partition by ward_name) percentage
        from w_count
    )
select ward_name, service_request_type, ward_count, round(percentage, 3)
from w_rank
where ward_rank < 4
order by ward_name, ward_rank
