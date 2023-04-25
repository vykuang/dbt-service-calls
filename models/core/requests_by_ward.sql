{{ config(materialized="table", cluster_by="ward_name") }}

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
select
    w.ward_name,
    service_request_type,
    ward_count,
    ward_rank,
    round(percentage, 3) as percentage,
    map.geometry
from w_rank as w
join {{ ref("stg_city_wards") }} map on w.ward_name = map.ward_name
order by ward_name, ward_rank
