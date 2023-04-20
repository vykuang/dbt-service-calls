{{ config(materialized="table") }}
with
    season_type_count as (
        select
            case
                when extract(month from creation_datetime) < 3 or extract(month from creation_datetime) > 11
                then 'winter'
                when extract(month from creation_datetime) < 6
                then 'spring'
                when extract(month from creation_datetime) < 9
                then 'summer'
                else 'fall'
            end season,
            service_request_type,
            count(1) request_count
        from {{ ref("stg_service_calls") }}
        group by season, service_request_type
    ),
    season_rank_table as (
        select
            season,
            service_request_type,
            request_count,
            row_number() over (
                partition by season order by request_count desc
            ) seasonal_rank
        from season_type_count
    )
select *
from season_rank_table
where seasonal_rank < 4
order by season, seasonal_rank
