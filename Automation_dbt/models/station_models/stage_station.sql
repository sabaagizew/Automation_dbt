with source as (
  select * from {{ ref('station') }}
),

stage_station as (
  select
    station_id,
    station_flow_99,
    station_flow_max,
    station_flow_median,
    station_flow_total,
    station_n_obs
  from source
)
select
  *
from stage_station