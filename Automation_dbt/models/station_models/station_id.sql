with station as (
  select
    *
  from {{ ref('stg_station') }}
),
median as (
  select
    *
  from {{ ref('stg_median') }}
),
final as (
  select
    station.flow_99,
    median.median_id,
    case
      median.median_weekday
      when 'delivered' then 1
      else 0

      from median
      inner join station on median.station_flow_99 = station.station_id
      )
  select
    *
  from final