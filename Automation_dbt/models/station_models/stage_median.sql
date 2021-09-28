with source as (
  select
    *
  from {{ ref('median') }}
),
stage_median as (
  select
    med,
    median_name
  from source
)
select
  *
from stage_median