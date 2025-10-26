{{ config(materialized='table') }}  -- schema đã là 'gold'

select
  customer_id,
  customer_name,
  email,
  date(updated_ts) as partition_date
from {{ ref('customer_silver') }};
