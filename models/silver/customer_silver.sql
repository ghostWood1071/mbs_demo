{{ config(materialized='table') }}  -- schema đã là 'silver' từ dbt_project.yml

with src as (
  select
    cast(id as int)  as customer_id,
    trim(name)       as customer_name,
    lower(email)     as email,
    cast(updated_timestamp as timestamp) as updated_ts
  from {{ source('bronze', 'customer_raw') }}
)
select * from src;
