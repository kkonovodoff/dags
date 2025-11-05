{{ config(materialized='view') }}

select *
from airbyte.product