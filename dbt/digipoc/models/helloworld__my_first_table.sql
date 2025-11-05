{{ config(materialized='view') }}

select *
from helloworld.my_first_table