-- Configurations: https://docs.getdbt.com/reference/resource-configs/snowflake-configs
{{
    config(
        materialized='table'
    )
}}

with orders as (

    select * from {{ ref('snap__orders') }} -- references the orders snapshot

),

renamed as (

    select
        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderstatus as status,
        o_totalprice as total_price,
        o_orderdate as order_date,
        SPLIT(o_orderpriority, '-')[0]::int as priority_int, -- parses "1" off of "1-URGENT" and casts to int
        SPLIT(o_orderpriority, '-')[1]::string as priority_str -- parses "URGENT" off of "1-URGENT"
    from
        orders
    where
        dbt_valid_to is null -- filter out expired records

)

select * from renamed