-- Configurations: https://docs.getdbt.com/reference/resource-configs/snowflake-configs
{{
    config(
        materialized='table'
    )
}}

with customer as (

    select * from {{ ref('stg__customer') }}

),

orders as (

    select * from {{ ref('stg__orders') }}

),

customer_orders as (

    select
        customer.customer_id,
        customer.name,
        count(orders.order_id) as num_total_orders,
        count(case when orders.status = 'P' then 1 end) as num_payed_orders,
        count(case when orders.status = 'O' then 1 end) as num_open_orders,
        count(case when orders.status = 'F' then 1 end) as num_fulfilled_orders,
        sum(orders.total_price) as sum_orders_cost
    from
        customer
    join
        orders using (customer_id)
    group by
        customer.customer_id,
        customer.name

)

select * from customer_orders