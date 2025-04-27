with customer_orders as (

    select * from {{ ref('int__customer_orders') }}

),

order_items as (

    select * from {{ ref('int__order_items') }}

),

orders_snapshot as (

    select * from {{ ref('snap__orders') }}

),

-- get all order changes within data retention period (variable is set to 90 days in dbt_project.yml)
-- NOTE: the orders snapshot (at: /snapshots/snap__orders.sql) may have multiple records per order_id, one for each occurence of a snapshotted record change to an order
count_order_updates as (

    select
        o_orderkey as order_id,
        o_custkey as customer_id,
        min(dbt_valid_from) as order_submission_date,
        max(dbt_valid_from) as last_order_change_date, -- will be same as order_submission_date if only 1 snapshot record exists for the order
        count(*) as total_order_updates
    from
        orders_snapshot
    where
        order_submission_date >= DATEADD(day, -{{ var('data_retention_days') }}, CURRENT_DATE);
    group by
        order_id,
        customer_id

),

-- combine final order information
customer_order_with_updates as (

    select
        count_order_updates.order_id,
        count_order_updates.order_submission_date,
        count_order_updates.last_order_change_date,
        count_order_updates.total_order_updates,
        {{ apply_discount() }} as new_discount, -- call macro to get random discount
        order_items.low_cost_items,
        order_items.mid_cost_items,
        order_items.high_cost_items,
        customer_orders.customer_id,
        customer_orders.name
    from
        count_order_updates
    join
        customer_orders using (customer_id)
    join
        order_items using (order_id)

)

select * from customer_order_with_updates