with customer as (

    select * from {{ ref('stg__customer') }}

),--testing

customer_orders as (

    select * from {{ ref('int__customer_orders') }}

),

-- combine final customer information
customer_info_final as (

    select
        customer.customer_id,
        customer.name,
        customer.address,
        customer.phone,
        customer.account_balance,
        customer_orders.num_total_orders,
        customer_orders.num_payed_orders,
        customer_orders.num_open_orders,
        customer_orders.num_fulfilled_orders,
        customer_orders.sum_orders_cost
    from
        customer
    join
        customer_orders using (customer_id)

)

select * from customer_info_final