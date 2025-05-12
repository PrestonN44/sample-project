-- NOTE: because this model uses the format_currency macro at /macros/format_currency.sql, which then references the 
--       usd_currency_conversion seed at /seeds/usd_currency_conversion.csv,
--       we need to manually let dbt know that in some way this model references that seed so that it can detect the dependency -
--       otherwise there will be an error on build. Do this with the "depends_on" comment below:

-- depends_on: {{ ref('usd_currency_conversion') }}

with customer as (

    select * from {{ ref('stg__customer') }}

),

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

        -- get amount in default configured currency, after multiplying by conversion factor (macro at: /macros/format_currency.sql)
        {{ format_currency('customer_orders.sum_orders_cost', var('default_currency_type')) }} as sum_orders_cost
    from
        customer
    join
        customer_orders using (customer_id)

)

select * from customer_info_final