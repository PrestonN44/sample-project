with orders as (

    select * from {{ ref('stg__orders') }}

),

lineitem as (

    select * from {{ ref('stg__lineitem') }}

),

-- set variables
{% set lower_amount = 10000 %}
{% set upper_amount = 500000 %}
{% set lineitem_amount_expression = 'quantity * extended_price' %}

order_items as (

    select
        orders.order_id,
        '$' || sum({{ lineitem_amount_expression }}) as total_line_amount,
        count(case when {{ lineitem_amount_expression }} <= {{ lower_amount }} then quantity end) as low_cost_items,
        count(case when {{ lineitem_amount_expression }} between {{ lower_amount }} and {{ upper_amount }} then quantity end) as mid_cost_items,
        count(case when {{ lineitem_amount_expression }} >= {{ upper_amount }} then quantity end) as high_cost_items,
    from
        orders
    join
        lineitem using (order_id)
    group by
        orders.order_id

)

select * from order_items