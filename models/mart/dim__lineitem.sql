with order_items as (

    select * from {{ ref('int__order_items') }}

),

lineitem as (

    select * from {{ ref('stg__lineitem') }}

),

-- combine final customer information
lineitem_info as (

    select
        order_items.order_id || '-' || lineitem.line_number as order_line_number_id, -- build composite key with order + line number
        order_items.total_line_amount,
        lineitem.ship_mode,
        lineitem.receipt_date
    from
        order_items
    join
        lineitem using (order_id)

)

select * from lineitem_info