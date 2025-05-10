with lineitem as (

    select * from {{ ref('stg__lineitem') }}

),

-- combine final customer information
lineitem_info as (

    select
        order_id || '-' || line_number as order_line_number_id, -- build composite key with order + line number to use as unique key
        order_id,
        ship_mode,
        receipt_date,
        quantity,

        -- get amount in configured currency, after multiplying by conversion factor (macro at: /macros/format_currency.sql)
        {{ format_currency('extended_price', var('default_currency_type')) }} as extended_price
    from
        lineitem

)

select * from lineitem_info