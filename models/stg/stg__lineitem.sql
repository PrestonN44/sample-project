with lineitem as (

    select * from {{ source('snowflake_sample_shop', 'lineitem') }}

),

renamed as (

    select
        l_orderkey as order_id,
        l_partkey as part_id,
        l_suppkey as supp_id,
        l_linenumber as line_number,
        l_quantity::int as quantity,
        l_extendedprice as extended_price,
        l_receiptdate as receipt_date,
        l_shipmode as ship_mode
    from
        lineitem

)

select * from renamed