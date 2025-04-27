with customer as (

    select * from {{ source('snowflake_sample_shop', 'customer') }}

),

deleted_customers as (

    select * from {{ ref('deleted_customers') }}

),

renamed as (

    select
        c_custkey as customer_id,
        INITCAP(c_name) as name, -- INITCAP converts to pascal case (joHn DOE -> John Doe)
        trim(c_address) as address,
        c_phone as phone,
        c_acctbal as account_balance,
        c_comment as comment
    from
        customer
    where 
        c_custkey not in ( -- filter out deleted customers
            select c_custkey
            from deleted_customers
        )

)

select * from renamed