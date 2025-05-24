with part as (

    select * from {{ ref('snap__part') }}

),

renamed as (

    select
        p_partkey as part_id,
        initcap(p_name) as name,
        p_mfgr as manufacturer,
        p_brand as brand,
        initcap(p_type) as type,
        p_size as size,
        p_retailprice as retail_price,
        trim(p_comment) as comment
    from
        part
    where
        dbt_valid_to IS NULL

)

select * from renamed