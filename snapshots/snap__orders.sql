{% snapshot snap__orders %}

    {{
        config(
            unique_key='o_orderkey',
            strategy='check',
            check_cols=['o_orderstatus', 'o_orderpriority', 'o_comment'],
        )
    }}

    select * from {{ source('snowflake_sample_shop', 'orders') }}

{% endsnapshot %}