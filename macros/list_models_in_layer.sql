-- More information about dbt macros: https://docs.getdbt.com/docs/build/jinja-macros

{% macro list_models_in_layer(layer) %}

    -- create and run query to pull layer models from information_schema
    {% set models_query %}

        select 
            UPPER(table_name) as model, 
            INITCAP(table_type) as object, 
            comment as description
        from 
            {{ target.database }}.INFORMATION_SCHEMA.TABLES
        where 
            -- target.schema is "dbt_[personal schema]"
            UPPER(table_schema) = UPPER('{{ target.schema ~ '_' ~ layer}}')
        order by 
            table_name

    {% endset %}
    {% set result = run_query(models_query) %}

    {{ log('*** Displaying ' ~ layer ~ ' models ***', info=True) }}

    -- loop through each row returned and log the model's name, object type, and its description
    {% for row in result %}
        {{ log(row.MODEL ~ '\t|\t' ~ row.OBJECT ~ '\t|\t' ~ row.DESCRIPTION, info=True) }}
    {% endfor %}

{% endmacro %}
