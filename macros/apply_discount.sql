-- More information about dbt macros: https://docs.getdbt.com/docs/build/jinja-macros

{% macro apply_discount() %}

    {% set discount_amount = [0.15, 0.10, 0.05, 0] %}

    {{ return(discount_amount | random) }}

{% endmacro %}