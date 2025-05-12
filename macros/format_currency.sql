-- More information about dbt macros: https://docs.getdbt.com/docs/build/jinja-macros

{% macro format_currency(amount_column, currency=var('default_currency_type')) %}

    {% if execute %}

        -- create and run query to pull factor for currency from seed (at: /seeds/usd_currency_conversion.csv)
        {% set currency_query %}
            select top 1 factor
            from {{ ref('usd_currency_conversion') }}
            where currency = '{{ currency }}'
        {% endset %}

        {% set result = run_query(currency_query) %}
        {% set factor = result.columns[0].values()[0] %}

        {% if currency == 'USD' or currency == 'CAD'  %} -- US/Canadian Dollar symbol
            '$' 
        {% elif currency == 'EUR' %} -- Euro symbol
            '€'
        {% elif currency == 'JPY' or currency == 'CNY' %} -- Yen/Yuan symbol
            '¥'
        {% else %}
            NULL
        {% endif %}

        -- add converted amount after symbol
        || LTRIM(TO_CHAR(round({{ amount_column }} * {{ factor }}, 2), '999,999,999,999,999.00'))

    {% endif %}

{% endmacro %}
