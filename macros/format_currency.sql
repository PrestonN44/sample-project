-- More information about dbt macros: https://docs.getdbt.com/docs/build/jinja-macros

{% macro format_currency(amount, currency=var('default_currency_type')) %}

    {% if execute %}
        -- create and run query to pull factor for currency from seed (at: /seeds/usd_currency_conversion.csv)
        {% set currency_query %}
            select top 1 factor
            from {{ ref('usd_currency_conversion') }}
            where currency = {{ currency }}
        {% endset %}

        {% set result = run_query(currency_query) %}

        {% set factor = result.columns[0].values()[0] %}

        {% set converted_amount = (amount * factor) %}

        {% if currency == 'USD' %}
            ('$' || round({{ converted_amount }}, 2))
        {% endif %}

        ('zzz' || round({{ converted_amount }}, 2))
    {% endif %}

{% endmacro %}
