{% test non_negative(model, column_name) %}

    -- test passes if no rows are returned
    select 
        *
    from 
        {{ model }}
    where 
        {{ column_name }} < 0

{% endtest %}
