{% test not_less_than_zero (model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} < 0

{% endtest %}