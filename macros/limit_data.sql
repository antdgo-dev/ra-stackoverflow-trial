{% macro limit_data (column_name, start_date, end_date) %}

    {% if target.name == 'default' %}
    where {{ column_name }} >= cast( '{{ start_date }}' as timestamp ) and {{ column_name }} < cast( '{{ end_date }}' as timestamp )
    {% endif %}

{% endmacro %}