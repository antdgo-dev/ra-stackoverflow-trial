{% macro get_opportunity_tags(n_answers, n_tags, l_limit, r_limit) %}

{%- call statement('opportunity_tags', fetch_result = True) -%}

with

    x_lowest_ratio_aq_tags_over_y_answers as (

        select
            tag,
            sum(questions) as n_questions,
            sum(answers) as n_answers,
            round(sum(answers) / sum(questions), 2) as ratio_aq

        from {{ ref('tbl_totals_by_tag') }}

        group by tag
        having n_answers > {{ n_answers }}

        order by ratio_aq

        limit {{ n_tags }}

    )

select tag
from x_lowest_ratio_aq_tags_over_y_answers
where n_answers between {{ l_limit }} and {{ r_limit }}

{%- endcall -%}

{%- set statement_data = load_result('opportunity_tags')['data'] -%}

{%- set extract_values = [] -%}

{%- for column_values in statement_data -%}
{% do extract_values.append( column_values[0] ) %}
{%- endfor %}

{%- set distinct_values = extract_values | unique | list | sort -%}

{{ return( distinct_values ) }}

{% endmacro %}
