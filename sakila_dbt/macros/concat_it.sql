{% macro concat_it(col_a, col_b) %}
     concat({{ col_a }}, '-', {{ col_b }})
{% endmacro %}