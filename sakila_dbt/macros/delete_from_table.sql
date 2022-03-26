{% macro delete_from_table(table) %}

{% set query %}
delete from examples.{{ table }}
{% endset %}

{% do run_query(query) %}

{% endmacro %}