-- A simple literal value
{% macro simple_macro() %}
'A simple macro'
{% endmacro %}

-- Another in the same file
{% macro hidden_macro() %}
'a hidden macro'
{% endmacro %}