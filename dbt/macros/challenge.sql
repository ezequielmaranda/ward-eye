{#- Extrae una métrica de challenges_json (columna `c`, tipo json) y la tipa.
    Los conteos pasan por double porque Riot a veces los serializa como 3.0. -#}
{% macro challenge(key, type='integer') -%}
    {%- if type == 'integer' -%}
        (c ->> '{{ key }}')::double::integer
    {%- else -%}
        (c ->> '{{ key }}')::{{ type }}
    {%- endif -%}
{%- endmacro %}
