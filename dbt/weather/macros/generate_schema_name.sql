{#-
  Use the schema configured on the model as-is (staging, marts) instead of
  dbt's default of prefixing it with the target schema. Keeps relation names
  readable: staging.stg_weather_daily, marts.mart_city_weather_daily.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
