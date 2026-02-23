{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set folder = node.original_file_path | lower -%}

    {# If the model is in the staging folder, use staging schema #}
    {%- if '/staging/' in folder -%}
        staging

    {# If the model is in the facts or dimensions folder, use analytics schema #}
    {%- elif '/facts/' in folder or '/dimensions/' in folder -%}
        analytics

    {# Otherwise, use target schema #}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}

{%- endmacro %}
