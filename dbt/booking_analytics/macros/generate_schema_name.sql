{#
  Écrit dans le dataset nommé tel quel (ex. staging_booking), au lieu du
  comportement par défaut « <dataset cible>_<schema> ». Les datasets sont
  fixes et seront gérés par Terraform (jour 26). Contrepartie : pas
  d'isolation par développeur. Voir ADR-030.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
