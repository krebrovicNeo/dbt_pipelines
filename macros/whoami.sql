-- macros/whoami.sql
{% macro whoami() %}
  {% set res = run_query("select current_user, current_database, current_schema, current_setting('search_path')") %}
  {{ log("user=" ~ res.columns[0].values()[0] ~
         ", db=" ~ res.columns[1].values()[0] ~
         ", schema=" ~ (res.columns[2].values()[0] or 'NULL') ~
         ", search_path=" ~ res.columns[3].values()[0], info=True) }}
{% endmacro %}