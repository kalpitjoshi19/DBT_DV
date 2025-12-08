{% macro log_row_count(model_relation) %}
  
  {# 
     Guard clause: Ensure this only runs during actual execution (dbt run), not compilation.
     This fixed the previous "'None' has no attribute 'table'" error.
  #}
  {% if execute %}
    
    {% set count_query %}
      SELECT COUNT(*) FROM {{ model_relation }}
    {% endset %}

    {# Execute the count query #}
    {% set results = run_query(count_query) %}

    {# Extract the count value from the result set #}
    {% set count_value = results.columns[0].values()[0] %}

    {# 
       FIX: Switched from '{{ dbt.log(...) }}' to '{% do log(..., info=true) %}' 
       to resolve the "'dict object' has no attribute 'log'" error.
       This uses the stable, global 'log' function for console output.
    #}
    {% do log("Row Count for " ~ model_relation.identifier ~ " (at " ~ target.name ~ "): " ~ count_value, info=true) %}
    
  {% endif %}
  
  {# Return an empty string to ensure nothing is injected into the final SQL #}
  {{ return('') }}

{% endmacro %}