{% macro dv_staging_filter(staging_model, primary_key_col) %}

    SELECT *
    FROM {{ ref(staging_model) }}
    WHERE {{ primary_key_col }} IS NOT NULL
      AND {{ primary_key_col }} <> -1 -- Placeholder Key Filter

{% endmacro %}