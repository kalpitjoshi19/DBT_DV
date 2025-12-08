{% macro log_all_materialized_row_counts() %}
    {% if execute %}
        
        {# Initialize the list to hold structured results before sorting #}
        {% set all_results = [] %}
        {% set summary_lines = [] %}
        
        {# --- 1. COLLECT AND PREPARE DATA --- #}

        {% for result in results | selectattr('status', 'equalto', 'success') %}
            
            {% set node = result.node %}

            {% if node.resource_type == 'model' and node.config.materialized not in ['ephemeral'] %}
                
                {% set target_db = node.database or target.database %}
                {% set target_schema = node.schema %}
                {% set target_alias = node.alias %}
                {% set target_materialization = node.config.materialized %}
                
                {# Define the primary sort rank based on the schema (Layer) #}
                {% set layer_rank = 5 %} {# Default low rank for unknown schemas #}
                {% if target_schema == 'DBT_STG' %}
                    {% set layer_rank = 1 %}
                {% elif target_schema == 'DBT_RDV' %}
                    {% set layer_rank = 2 %}
                {% elif target_schema == 'DBT_BDV' %}
                    {% set layer_rank = 3 %}
                {% elif target_schema == 'DBT_CON' %}
                    {% set layer_rank = 4 %}
                {% endif %}

                {# Define the secondary sort rank based on object type (Crucial for RDV) #}
                {% set object_sub_rank = 4 %} {# Default rank #}
                {% if target_schema == 'DBT_RDV' %}
                    {% set alias_lower = target_alias | lower %}
                    {# Use standard Python .find() == 0 for robust prefix checking #}
                    {% if alias_lower.find('hub_') == 0 %}
                        {% set object_sub_rank = 1 %}
                    {% elif alias_lower.find('sat_') == 0 %}
                        {% set object_sub_rank = 2 %}
                    {% elif alias_lower.find('link_') == 0 %}
                        {% set object_sub_rank = 3 %}
                    {% endif %}
                {% endif %}

                {# Combine ranks into a single sortable string key: Layer-SubRank-Alias #}
                {% set sort_key = layer_rank ~ "-" ~ object_sub_rank ~ "-" ~ target_alias %}

                {# Run the count query against the database #}
                {% set model_relation = api.Relation.create(database=target_db, schema=target_schema, identifier=target_alias) %}
                {% set count_query = "SELECT COUNT(*) FROM " ~ model_relation %}
                {% set count_result = run_query(count_query) %}
                {% set count_value = count_result.columns[0].values()[0] %}
                
                {# Format the output line #}
                {% set log_line = target_schema ~ "." ~ target_alias ~ " [" ~ target_materialization ~ "]: " ~ count_value %}
                
                {# Add the result with its calculated sort key to the list #}
                {% do all_results.append({'key': sort_key, 'line': log_line}) %}
                
            {% endif %}

        {% endfor %}

        {# --- 2. SORT THE RESULTS --- #}

        {# Sort the collected list of dictionaries based on the 'key' attribute #}
        {% set sorted_results = all_results | sort(attribute='key') %}


        {# --- 3. LOG THE SORTED RESULTS WITH CUSTOM SEPARATORS --- #}
        
        {# Map the combined rank (Layer-SubRank) to the required header text #}
        {% set layer_header_map = {
            '2-1': '———————— RDV HUB ———————',
            '2-2': '———————— RDV SATELLITE ———————',
            '2-3': '———————— RDV LINK ———————',
            '3-4': '———————— BDV ———————',
            '4-4': '———————— CONSUMPTION ———————'
        } %}
        
        {# Use a namespace to ensure the previous key state persists across loop iterations #}
        {% set ns = namespace(previous_key_segment='0-0') %}

        {% do summary_lines.append("=" * 50) %}
        {% do summary_lines.append(" FINAL MODEL ROW COUNT SUMMARY (Data Vault Layer Order) ") %}
        {% do summary_lines.append("=" * 50) %}

        {% for item in sorted_results %}
            {# Extract the combined layer and sub-rank segment (e.g., '1-4', '2-1') #}
            {% set key_parts = item.key.split('-') %}
            {% set current_key_segment = key_parts[0] ~ "-" ~ key_parts[1] %}

            {# Check if the grouping segment has changed #}
            {% if current_key_segment != ns.previous_key_segment %}
                
                {# Only insert a header if a mapping exists (i.e., skipping the initial STG header) #}
                {% if layer_header_map[current_key_segment] is defined %}
                    {% do summary_lines.append(layer_header_map[current_key_segment]) %}
                {% endif %}
                
                {# Update the tracking variable using the namespace #}
                {% set ns.previous_key_segment = current_key_segment %}
            {% endif %}

            {% do summary_lines.append(item.line) %}
        {% endfor %}

        {% do summary_lines.append("=" * 50) %}

        {# Log the full summary array at once #}
        {% for line in summary_lines %}
            {% do log(line, info=true) %}
        {% endfor %}

    {% endif %}
    
    {{ return('') }}
{% endmacro %}