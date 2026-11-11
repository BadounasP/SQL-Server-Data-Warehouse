{% macro sync_customer_keys() %}

    {% if execute %}

        {% do run_query(
            "EXEC dbt_dev.sync_customer_key_map;"
        ) %}

        {% do log(
            "Customer key synchronization completed.",
            info=True
        ) %}

    {% endif %}

{% endmacro %}
