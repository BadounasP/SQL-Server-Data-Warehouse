{% macro sync_product_keys() %}

    {% if execute %}

        {% do run_query(
            "EXEC dbt_dev.sync_product_key_map;"
        ) %}

        {% do log(
            "Product key synchronization completed.",
            info=True
        ) %}

    {% endif %}

{% endmacro %}
