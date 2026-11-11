{% macro assert_bronze_ready() %}

    {#
      Validate the six Bronze source tables before
      running downstream warehouse transformations.

      This macro only reads data.
      It does not reload, truncate, or modify tables.
    #}

    {% set bronze_sources = [
        ('crm', 'crm_cust_info'),
        ('crm', 'crm_prd_info'),
        ('crm', 'crm_sales_details'),
        ('erp', 'erp_cust_az12'),
        ('erp', 'erp_loc_a101'),
        ('erp', 'erp_px_cat_g1v2')
    ] %}

    {% for source_name, table_name in bronze_sources %}

        {% set query %}
            SELECT COUNT_BIG(*) AS row_count
            FROM {{ source(source_name, table_name) }}
        {% endset %}

        {% set result = run_query(query) %}

        {% if result is none %}
            {{ exceptions.raise_compiler_error(
                'Could not check Bronze table: ' ~ table_name
            ) }}
        {% endif %}

        {% set row_count = result.columns[0].values()[0] | int %}

        {% do log(
            'Bronze ' ~ table_name ~ ': ' ~ row_count ~ ' rows',
            info=True
        ) %}

        {% if row_count <= 0 %}
            {{ exceptions.raise_compiler_error(
                'Bronze table is empty: ' ~ table_name
            ) }}
        {% endif %}

    {% endfor %}

    {% do log('All Bronze source checks passed.', info=True) %}

{% endmacro %}
