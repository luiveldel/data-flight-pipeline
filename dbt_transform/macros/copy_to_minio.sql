{% macro copy_to_minio(target_table, column_names, s3_path) %}
    {% set sql %}
        create or replace table {{ target_table }} as
        select
            {% for column_name, column_type in column_names %}
                {{ column_name }}::{{ column_type }} as {{ column_name }}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        from read_parquet('{{ s3_path }}/*.parquet');
    {% endset %}

    {% do run_query(sql) %}

{%- endmacro %}
