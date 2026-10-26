{#
  Runs at the start of every dbt command (see on-run-start in dbt_project.yml).
  Exposes the raw Parquet files as a view, raw.flights, so that:
    - models read it like any table via source('raw', 'flights')
    - dbt unit tests can look up its columns to build fake input rows
    - you can query raw data by name in DuckDB
  The path is relative to the repo root, where dbt, the dashboard and the ML project run.
  DELTA_RAW_DIR overrides it, so tests and CI can point dbt at a small sample.
#}
{% macro create_raw_views() %}
    create schema if not exists raw;
    create or replace view raw.flights as
    select * from read_parquet('{{ env_var("DELTA_RAW_DIR", "data/raw") }}/flights/*/*.parquet', hive_partitioning = true);
{% endmacro %}
