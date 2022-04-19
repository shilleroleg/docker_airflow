delete from {{params.schema_name}}.{{params.mart_name}}
where date_trunc('day', "dt_d") >= date_trunc('day', current_timestamp at time zone 'UTC-07') - INTERVAL '{{day_count}} DAY'