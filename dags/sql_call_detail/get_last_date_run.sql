select 
	distinct ti.day_count,
	si.id
FROM 
	{{params.log_schema_name}}.tables_info ti
	left join {{params.log_schema_name}}.scripts_and_tables sat 
		on ti.id = sat.id_table
	left join {{params.log_schema_name}}.scripts_info si 
		on si.id = sat.id_script
WHERE ti.table_name = '{{params.table_name}}'
	and si.script_name = '{{params.script_name}}'
	and ti.last_date_update > si.last_date_run