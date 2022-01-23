select 
	distinct date_trunc('day', tl.last_time) as last_time,
	tl.id as id
FROM 
	{{params.schema_name}}.table_log tl
-- 	pkap_247_sch.table_log tl
WHERE 
	date_trunc('day', tl.last_time) = CURRENT_DATE - 1