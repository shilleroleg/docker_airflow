select
	date_trunc('month', START_TIME) as dt_m, 
	TENANT_SERVICE_NAME as CLIENT, 
	TENANT_NAME_NEW as TENANT, 
	SERVICE_NAME_NEW as SERVICE, 
	MEDIA_TYPE as MEDIA_TYPE, 
	_CALL_TYPE_ as CALL_TYPE
from 
	{{params.schema_name}}.temp_{{params.table_name}} 
where 
	_CALL_TYPE_ in ('in','out')
group by 
	date_trunc('month', START_TIME), 
	TENANT_SERVICE_NAME, 
	TENANT_NAME_NEW, 
	SERVICE_NAME_NEW, 
	MEDIA_TYPE, 
	CALL_TYPE
order by dt_m, CLIENT, TENANT, SERVICE, MEDIA_TYPE, CALL_TYPE;
