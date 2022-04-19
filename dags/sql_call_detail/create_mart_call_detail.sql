CREATE TABLE IF NOT EXISTS {{params.schema_name}}.{{params.mart_name}}(
		dt_d 			timestamp,
		dt_n_h 			numeric,
		client  		varchar(250),
		tenant  		varchar(250),
		service  		varchar(250),
		media_type  	varchar(250),
		call_type  		varchar(250),
		metric_order 	numeric,
		metric_type 	varchar(250),
		metric_name 	varchar(250),
		chisl  			numeric,
		znam			numeric
    )