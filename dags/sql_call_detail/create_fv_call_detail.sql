CREATE TABLE IF NOT EXISTS {{params.schema_name}}.test_fv_{{params.table_name}}(
        Дата         timestamp, 
        ГОД          varchar(250),   
        КВАРТАЛ      varchar(250),
        МЕСЯЦ        varchar(250),
        ДЕНЬ         varchar(250),
        ЧАС          varchar(250),
        tenant_name  varchar(250),
        ДЗО          varchar(250),
        service_name varchar(250),
        media_type   varchar(250),
        call_type    varchar(250),
        ТИП_МЕТРИКИ  varchar(250),
        МЕТРИКА      varchar(250),
        ЧИСЛИТЕЛЬ    numeric,
        ЗНАМЕНАТЕЛЬ  numeric,
        ORDERS       varchar(250)
    )