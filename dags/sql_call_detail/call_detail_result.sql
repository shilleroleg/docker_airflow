with call_detail_temp as(
select
        dts.CLIENT
      , dts.TENANT
      , dts.SERVICE
      , case when CALLER_PHONE_TYPE = 'EXTERNAL' and CALLEE_PHONE_TYPE = 'INTERNAL' then 'in'
              when CALLER_PHONE_TYPE = 'INTERNAL' and CALLEE_PHONE_TYPE = 'EXTERNAL' then 'out'
              when CALLER_PHONE_TYPE = 'EXTERNAL' and CALLEE_PHONE_TYPE = 'EXTERNAL' then 'external'
              when CALLER_PHONE_TYPE = 'INTERNAL' and CALLEE_PHONE_TYPE = 'INTERNAL' then 'internal'
              when CALLER_PHONE_TYPE is null and CALLEE_PHONE_TYPE = 'INTERNAL' then 'internal'
              when CALLER_PHONE_TYPE is null and CALLEE_PHONE_TYPE = 'EXTERNAL' then 'out'
              when CALLER_PHONE_TYPE = 'INTERNAL' and CALLEE_PHONE_TYPE is null then 'internal'
              when CALLER_PHONE_TYPE = 'EXTERNAL' and CALLEE_PHONE_TYPE is null then 'in'
              else 'other' end as CALL_TYPE
      , cd.START_TIME + interval '3 hour' as START_TIME_MSK
      , cd.START_TIME + interval '3 hour' + (duration || 'SECOND')::INTERVAL as END_TIME_MSK
      , cd.*
      , dts.FLAG_MOTIV
      , dts.FLAG_MONITORING
      , dts.FLAG_ACT
      , dts.ORG_NAME
      , dts.COVENANT
      , dts.INN
      , dts.KPP
      from {{params.schema_name}}.temp_{{params.table_name}} cd
      left join (select bad_o.*
                , case when DATE_END is null then to_date('31.12.9999','DD.MM.YYYY') else DATE_END end as END_DATE_NEW
                from {{params.schema_name}}.bp_and_dict_old bad_o ) dts on dts.TENANT_NAME = cd.TENANT_NAME and COALESCE(dts.SERVICE_NAME,'0') = COALESCE(cd.SERVICE_NAME,'0')
                and cd.START_TIME between dts.DATE_START and dts.END_DATE_NEW
),


--========= call_detail ===========
call_detail as(
select date_trunc('hour',start_time_msk) as dt_h
      , (case when COALESCE(disposition,'UNDEFINED') not in ('CALLBACK_REQUESTED','CALLER_TERMINATED','CALLEE_TERMINATED','ABANDONED','SYSTEM_DISCONNECTED','ABANDONED_RINGING','ABANDONED_QUEUE','CLOSED_WITHOUT_REPLY','SENT','UNDEFINED','REPLIED','NO_ANSWER','TRANSFERRED','CONFERENCED','CALLER_TRANSFERRED','CALLED_PARTY_BUSY','FORWARDED','NETWORK_BUSY','SELF_SERVICE') then 1 else 0 end) as flag_new_disposition
      
      , (case when COALESCE(disposition,'UNDEFINED') = 'CALLBACK_REQUESTED' then 1 else 0 end) as flag_callback
      , (case when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' then 1 else 0 end) as flag_conferenc
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLER_TERMINATED','CALLEE_TERMINATED','ABANDONED','SYSTEM_DISCONNECTED','ABANDONED_RINGING','ABANDONED_QUEUE','CLOSED_WITHOUT_REPLY','SENT','UNDEFINED','REPLIED','NO_ANSWER','TRANSFERRED','CALLER_TRANSFERRED','CALLED_PARTY_BUSY','FORWARDED','NETWORK_BUSY','SELF_SERVICE') then 1
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' then 1
              else 0 end) as call_all
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLEE_TERMINATED','CALLER_TERMINATED','CALLER_TRANSFERRED','REPLIED','SENT','TRANSFERRED','FORWARDED') then 1 else 0 end) as flag_answer
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and COALESCE(cd.media_type,'0') <> 'EMAIL' and talk_time > 0 then 1
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' and talk_time > 0 then 1
              else 0 end) as flag_with_talk
      , (case when COALESCE(disposition,'UNDEFINED') = 'SELF_SERVICE' then 1 else 0 end) as flag_answer_ivr
      
      , (case when COALESCE(disposition,'UNDEFINED') = 'CLOSED_WITHOUT_REPLY' then 1 else 0 end) as flag_not_reply
      
      , (case when COALESCE(disposition,'UNDEFINED') = 'ABANDONED' then 1 else 0 end) as flag_lost_ivr
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and pending_time = 0 and queue_time = 0 then 1 
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' and talk_time = 0 and pending_time = 0 and queue_time = 0 then 1
              else 0 end) as flag_no_talk_no_ring_no_que
      
      , (case when COALESCE(disposition,'UNDEFINED') = 'ABANDONED_QUEUE' then 1 else 0 end) as flag_lost_que
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and pending_time = 0 and queue_time > 0 then 1 
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' and talk_time = 0 and pending_time = 0 and queue_time > 0 then 1
              else 0 end) as flag_no_talk_no_ring_with_que
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('ABANDONED_RINGING', 'NO_ANSWER') then 1 else 0 end) as flag_lost_ring 
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and pending_time > 0 then 1
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' and talk_time = 0 and pending_time > 0 then 1
              else 0 end) as flag_no_talk_with_ring
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLED_PARTY_BUSY', 'NETWORK_BUSY') then 1 else 0 end) as flag_not_reach
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 then 1 
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' and talk_time = 0 then 1
              else 0 end) as flag_no_talk
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('ABANDONED_QUEUE', 'ABANDONED_RINGING','NO_ANSWER','CALLED_PARTY_BUSY','NETWORK_BUSY') and (queue_time + pending_time) < 5 then 1 else 0 end) as flag_lost_que_short
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and (queue_time + pending_time) > 0 and (queue_time + pending_time) < 5 then 1 
              when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' and MEDIA_TYPE = 'CHAT' and client = 'Деловая Среда' and talk_time = 0 and (queue_time + pending_time) > 0 and (queue_time + pending_time) < 5 then 1
              else 0 end) as flag_no_talk_with_que_short
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLER_TRANSFERRED','TRANSFERRED','FORWARDED') then 1 else 0 end) as flag_transfer
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('ABANDONED','SYSTEM_DISCONNECTED','UNDEFINED') and CALLER_LOGIN_ID is null and call_type = 'out' and talk_time = 0 then 1 else 0 end) as flag_out_autodial
      , (case when CALLER_LOGIN_ID is not null and call_type = 'out' then 1 else 0 end) as flag_out_manual_call
      
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') then 1 else 0 end) as flag_disconnect
      
      , (case when cd.call_type = 'in' then callee_login_id
              when cd.call_type = 'out' then caller_login_id end) as operator_id
      ,cd.*
      from call_detail_temp cd
      where call_type in ('in', 'out')
--========= call_detail ===========
), 


--========= Основной запрос ===========
call_detail_agr as (select cd.dt_h
        , cd.client
        , cd.tenant
        , cd.service
        , cd.media_type
        , cd.call_type
        , sum(cd.flag_new_disposition) as flag_new_disposition
        , sum(cd.flag_callback) as flag_callback
        , sum(cd.flag_conferenc) as flag_conferenc
        , count(distinct(case when DISPOSITION = 'CONFERENCED' then GLOBAL_INTERACTION_ID end)) as flag_conferenc_unic
        , sum(cd.call_all) as call_all
        , sum(case when(cd.flag_answer + cd.flag_with_talk) = 0 then 0 else 1 end) as flag_answer
        , sum(flag_answer_ivr) as flag_answer_ivr
        , sum(flag_not_reply) as flag_not_reply
        , sum(case when (cd.flag_lost_ivr + cd.flag_lost_que + cd.flag_lost_ring + flag_not_reach + cd.flag_no_talk) = 0 then 0 else 1 end) as flag_lost
        , sum(case when (cd.flag_lost_ivr + cd.flag_no_talk_no_ring_no_que) = 0 then 0 else 1 end) as flag_lost_ivr
        , sum(case when (cd.flag_lost_que + cd.flag_no_talk_no_ring_with_que) = 0 then 0 else 1 end) as flag_lost_que
        , sum(case when (cd.flag_lost_ring + cd.flag_no_talk_with_ring) = 0 then 0 else 1 end) as flag_lost_ring
        , sum(flag_not_reach) as flag_not_reach
        , sum(case when (cd.flag_lost_que_short + cd.flag_no_talk_with_que_short) = 0 then 0 else 1 end) as flag_lost_que_short
        , sum(cd.flag_transfer) as flag_transfer
        , sum(cd.flag_out_autodial) as flag_out_autodial
        , sum(cd.flag_out_manual_call) as flag_out_manual_call
        , sum(cd.flag_disconnect) as flag_disconnect
        , sum(cd.queue_time) as queue_time
        , sum(cd.pending_time) as pending_time
        , sum(cd.talk_time) as talk_time
        , sum(cd.hold_time) as hold_time
        , sum(cd.acw_time) as acw_time
        , sum(case when (cd.flag_answer + cd.flag_with_talk) >= 1  and (cd.queue_time + cd.pending_time) <= cc_o.SL_SEC then 1 else 0 end) as flag_sl
        , sum(cc_o.SL_PERC) as SL_PERC_CHISL
        , sum(cc_o.LCR_PERC) as LCR_PERC_CHISL
        , sum(cc_o.AHT_SEC) as AHT_SEC_CHISL
        , count(cc_o.SL_PERC) as SL_PERC_ZNAM
        , count(cc_o.LCR_PERC) as LCR_PERC_ZNAM
        , count(cc_o.AHT_SEC) as AHT_SEC_ZNAM
        from call_detail cd
            left join {{params.schema_name}}.co_client_old cc_o on cc_o.client = cd.client and cc_o.call_type = cd.call_type
            and cc_o.media_type = cd.media_type and date_trunc('day',cd.start_time_msk) >= date_trunc('day',cc_o.date_start) and date_trunc('day',cd.start_time_msk) <= date_trunc('day',cc_o.date_end)
        group by cd.dt_h
        , cd.client
        , cd.tenant
        , cd.service
        , cd.media_type
        , cd.call_type
      )
--========= Основной запрос ===========


select *
from (
select 
date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 1 as metric_order
, 'СЕРВИСНАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' then 'SL,факт'
      else 'NONAME' end as metric_name
, cda.flag_sl as CHISL
, cda.call_all + cda.flag_lost_ivr + cda.flag_answer_ivr + cda.flag_lost_que_short as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 2 as metric_order
, 'СЕРВИСНАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' then 'SL,план'
      else 'NONAME' end as metric_name
, cda.SL_PERC_CHISL/100 as CHISL
, case when cda.SL_PERC_CHISL is null then null else cda.SL_PERC_ZNAM end as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 3 as metric_order
, 'СЕРВИСНАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type <> 'EMAIL' then 'LCR,факт'
      else 'NONAME' end as metric_name
, cda.flag_lost - cda.flag_lost_que_short - cda.flag_lost_ivr as CHISL
, cda.call_all - cda.flag_lost_que_short - cda.flag_lost_ivr - cda.flag_answer_ivr as ZNAM
from call_detail_agr cda 

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 4 as metric_order
, 'СЕРВИСНАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type <> 'EMAIL' then 'LCR,план'
      else 'NONAME' end as metric_name
, cda.LCR_PERC_CHISL/100 as CHISL
, case when cda.LCR_PERC_CHISL is null then null else cda.LCR_PERC_ZNAM end as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 5 as metric_order
, 'ТАЙМИНГ' as metric_type
, 'AHT,факт' as metric_name
, case when cda.media_type = 'EMAIL' then (cda.talk_time + cda.acw_time) else (cda.pending_time + cda.talk_time + cda.hold_time + cda.acw_time) end as CHISL
, cda.flag_answer as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 6 as metric_order
, 'ТАЙМИНГ' as metric_type
, 'AHT,план' as metric_name
, cda.AHT_SEC_CHISL as CHISL
, case when cda.AHT_SEC_CHISL is null then null else cda.AHT_SEC_ZNAM end as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 7 as metric_order
, 'ТАЙМИНГ' as metric_type
, case when cda.call_type = 'in' and cda.media_type in ('VOICE','CHAT') then 'AWT,факт' 
        else 'NONAME' end as metric_name
, cda.queue_time + cda.pending_time as CHISL
, cda.call_all - cda.flag_lost_ivr - cda.flag_answer_ivr as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 8 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'VOICE' then 'КОЛИЧЕСТВО ЗВОНКОВ' 
        when cda.call_type = 'out' and cda.media_type = 'VOICE' then 'КОЛИЧЕСТВО ВЫЗОВОВ' 
        when cda.call_type = 'in' and cda.media_type = 'CHAT' then 'КОЛИЧЕСТВО ЧАТОВ/SMS' 
        when cda.call_type = 'in' and cda.media_type = 'EMAIL' then 'КОЛИЧЕСТВО ВХОДЯЩИЕ' 
        when cda.call_type = 'out' and cda.media_type = 'EMAIL' then 'КОЛИЧЕСТВО ИСХОДЯЩИХ' 
        else 'NONAME' end as metric_name
, cda.call_all as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 9 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'VOICE' then 'ЗАВЕРШЕНО в IVR' 
        when cda.call_type = 'in' and cda.media_type = 'CHAT' then 'ЗАВЕРШЕНО ЧАТОВ/SMS в IVR' 
        else 'NONAME' end as metric_name
, cda.flag_lost_ivr as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 10 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'VOICE' then 'ОБРАБОТАНО в IVR' 
        when cda.call_type = 'in' and cda.media_type = 'CHAT' then 'ОБРАБОТАНО ЧАТ-БОТОМ' 
        else 'NONAME' end as metric_name
, cda.flag_answer_ivr as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 11 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'VOICE' then 'ПОСТУПИЛО ЗВОНКОВ' 
        when cda.call_type = 'in' and cda.media_type = 'CHAT' then 'ПОСТУПИЛО ЧАТОВ/SMS' 
        else 'NONAME' end as metric_name
, cda.call_all - cda.flag_lost_ivr - cda.flag_answer_ivr as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 12 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'VOICE' then 'ПРИНЯТЫЕ' 
        when cda.call_type = 'out' and cda.media_type = 'VOICE' then 'КОЛИЧЕСТВО ДОЗВОНОВ (руч.)' 
        when cda.call_type = 'in' and cda.media_type = 'CHAT' then 'ПРИНЯТЫЕ ЧАТЫ/SMS' 
        when cda.call_type = 'in' and cda.media_type = 'EMAIL' then 'ОБРАБОТАНО' 
        else 'NONAME' end as metric_name
, cda.flag_answer as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 13 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type in ('VOICE','CHAT') then 'КОЛИЧЕСТВО ПОТЕРЯННЫХ' 
        else 'NONAME' end as metric_name
, cda.flag_lost_que + cda.flag_lost_ring + cda.flag_not_reach as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 14 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type in ('VOICE','CHAT') then 'потеряны в QUEUE' 
        else 'NONAME' end as metric_name
, cda.flag_lost_que as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 15 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type in ('VOICE','CHAT') then 'потеряны в RING' 
        else 'NONAME' end as metric_name
, cda.flag_lost_ring as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 16 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type in ('VOICE','CHAT') then 'недозвоны' 
        else 'NONAME' end as metric_name
, cda.flag_not_reach as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 17 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type in ('VOICE','CHAT') then 'Количество коротких потерянных(<5 сек)' 
        else 'NONAME' end as metric_name
, cda.flag_lost_que_short as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 18 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'VOICE' then 'ЗАПРОШЕН CALLBACK' 
        else 'NONAME' end as metric_name
, cda.flag_callback as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 19 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.media_type in ('VOICE','CHAT') then 'переведено' 
        when cda.media_type = 'EMAIL' then 'перенаправлено' 
        else 'NONAME' end as metric_name
, cda.flag_transfer as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 20 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'EMAIL' then 'ЗАКРЫТО БЕЗ ОТВЕТА' 
        else 'NONAME' end as metric_name
, cda.flag_not_reply as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 21 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'in' and cda.media_type = 'EMAIL' then 'НЕ РАСПРЕДЕЛЕНО' 
        else 'NONAME' end as metric_name
, cda.flag_disconnect as CHISL
, null as ZNAM
from call_detail_agr cda

union

select date_trunc('day',cda.dt_h) as dt_d
, to_char(cda.dt_h,'HH24') as dt_n_h
, cda.client
, cda.tenant
, cda.service
, cda.media_type
, cda.call_type
, 22 as metric_order
, 'КОЛИЧЕСТВЕННАЯ МЕТРИКА' as metric_type
, case when cda.call_type = 'out' and cda.media_type = 'VOICE' then 'КОЛИЧЕСТВО АВТОДОЗВОНОВ' 
        else 'NONAME' end as metric_name
, cda.flag_out_autodial as CHISL
, null as ZNAM
from call_detail_agr cda
) itog
where METRIC_NAME <> 'NONAME'
order by DT_D, DT_N_H, CLIENT, TENANT, SERVICE, MEDIA_TYPE, CALL_TYPE, METRIC_ORDER
