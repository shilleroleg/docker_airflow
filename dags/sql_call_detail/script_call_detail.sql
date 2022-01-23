with
--=== фильтр по дате ====
date_start as(
select date_trunc('day', TIMESTAMP '01.10.2021') as b_date, 
date_trunc('day', TIMESTAMP '12.01.2022') as e_date
),
call_detail as(
select start_time as dt_d 
 , CASE 
          WHEN to_char(start_time, 'MM')='01' THEN 'ЯНВАРЬ' 
          WHEN to_char(start_time, 'MM')='02' THEN 'ФЕВРАЛЬ'           
          WHEN to_char(start_time, 'MM')='03' THEN 'МАРТ' 
          WHEN to_char(start_time, 'MM')='04' THEN 'АПРЕЛЬ'           
          WHEN to_char(start_time, 'MM')='05' THEN 'МАЙ'           
          WHEN to_char(start_time, 'MM')='06' THEN 'ИЮНЬ'           
          WHEN to_char(start_time, 'MM')='07' THEN 'ИЮЛЬ'           
          WHEN to_char(start_time, 'MM')='08' THEN 'АВГУСТ'          
          WHEN to_char(start_time, 'MM')='09' THEN 'СЕНТЯБРЬ'           
          WHEN to_char(start_time, 'MM')='10' THEN 'ОКТЯБРЬ'           
          WHEN to_char(start_time, 'MM')='11' THEN 'НОЯБРЬ'
          ELSE 'ДЕКАБРЬ' END AS MONTHH
, (hold_time + pending_time + talk_time + acw_time) as service_time
      , (case when COALESCE(disposition,'UNDEFINED') not in ('CALLBACK_REQUESTED','CALLER_TERMINATED','CALLEE_TERMINATED','ABANDONED','SYSTEM_DISCONNECTED','ABANDONED_RINGING','ABANDONED_QUEUE','CLOSED_WITHOUT_REPLY','SENT','UNDEFINED','REPLIED','NO_ANSWER','TRANSFERRED','CONFERENCED','CALLER_TRANSFERRED','CALLED_PARTY_BUSY','FORWARDED','NETWORK_BUSY','SELF_SERVICE') then 1 else 0 end) as fnd
      , (case when COALESCE(disposition,'UNDEFINED') = 'CALLBACK_REQUESTED' then 1 else 0 end) as fl_callback
      , (case when COALESCE(disposition,'UNDEFINED') = 'CONFERENCED' then 1 else 0 end) as flag_conferenc
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLER_TERMINATED','CALLEE_TERMINATED','ABANDONED','SYSTEM_DISCONNECTED','ABANDONED_RINGING','ABANDONED_QUEUE','CLOSED_WITHOUT_REPLY','SENT','UNDEFINED','REPLIED','NO_ANSWER','TRANSFERRED','CALLER_TRANSFERRED','CALLED_PARTY_BUSY','FORWARDED','NETWORK_BUSY','SELF_SERVICE') then 1 else 0 end) as call_all
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLEE_TERMINATED','CALLER_TERMINATED','CALLER_TRANSFERRED','REPLIED','SENT','TRANSFERRED','FORWARDED') then 1 else 0 end) as flag_answer
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and COALESCE(cd.media_type,'0') <> 'EMAIL' and talk_time > 0 then 1 else 0 end) as flag_with_talk
      , (case when COALESCE(disposition,'UNDEFINED') = 'SELF_SERVICE' then 1 else 0 end) as flag_answer_ivr
      , (case when COALESCE(disposition,'UNDEFINED') = 'CLOSED_WITHOUT_REPLY' then 1 else 0 end) as flag_not_reply
      , (case when COALESCE(disposition,'UNDEFINED') = 'ABANDONED' then 1 else 0 end) as flag_lost_ivr
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and pending_time = 0 and queue_time = 0 then 1 else 0 end) as fntnrnq
      , (case when COALESCE(disposition,'UNDEFINED') = 'ABANDONED_QUEUE' then 1 else 0 end) as flag_lost_que
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and pending_time = 0 and queue_time > 0 then 1 else 0 end) as fntnrwq
      , (case when COALESCE(disposition,'UNDEFINED') in ('ABANDONED_RINGING', 'NO_ANSWER') then 1 else 0 end) as flag_lost_ring 
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and pending_time > 0 then 1 else 0 end) as fntwr
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLED_PARTY_BUSY', 'NETWORK_BUSY') then 1 else 0 end) as flag_not_reach
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 then 1 else 0 end) as flag_no_talk
      , (case when COALESCE(disposition,'UNDEFINED') in ('ABANDONED_QUEUE', 'ABANDONED_RINGING','NO_ANSWER','CALLED_PARTY_BUSY','NETWORK_BUSY') and (queue_time + pending_time) < 5 then 1 else 0 end) as flqs
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') and talk_time = 0 and (queue_time + pending_time) > 0 and (queue_time + pending_time) < 5 then 1 else 0 end) as fntwqs
      , (case when COALESCE(disposition,'UNDEFINED') in ('CALLER_TRANSFERRED','TRANSFERRED','FORWARDED') then 1 else 0 end) as flag_transfer
      , (case when COALESCE(disposition,'UNDEFINED') in ('ABANDONED','SYSTEM_DISCONNECTED','UNDEFINED') and CALLER_LOGIN_ID is null and call_type = 'out' and talk_time = 0 then 1 else 0 end) as flag_out_autodial
      , (case when CALLER_LOGIN_ID is not null and call_type = 'out' then 1 else 0 end) as fomc
      , (case when COALESCE(disposition,'UNDEFINED') in ('SYSTEM_DISCONNECTED','UNDEFINED') then 1 else 0 end) as flag_disconnect
      , (case when custom1 is null then 0 else 1 end) as flag_subject
      , (case when lead(start_time,1) over(partition by from_phone, custom1 order by start_time) <= start_time + interval '24 hours' then 0 else 1 end) as flag_fcr          
,cd.*
from pkap_247_sch.call_detail cd
   where call_type in ('in', 'out')
	  and TENANT_SERVICE_NAME not in ('НЛМК', 'Остров Мечты')
    and TENANT_SERVICE_NAME in ('Деловая Среда','СберСтрахование')
      and date_trunc('day', start_time) between (select b_date from date_start) and (select e_date from date_start)
),
 metriks as (
      select cd.dt_d 
        , cd.MONTHH
        , cd.TENANT_NAME_NEW as TENANT_NAME
        , cd.TENANT_SERVICE_NAME 
        , cd.service_name_new as SERVICE_NAME
        , cd.media_type
        , cd.call_type
        , sum(cd.fnd) as fnd
        , sum(cd.fl_callback) as fl_callback
        , sum(cd.flag_conferenc) as flag_conferenc
        , count(distinct(case when DISPOSITION = 'CONFERENCED' then GLOBAL_INTERACTION_ID end)) as flag_conferenc_unic
        , sum(cd.call_all) as call_all
        , sum(case when(cd.flag_answer + cd.flag_with_talk) = 0 then 0 else 1 end) as flag_answer
        , sum(flag_answer_ivr) as flag_answer_ivr
        , sum(flag_not_reply) as flag_not_reply
        , sum(case when (cd.flag_lost_ivr + cd.flag_lost_que + cd.flag_lost_ring + flag_not_reach + cd.flag_no_talk) = 0 then 0 else 1 end) as flag_lost
        , sum(case when (cd.flag_lost_ivr + cd.fntnrnq) = 0 then 0 else 1 end) as flag_lost_ivr
        , sum(case when (cd.flag_lost_que + cd.fntnrwq) = 0 then 0 else 1 end) as flag_lost_que
        , sum(case when (cd.flag_lost_ring + cd.fntwr) = 0 then 0 else 1 end) as flag_lost_ring
        , sum(flag_not_reach) as flag_not_reach
        , sum(case when (cd.flqs + cd.fntwqs) = 0 then 0 else 1 end) as flqs
        , sum(cd.flag_transfer) as flag_transfer
        , sum(cd.flag_out_autodial) as flag_out_autodial
        , sum(cd.fomc) as fomc
        , sum(flag_disconnect) as flag_disconnect
        , sum(cd.queue_time) as queue_time
        , sum(cd.pending_time) as pending_time
        , sum(cd.talk_time) as talk_time
        , sum(cd.hold_time) as hold_time
        , sum(cd.acw_time) as acw_time
        , sum(cd.duration) as duration
        , sum(cd.service_time) as service_time --HT
--        , sum(case when (cd.flag_answer + cd.flag_with_talk) >= 1  and (cd.queue_time + cd.pending_time) <= dcod.SL_SEC then 1 else 0 end) as flag_sl
        , sum(case when cd.call_type = 'in' and cd.flag_subject = 1 then cd.flag_fcr else 0 end) as flag_fcr
        , sum(case when cd.call_type = 'in' and cd.flag_subject = 1 and cd.flag_answer = 1 then 1 else 0 end) as flag_fcr_znam
--        , sum(dcod.SL_PERC) as SL_PERC_CHISL
--        , sum(dcod.LCR_PERC) as LCR_PERC_CHISL
--        , sum(dcod.AHT_SEC) as AHT_SEC_CHISL
--        , count(dcod.SL_PERC) as SL_PERC_ZNAM
--        , count(dcod.LCR_PERC) as LCR_PERC_ZNAM
--        , count(dcod.AHT_SEC) as AHT_SEC_ZNAM    
        from call_detail cd  
        group by 
         cd.dt_d
        , cd.MONTHH
        , cd.TENANT_NAME_NEW
        , cd.TENANT_SERVICE_NAME
        , cd.service_name_new 
        , cd.media_type
        , cd.call_type    
  ) 
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'СЕРВИСНАЯ МЕТРИКА' as "ТИП МЕТРИКИ" 
        , case 
            when mt.call_type = 'in' then 'SL'  
            ELSE 'NONAME' END AS "МЕТРИКА"
        , CAST(NULL AS bigint) as "ЧИСЛИТЕЛЬ" 
        , sum(mt.CALL_ALL - mt.FLAG_LOST_IVR- mt.FLAG_ANSWER_IVR - mt.flqs) as "ЗНАМЕНАТЕЛЬ"       
        , '1' as "ORDER" 
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD')  
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'СЕРВИСНАЯ МЕТРИКА' as "ТИП МЕТРИКИ" 
        , case 
            when mt.call_type = 'in' then 'SL_PLAN'  
            ELSE 'NONAME' END AS "МЕТРИКА"
        , CAST(NULL AS bigint) as "ЧИСЛИТЕЛЬ" 
        , CAST(NULL AS bigint) as "ЗНАМЕНАТЕЛЬ"       
        , '2' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD') 
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'СЕРВИСНАЯ МЕТРИКА' as "ТИП МЕТРИКИ" 
        , case 
            when mt.call_type = 'in' and mt.media_type not in ('EMAIL') then 'LCR'  
            ELSE 'NONAME' END AS "МЕТРИКА" 
        , sum (mt.FLAG_LOST-mt.flqs-mt.FLAG_LOST_IVR ) as "ЧИСЛИТЕЛЬ" 
        , sum(mt.CALL_ALL-mt.flqs-mt.FLAG_LOST_IVR-mt.FLAG_ANSWER_IVR) as "ЗНАМЕНАТЕЛЬ"       
        , '3' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD')
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'СЕРВИСНАЯ МЕТРИКА' as "ТИП МЕТРИКИ" 
        , null AS "МЕТРИКА"
        , CAST(NULL AS bigint) as "ЧИСЛИТЕЛЬ" 
        , CAST(NULL AS bigint) as "ЗНАМЕНАТЕЛЬ"       
        , '4' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD') 
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'СЕРВИСНАЯ МЕТРИКА' as "ТИП МЕТРИКИ" 
        , case 
            when mt.call_type = 'in' and mt.media_type = 'VOICE' then 'FCR' 
            ELSE 'NONAME' END AS "МЕТРИКА"
        , CAST(NULL AS bigint) as "ЧИСЛИТЕЛЬ" 
        , CAST(NULL AS bigint) as "ЗНАМЕНАТЕЛЬ"       
        , '5' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD')
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'ТАЙМИНГ' as "ТИП МЕТРИКИ" 
        , case 
            when mt.media_type = 'VOICE' then 'AHT'
            when mt.call_type = 'in' and mt.media_type in ('EMAIL','CHAT') then 'AHT'
            ELSE 'NONAME' END AS "МЕТРИКА" 
        , sum (mt.TALK_TIME+mt.PENDING_TIME+mt.HOLD_TIME+mt.ACW_TIME) as "ЧИСЛИТЕЛЬ" 
        , sum(mt.FLAG_ANSWER) as "ЗНАМЕНАТЕЛЬ"       
        , '6' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD')      
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'ТАЙМИНГ' as "ТИП МЕТРИКИ" 
        , case 
            when mt.media_type = 'VOICE' then 'AHT_PLAN'
            when mt.call_type = 'in' and mt.media_type in ('EMAIL','CHAT') then 'AHT_PLAN'
            ELSE 'NONAME' END AS "МЕТРИКА"
        , CAST(NULL AS bigint) as "ЧИСЛИТЕЛЬ" 
        , CAST(NULL AS bigint) as "ЗНАМЕНАТЕЛЬ"       
        , '7' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD')             
UNION
--==================================================================
 select 
         mt.dt_d as "Дата"        
        , TO_CHAR (mt.dt_d,'YYYY') AS "ГОД"
        , TO_CHAR (mt.dt_d,'Q') AS "КВАРТАЛ"
        , mt.MONTHH as "МЕСЯЦ"
        , TO_CHAR (mt.dt_d,'DD') AS "ДЕНЬ"
        , TO_CHAR (mt.dt_d,'HH24') AS "ЧАС"
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME as "ДЗО"
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , 'ТАЙМИНГ' as "ТИП МЕТРИКИ" 
        , case 
            when mt.call_type = 'in' and mt.media_type = 'VOICE' then 'AWT'
            ELSE 'NONAME' END AS "МЕТРИКА"
        , sum (mt.QUEUE_TIME+mt.PENDING_TIME) as "ЧИСЛИТЕЛЬ" 
        , sum(mt.CALL_ALL-mt.FLAG_LOST_IVR-mt.FLAG_ANSWER_IVR) as "ЗНАМЕНАТЕЛЬ"       
        , '8' as "ORDER"
from  metriks mt
GROUP BY 
         mt.dt_d       
        , TO_CHAR (mt.dt_d,'YYYY') 
        , TO_CHAR (mt.dt_d,'Q')
        , mt.MONTHH 
        , TO_CHAR (mt.dt_d,'HH24') 
        , mt.TENANT_NAME 
        , mt.TENANT_SERVICE_NAME 
        , mt.SERVICE_NAME
        , mt.media_type
        , mt.call_type
        , TO_CHAR (mt.dt_d,'DD')