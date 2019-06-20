insert overwrite  table route_prediction_features_offline partition(dt)
select 
 t.qunar_username
 ,count(1) over(partition by t.qunar_username) as route_cnt---------航线数量-------------
 ,search_route
 ,search_route_cnt
 ,search_route_single_cnt
 ,search_route_single_cnt*1.0/search_route_cnt as search_rate
 ,ota_route_cnt
 ,ota_route_single_cnt
 ,ota_route_single_cnt*1.0/ota_route_cnt as ota_rate
 ,search_pre_days_max
 ,search_pre_days_min
 ,search_pre_days_avg
 ,if(ota_pre_days_max is null ,search_pre_days_max,ota_pre_days_max) as ota_pre_days_max
 ,if(ota_pre_days_min is null ,search_pre_days_min,ota_pre_days_min) as ota_pre_days_min
 ,if(ota_pre_days_avg is null ,search_pre_days_avg,ota_pre_days_avg) as ota_pre_days_avg
 ,dep_city_cnt----------搜索的出发城市数-----------
 ,arr_city_cnt----------搜索的到达城市数--------
 ,if(history_route.order_route is not null,1,0) as has_history_route
 ,is_student
 ,is_trader
 ,'$QDATE(-1,'yyyyMMdd')' as dt
from(
    select
        qunar_username
		,max(dep_city_cnt) as dep_city_cnt
		,max(arr_city_cnt) as arr_city_cnt
        ,search_route
        ,max(search_route_cnt) as search_route_cnt
        ,count(if(process='list',1,NULL)) as search_route_single_cnt
        ,max(ota_route_cnt) as ota_route_cnt
        ,count(if(process='ota',1,NULL)) as ota_route_single_cnt
		,max(if(process='list',pre_days,NULL)) as search_pre_days_max
        ,min(if(process='list',pre_days,NULL)) as search_pre_days_min
        ,avg(if(process='list',pre_days,NULL)) as search_pre_days_avg
        ,max(if(process='ota',pre_days,NULL)) as ota_pre_days_max
        ,min(if(process='ota',pre_days,NULL)) as ota_pre_days_min
        ,avg(if(process='ota',pre_days,NULL)) as ota_pre_days_avg		
    from 
    (
    select 
     log.qunar_username as qunar_username
     ,log.pre_days
     ,log.search_route
     ,count(if(process='list',1,NULL)) over(partition by log.qunar_username) as search_route_cnt
     ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username) as ota_route_cnt
	 ,size(collect_set(dep_city) over(partition by log.qunar_username)) as dep_city_cnt----------搜索的出发城市数-----------
     ,size(collect_set(arr_city) over(partition by log.qunar_username)) as arr_city_cnt----------搜索的到达城市数--------
     ,process
    from
          
        (select 
          username as qunar_username
          ,concat(dep_city, '-', arr_city) as search_route
		  ,dep_city
		  ,arr_city
          ,pre_days
          ,'list' as process
         from f_analysis.user_search2list_behavior
            where dt between '$QDATE(-2,'yyyyMMdd')' and '$QDATE(-1,'yyyyMMdd')' and username is not  null and  username not in ('','NULL','null')
         
         union all--合并两个结果集并且允许重复的值
         
         select 
          username as qunar_username
          ,concat(dep_city, '-', arr_city) as search_route
		  ,dep_city
		  ,arr_city
          ,pre_days
          ,'ota' as process
         from f_analysis.user_search2ota_behavior
            where dt between '$QDATE(-2,'yyyyMMdd')' and '$QDATE(-1,'yyyyMMdd')' and username is not  null and  username not in ('','NULL','null')

        ) log
		left outer join
		(select temp1.qunar_username as qunar_username
            from
           (select qunar_username
            from f_wide.wide_order where pay_ok=1 and dt>='$QDATE(-2,'yyyyMMdd')' and dt<='$QDATE(-1,'yyyyMMdd')' and qunar_username is not null and qunar_username not in ('','NULL','null')
            group by qunar_username
           ) temp1
        ) t
		on log.qunar_username=t.qunar_username 
		where t.qunar_username is null
    ) t
    group by qunar_username,search_route
)t 
left join 
(select qunar_username
        ,concat(dep_city, '-', arr_city)  order_route
        from f_wide.wide_order where pay_ok=1 and dt>='$QDATE(-731,'yyyyMMdd')' and dt<'$QDATE(-2,'yyyyMMdd')' and qunar_username is not null and  qunar_username not in ('','NULL','null')
        group by qunar_username,concat(dep_city, '-', arr_city)
) history_route
on t.qunar_username=history_route.qunar_username and t.search_route=history_route.order_route
left join 
(select
     key  
    ,max(if(tag='is_trader',1,0)) as is_trader
    ,max(if(tag='is_student',1,0)) as is_student
    from
    user.wide_user_tag
    where tag in('is_trader','is_student')
    group by key  
) tag
on t.qunar_username=tag.key
distribute by dt