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
 ,if(array_contains(order_route_distinct,search_route),1,0) as label  --search_route是否在order_route_distinct集合中
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
        ,max(order_route_distinct) as order_route_distinct
		
    from 
    (
    select 
     t.qunar_username
     ,t.order_route
     ,log.pre_days
     ,log.search_route
     ,count(if(process='list',1,NULL)) over(partition by t.qunar_username) as search_route_cnt
     ,count(if(process='ota',1,NULL)) over(partition by t.qunar_username) as ota_route_cnt
	 ,size(collect_set(dep_city) over(partition by t.qunar_username)) as dep_city_cnt----------搜索的出发城市数-----------
     ,size(collect_set(arr_city) over(partition by t.qunar_username)) as arr_city_cnt----------搜索的到达城市数--------
     ,t.order_route_distinct
     ,process
    from
        (select temp1.qunar_username as qunar_username
            ,temp1.order_route as order_route
            ,temp1.create_time_max as create_time_max
            ,temp1.order_route_distinct as order_route_distinct
            from
           (select qunar_username, concat(dep_city, '-', arr_city) as order_route
            ,max(create_time)  as create_time_max
            ,collect_set(concat(dep_city, '-', arr_city)) over(partition by qunar_username) as order_route_distinct
            from f_wide.wide_order where pay_ok=1 and dt='20190420' and qunar_username is not null and  qunar_username not in ('','NULL','null')
            group by qunar_username,concat(dep_city, '-', arr_city)
            having count(1)<20
           ) temp1
        left outer join
           (select qunar_username
            from f_wide.wide_order where pay_ok=1 and dt between '20190418' and '20190419' and qunar_username is not null and  qunar_username not in ('','NULL','null')
            group by qunar_username
            ) temp2
        on (temp1.qunar_username = temp2.qunar_username) where temp2.qunar_username is null
        ) t 
        inner join 
        (select 
          username as qunar_username
          ,concat(dep_city, '-', arr_city) as search_route
		  ,dep_city
		  ,arr_city
          ,pre_days
          ,concat(search_date,' ',search_time) as log_time
          ,'list' as process
         from f_analysis.user_search2list_behavior
            where dt between '20190418' and '20190419' and username is not  null and  username not in ('','NULL','null')
         
         union all--合并两个结果集并且允许重复的值
         
         select 
          username as qunar_username
          ,concat(dep_city, '-', arr_city) as search_route
		  ,dep_city
		  ,arr_city
          ,pre_days
          ,concat(search_date,' ',search_time) as log_time
          ,'ota' as process
         from f_analysis.user_search2ota_behavior
            where dt between '20190418' and '20190419' and username is not  null and  username not in ('','NULL','null')

        ) log
        on t.qunar_username=log.qunar_username
        where log.log_time<=t.create_time_max 
    ) t
    group by qunar_username,search_route
)t 
left join 
(select qunar_username
        ,concat(dep_city, '-', arr_city)  order_route
        from f_wide.wide_order where pay_ok=1 and dt>='20170418' and dt<='20190419' and qunar_username is not null and  qunar_username not in ('','NULL','null')
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

