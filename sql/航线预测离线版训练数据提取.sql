--sudo -uflightdev /home/q/big_hive/apache-hive-1.0.0-bin/bin/hive -e"
set mapred.reduce.slowstart.completed.maps=0.9;
set mapred.reduce.tasks=500;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select
    tt.qunar_username
    ,count(1) over(partition by tt.qunar_username) as route_cnt
    ,search_route--后缀 _7days
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
    ,dep_city_cnt
    ,arr_city_cnt
    ,if(history_route.order_route is not null,1,0) as has_history_route
    ,is_student
    ,is_trader
    ,label
    from
    (
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
        ,max(dt) as dt
        ,max(label) as label
        from
        (
        select
            log.qunar_username
            ,log.pre_days
            ,log.search_route
            ,count(if(process='list',1,NULL)) over(partition by log.qunar_username) as search_route_cnt
            ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username) as ota_route_cnt
	        ,size(collect_set(dep_city) over(partition by log.qunar_username)) as dep_city_cnt
            ,size(collect_set(arr_city) over(partition by log.qunar_username)) as arr_city_cnt
            ,process
            ,log.dt
            ,case when wide.create_time between date_add(log.search_date,1) and date_add(log.search_date,7) then 1 else 0 end as label --date_sub() 只适用于2018-01-01类型的日期
            from
            (
            select --样本list/ota的人
                username as qunar_username
                ,concat(dep_city, '-', arr_city) as search_route
	    	    ,dep_city
	    	    ,arr_city
                ,pre_days
                ,search_date
                ,'list' as process
                ,dt
                from f_analysis.user_search2list_behavior
                where dt between '20190421' and '20190427' and username is not  null and  username not in ('','NULL','null') --27样本/28打标
            union all--合并两个结果集并且允许重复的值
            select
                username as qunar_username
                ,concat(dep_city, '-', arr_city) as search_route
	    	    ,dep_city
	    	    ,arr_city
                ,pre_days
                ,search_date
                ,'ota' as process
                ,dt
                from f_analysis.user_search2ota_behavior
                where dt between '20190421' and '20190427' and username is not  null and  username not in ('','NULL','null')
            ) log
            inner join
            (
            select
                cityname_zh
                ,countryname_zh as dep_country
                from dim.dim_airport_region
            ) c1
            on log.dep_city=c1.cityname_zh
            inner join
            (
            select
                cityname_zh
                ,countryname_zh as arr_country
                from dim.dim_airport_region
            ) c2
            on log.arr_city=c2.cityname_zh
            left join  -- 取 T 未来7天下单的用户为正样本
            (
            select
                ta.qunar_username
                ,tb.dep_city as order_dep_city
                ,tb.arr_city as order_arr_city
                ,concat(tb.dep_city, '-', tb.arr_city) as order_route
                ,to_date(ta.create_time) as create_time
                ,ta.dt
                from f_wide.wide_order ta inner join f_wide.wide_flight_segment tb
                on ta.order_no = tb.order_no
                and ta.dt = tb.dt
                where ta.pay_ok=1 and ta.dt between '20190428' and '20190504' and ta.qunar_username is not null and ta.qunar_username not in ('','NULL','null')
                and tb.dt between '20190428' and '20190504'
            ) wide
            on wide.qunar_username=log.qunar_username and wide.order_dep_city = log.dep_city and wide.order_arr_city = log.arr_city
            where (dep_country!='中国' or arr_country!='中国')
        ) t1
        group by qunar_username,search_route
    ) tt
    left join
  (
    select 
        t.qunar_username
        ,concat(s.dep_city, '-', s.arr_city) as order_route
        from 
        (
        select * 
            from f_wide.wide_order
            where dt between '20170421' and '20190421' and pay_ok=1 and qunar_username is not null and qunar_username not in ('','NULL','null')
        )  t
        inner join 
        (
        select * 
            from f_wide.wide_flight_segment 
            where dt between '20170421' and '20190421'
        ) s
        on t.dt = s.dt and t.order_no = s.order_no
        where s.dep_city is not null  and s.arr_city is not null 
        group by t.qunar_username,concat(s.dep_city, '-', s.arr_city)
    ) history_route
    on tt.qunar_username=history_route.qunar_username and tt.search_route=history_route.order_route
    left join
    (select
        key
        ,max(if(tag='is_trader',1,NULL)) as is_trader
        ,max(if(tag='is_student',1,NULL)) as is_student
        ,max(dt) as dt
        from
        user.wide_user_tag_history
        where dt = '20190427' and tag in('is_trader','is_student')
        group by key
    ) tag
    on tt.qunar_username=tag.key and tt.dt = tag.dt
    " > /home/q/tmp/wenjin.li/tmp/inter_user_route_prediction_offline_data_20190421.csv