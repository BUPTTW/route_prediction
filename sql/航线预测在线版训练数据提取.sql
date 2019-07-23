--sudo -uflightdev /home/q/big_hive/apache-hive-1.0.0-bin/bin/hive -e"
--set mapred.reduce.slowstart.completed.maps=0.9;
--set hive.cli.print.header=true;
--set hive.resultset.use.unique.column.names=false;
--set hive.cli.print.header=true;
select 
    t.qunar_username
    ,log_time
    ,count(1) over(partition by t.qunar_username,log_time) as route_search_cnt_60minute
    ,search_route
    ,dep_city_hot
    ,arr_city_hot
    ,search_cnt_60minute
    ,current_route_search_cnt_60minute
    ,if(search_cnt_60minute=0,-1,current_route_search_cnt_60minute*1.0/search_cnt_60minute) as current_route_search_rate_60minute
    ,ota_cnt_60minute
    ,current_route_ota_cnt_60minute
    ,if(ota_cnt_60minute=0,-1,current_route_ota_cnt_60minute*1.0/ota_cnt_60minute) as current_route_ota_rate_60minute
    ,current_route_search_pre_days_max_60minute
    ,current_route_search_pre_days_min_60minute
    ,current_route_search_pre_days_avg_60minute
    ,current_route_search_pre_days_max_60minute-current_route_search_pre_days_min_60minute as current_route_search_dep_date_span_60minute
    ,if(current_route_ota_pre_days_max_60minute is null ,0,current_route_ota_pre_days_max_60minute) as current_route_ota_pre_days_max_60minute
    ,if(current_route_ota_pre_days_min_60minute is null ,0,current_route_ota_pre_days_min_60minute) as current_route_ota_pre_days_min_60minute
    ,if(current_route_ota_pre_days_avg_60minute is null ,0,current_route_ota_pre_days_avg_60minute) as current_route_ota_pre_days_avg_60minute
    ,if(current_route_ota_pre_days_max_60minute is null ,0,current_route_ota_pre_days_max_60minute)-if(current_route_ota_pre_days_min_60minute is null ,0,current_route_ota_pre_days_min_60minute) as current_route_ota_dep_date_span_60minute
    ,dep_city_cnt_60minute
    ,arr_city_cnt_60minute
    ,search_cnt_60minute*1.0/dep_city_cnt_60minute as search_depcity_rate_60minute
    ,search_cnt_60minute*1.0/arr_city_cnt_60minute as search_arrcity_rate_60minute
    ,search_cnt_60minute*1.0/(count(1) over(partition by t.qunar_username,log_time)) as search_airline_rate_60minute
    ,current_route_search_cnt_60minute*1.0/(count(1) over(partition by t.qunar_username,log_time)) as current_route_search_airline_rate_60minute
    ,ota_cnt_60minute*1.0/search_cnt_60minute as ota_search_rate_60minute
    ,ota_cnt_60minute*1.0/(count(1) over(partition by t.qunar_username,log_time)) as ota_airline_rate_60minute
    ,current_route_ota_cnt_60minute*1.0/current_route_search_cnt_60minute as current_route_ota_search_rate_60minute
    ,if(history_route.order_route is not null,1,0) as has_history_route
    ,is_student
    ,is_trader
    ,dom_hf_personal
    ,inter_hf_personal
    ,user_continuous_active_days
    ,label
    from 
    (
    select
        qunar_username
        ,log_time
        ,search_route
        ,max(dep_city_cnt) as dep_city_cnt_60minute
        ,max(arr_city_cnt) as arr_city_cnt_60minute
        ,max(dep_city_hot) as dep_city_hot
        ,max(arr_city_hot) as arr_city_hot
        ,max(search_cnt) as search_cnt_60minute
        ,count(if(process='list',1,NULL)) as current_route_search_cnt_60minute
        ,max(ota_cnt) as ota_cnt_60minute
        ,count(if(process='ota',1,NULL)) as current_route_ota_cnt_60minute
        ,max(if(process='list',pre_days,NULL)) as current_route_search_pre_days_max_60minute
        ,min(if(process='list',pre_days,NULL)) as current_route_search_pre_days_min_60minute
        ,avg(if(process='list',pre_days,NULL)) as current_route_search_pre_days_avg_60minute
        ,max(if(process='ota',pre_days,NULL)) as current_route_ota_pre_days_max_60minute
        ,min(if(process='ota',pre_days,NULL)) as current_route_ota_pre_days_min_60minute
        ,avg(if(process='ota',pre_days,NULL)) as current_route_ota_pre_days_avg_60minute
        --,max(order_route_distinct) as order_route_distinct
        ,max(label) as label
        from
        (
        select
            log.qunar_username as qunar_username
            ,log.search_route as search_route
            ,log.log_time as log_time
            ,process
            ,log.pre_days as pre_days
            ,count(if(process='list',1,NULL)) over(partition by log.qunar_username,log.log_time) as search_cnt --按用户分组
            ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username,log.log_time) as ota_cnt
            ,size(collect_set(dep_city) over(partition by log.qunar_username,log.log_time)) as dep_city_cnt
            ,size(collect_set(arr_city) over(partition by log.qunar_username,log.log_time)) as arr_city_cnt
            ,if(dep_city in ('澳门','南通','绵阳','西双版纳','香港','无锡','揭阳','拉萨','桂林','丽江','烟台','西宁','泉州','宁波','合肥','石家庄',
                             '银川','南昌''长春','温州','太原','三亚','福州','珠海','呼和浩特','沈阳','兰州','济南','南宁','大连','哈尔滨','天津',
                             '贵阳','武汉','青岛','厦门','长沙','海口','南京','郑州','乌鲁木齐','杭州','广州','重庆','西安','昆明','成都','深圳','北京','上海'),1,0) as dep_city_hot
            ,if(arr_city in ('澳门','南通','绵阳','西双版纳','香港','无锡','揭阳','拉萨','桂林','丽江','烟台','西宁','泉州','宁波','合肥','石家庄',
                             '银川','南昌''长春','温州','太原','三亚','福州','珠海','呼和浩特','沈阳','兰州','济南','南宁','大连','哈尔滨','天津',
                             '贵阳','武汉','青岛','厦门','长沙','海口','南京','郑州','乌鲁木齐','杭州','广州','重庆','西安','昆明','成都','深圳','北京','上海'),1,0) as arr_city_hot
            --,wide2.order_route_distinct as order_route_distinct
            ,case when log.log_time between wide2.create_time-7*24*60*60*1000 and wide2.create_time then 1 else 0 end as label
            ,case when (wide1.create_time>=(log.log_time-2*24*60*60*1000) and wide1.create_time<log.log_time) then 1 else 0 end as filter_order_2days
            from
            (
            select
                qunar_username
                ,dep_city
                ,arr_city
                ,search_route
                ,log_time
                ,process
                ,pre_days
                ,cnt
                ,cnt_all
                from
                (
                select
                    log4.qunar_username as qunar_username
                    ,log4.dep_city as dep_city
                    ,log4.arr_city as arr_city
                    ,log4.search_route as search_route
                    ,log3.log_time as log_time
                    ,log4.process as process
                    ,log4.pre_days as pre_days
                    ,count(if(log4.process in ('list','ota'),1,NULL)) over (partition by log4.qunar_username,log3.log_time) as cnt
                    ,count(1) over (partition by log4.qunar_username,log3.log_time) as cnt_all
                    from
                    (
                    select
                        qunar_username
                        ,dep_city
                        ,arr_city
                        ,search_route
                        ,log_time
                        ,lead_log_time
                        ,process
                        ,pre_days
                        from
                        (
                        select
                            qunar_username
                            ,dep_city
                            ,arr_city
                            ,search_route
                            ,log_time
                            ,lead(log_time,1) over (partition by qunar_username order by log_time) as lead_log_time
                            ,process
                            ,datediff(p_dep_date,'1970-01-01')-day as pre_days
                            from
                            (
                            select
                                p_qunar_username as qunar_username
                                ,p_dep_city as dep_city
                                ,p_arr_city as arr_city
                                ,concat(p_dep_city,'-',p_arr_city) as search_route
                                ,cast(time as bigint) as log_time
                                ,p_process as process
                                ,day
                                ,p_dep_date
                                from qlibra.flight_server_log
                                where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and p_process=p_subprocess and dt between '20190615' and '20190618'  
                            ) log1
                        ) log2
                        where lead_log_time - log_time>10*60*1000 or lead_log_time is null and process in ('list','ota')
                    ) log3
                    left join
                    (
                    select
                        p_qunar_username as qunar_username
                        ,p_dep_city as dep_city
                        ,p_arr_city as arr_city
                        ,concat(p_dep_city,'-',p_arr_city) as search_route
                        ,cast(time as bigint) as log_time
                        ,p_process as process
                        ,day
                        ,p_dep_date
                        ,datediff(p_dep_date,'1970-01-01')-day as pre_days
                        from qlibra.flight_server_log
                        where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and p_process=p_subprocess and dt between '20190615' and '20190618'
                    ) log4
                    on log3.qunar_username=log4.qunar_username
                    where log4.log_time < log3.log_time and log4.log_time >= log3.log_time-60*60*1000
                ) log5
                --where cnt=cnt_all
            ) log
            left join  --  去掉 T 时刻两天前之内有下单的用户
            (
            select
                qunar_username
                ,unix_timestamp(create_time)*1000 as create_time
                from f_wide.wide_order
                where pay_ok=1 and dt between '20190613' and '20190618' and qunar_username is not null and qunar_username not in ('','NULL','null')
            ) wide1
            on log.qunar_username=wide1.qunar_username
            left join  -- 取 T 未来7天下单的用户为正样本
            (
            select
                qunar_username
                ,concat(dep_city, '-', arr_city) as order_route
                ,unix_timestamp(create_time)*1000 as create_time
                --,collect_set(concat(dep_city, '-', arr_city)) over(partition by qunar_username) as order_route_distinct
                from f_wide.wide_order
                where pay_ok=1 and dt between '20190617' and '20190630' and qunar_username is not null and qunar_username not in ('','NULL','null')
                --group by qunar_username,concat(dep_city, '-', arr_city)
                --having count(1)<20 --排除下单超过20单的人
            ) wide2
            on wide2.qunar_username=log.qunar_username and wide2.order_route=log.search_route
            --where ((wide1.create_time_max>=(log.log_time-2*24*60*60*1000) and wide1.create_time_max<log.log_time) or wide1.qunar_username is null)
            --where log.log_time between wide.create_time_max-2*24*60*60*1000) and wide.create_time_max
        ) tlog
        group by qunar_username,search_route,log_time
        having (max(filter_order_2days)=0 and sum(filter_order_2days)<20)
    ) t
    left join 
    (
    select 
        qunar_username
        ,concat(dep_city, '-', arr_city) order_route
        from f_wide.wide_order 
        where pay_ok=1 and dt between '20170521' and '20190520' and qunar_username is not null and  qunar_username not in ('','NULL','null')
        group by qunar_username,concat(dep_city, '-', arr_city)
    ) history_route
    on t.qunar_username=history_route.qunar_username and t.search_route=history_route.order_route
    left join
    (
    select
        key  
        ,max(if(tag='is_trader',1,0)) as is_trader
        ,max(if(tag='is_student',1,0)) as is_student
        ,max(if(tag='dom_hf_personal',1,0)) as dom_hf_personal
        ,max(if(tag='inter_hf_personal',1,0)) as inter_hf_personal
        ,max(if(tag='user_continuous_active_days',cast(value as bigint),-1)) as user_continuous_active_days
        from
        user.wide_user_tag
        where tag in('is_trader','is_student','dom_hf_personal','inter_hf_personal','user_continuous_active_days')
        group by key  
    ) tag
    on t.qunar_username=tag.key;
" > /home/q/tmp/wenjin.li/tmp/user_route_prediction_online_new_feature_data_20190623.csv