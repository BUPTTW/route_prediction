#!/bin/sh
set -e
source /home/q/scripts/utils.sh $@

basepath=$(cd `dirname $0`; pwd)

hive_user=${hive_user}
update_date=$2
label_date=$3
filename=user_route_pred_data_${label_date}
echo ${update_date}, ${filename}
base_dir="/home/q/tmp/f_algorithm_model/flight_growth"

outsql="
select
qunar_username
,route_cnt
,search_route
,search_route_cnt
,search_route_single_cnt
,search_rate
,ota_route_cnt
,ota_route_single_cnt
,ota_rate
,search_pre_days_max
,search_pre_days_min
,search_pre_days_avg
,ota_pre_days_max
,ota_pre_days_min
,ota_pre_days_avg
,dep_city_cnt
,arr_city_cnt
,has_history_route
,is_student
,is_trader
,dt
from f_analysis.route_prediction_features_offline
where dt='${update_date}'
"
echo ${outsql}

sudo -uflightdev /home/q/apache-hive-1.2.1-bin/bin/hive -e"
set mapred.reduce.slowstart.completed.maps=0.9;
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;
${outsql}">${base_dir}/dataset/${filename}.csv











