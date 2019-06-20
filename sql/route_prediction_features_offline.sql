CREATE TABLE route_prediction_features_offline(
  qunar_username string COMMENT '用户名', 
  route_cnt int COMMENT '不同的航线数', 
  search_route string COMMENT '搜索的航线',  
  search_route_cnt int COMMENT '搜索航线的总次数', 
  search_route_single_cnt int COMMENT '单条航线搜索次数', 
  search_rate double COMMENT '搜索的每条航线占搜索总次数比例', 
  ota_route_cnt int COMMENT 'ota航线的总次数', 
  ota_route_single_cnt int COMMENT '单条航线ota次数', 
  ota_rate double COMMENT '每条航线的ota次数占总ota次数比例', 
  search_pre_days_max double COMMENT '搜索提前出发天数最大值',
  search_pre_days_min double COMMENT '搜索提前出发天数最小值',
  search_pre_days_avg double COMMENT '搜索提前出发天数平均值',
  ota_pre_days_max double COMMENT 'ota提前出发天数最大值',
  ota_pre_days_min double COMMENT 'ota提前出发天数最小值',
  ota_pre_days_avg double COMMENT 'ota提前出发天数平均值',
  dep_city_cnt int COMMENT '出发城市数', 
  arr_city_cnt int COMMENT '到达城市数', 
  has_history_route int COMMENT '该航线在历史订单中存在', 
  is_student int COMMENT '学生',
  is_trader int COMMENT '商旅'
)
COMMENT '航线预测离线特征库'
PARTITIONED BY ( 
  dt string COMMENT '分区字段，和搜索日期对应'
)
STORED AS ORC