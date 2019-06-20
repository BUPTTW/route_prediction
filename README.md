## **用户航线预测**:

执行项目根目录下的`predict.sh`进行全量用户的航线预测的处理过程：
注：所有的模型训练过程都在jupyter上进行

#### **1. 获取待预测数据**

```shell
   sh ./sql/extract_predict_data.sh ${update_date} ${label_date}
```   

#### **2. 加载文件**

加载待预测数据文件<br>

```python
    data= pd.read_csv('/home/q/tmp/f_algorithm_model/flight_growth/dataset/user_route_pred_data_${label_date}.csv',sep='\t')
```

#### **3. 数据预处理**

将缺失值全部替换为0,主要是用户画像分类的缺失数据<br/>

```python
    df = df.fillna(0)
	X_val = df.drop(['qunar_username', 'search_route','dt'], axis=1)
```
#### **4. 用户航线预测**
##### 待预测的数据放在dataset目录下,名为user_route_pred_data_${label_date}.csv，其中label_date为待预测数据的日期，模型保存在model目录下，名为route_prediction_xgboost_v1.2.model<br>
```python
    model = load_model(model_file)
	y_pred = model_validate(X_val, model)
```
#### **5. 结果导入hive库**
   最后将结果保存到hive库中的route_prediction_offline_result表中<br/>
```shell
    sh ./sql/insert_to_hive.sh ${update_date} ${label_date}
```
##常用SQL
* 删除分区 
-- 删除20190613的分区
alter table f_analysis.result_user_route_prediction drop partition(dt='20190613');
* 查询预测结果
select *  from f_analysis.result_user_route_prediction limit 5;
* 手工导入数据
load data local inpath
     '/home/q/guofang.li/urp_result.csv'
     overwrite into table f_analysis.result_user_route_prediction
     partition(dt = '20190614');
* 删除表
drop table f_analysis.result_user_route_prediction;
* 建表，注意分隔符
CREATE TABLE f_analysis.result_user_route_prediction(
  qunar_username string COMMENT '用户名', 
  search_route string COMMENT '搜索的航线',  
  q_ratio string COMMENT '航线得分'  
)
COMMENT '航线预测离线版本结果'
PARTITIONED BY ( 
  dt string COMMENT '分区字段，yyyymmdd和搜索日期对应'
)
row format delimited fields terminated by ','
lines terminated by '\n'
tblproperties('skip.header.line.count'='1');

* 出发日期预测
---
-- 删除表
drop table f_analysis.result_user_depdate_prediction;
-- 建表，注意分隔符
CREATE TABLE f_analysis.result_user_depdate_prediction(
  qunar_username string COMMENT '用户名', 
  dep_date string COMMENT '搜索的出发日期',  
  q_ratio string COMMENT '预测得分'
)
COMMENT '出发日期预测离线版本结果'
PARTITIONED BY ( 
  dt string COMMENT '分区字段，yyyymmdd和搜索日期对应'
)
row format delimited fields terminated by ','
lines terminated by '\n'
tblproperties('skip.header.line.count'='1')

---
