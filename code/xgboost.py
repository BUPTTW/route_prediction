from __future__ import division
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns
import time;
import datetime;
import warnings
from sklearn.utils import shuffle
from sklearn import linear_model, datasets
from sklearn import preprocessing
from sklearn import cross_validation, metrics
from sklearn.cross_validation import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import StratifiedKFold
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, accuracy_score,roc_auc_score
import xgboost as xgb
import pickle
import itertools
warnings.filterwarnings(action='ignore', category=UserWarning, module='matplotlib')
pd.options.mode.chained_assignment = None  # default='warn'
plt.style.use('seaborn-whitegrid')# 设置图形的显示风格


def model_train(train_x, train_y, val_x, val_y):
    xgb_train = xgb.DMatrix(train_x, train_y, silent=True)
    xgb_val = xgb.DMatrix(val_x, val_y, silent=True)
    params_xgb = {
        'booster': 'gbtree',
        # 取决于使用哪种booster,有两中模型可以选择gbtree和gblinear。gbtree使用基于树的模型进行提升计算，gblinear使用线性模型进行提升计算
        'objective': 'binary:logistic',  # 定义学习任务及相应的学习目标，可选的目标函数如下：“reg:linear” —— 线性回归,“reg:logistic”—— 逻辑回归。
        'gamma': 0.03,  # 最小损失减少，模型在默认情况下，对于一个节点的划分只有在其loss function 得到结果大于0的情况下才进行，而gamma 给定了所需的最低loss function的值
        'max_depth': 7,  # 数的最大深度。缺省值为6，树的深度越大，则对数据的拟合程度越高（过拟合程度也越高）。即该参数也是控制过拟合
        'lambda': 1,  # L2正则的惩罚系数
        'alpha': 1,  # L1
        'subsample': 0.75,  # 用于训练模型的子样本占整个样本集合的比例。如果设置为0.5则意味着XGBoost将随机的从整个样本集合中随机的抽取出50%的子样本建立树模型，这能够防止过拟合。
        'colsample_bytree': 0.9,  # 在建立树时对特征采样的比例。缺省值为1。
        'colsample_bylevel': 0.9,
        'eval_metric': 'auc',
        'min_child_weight': 0.8,  # 孩子节点中最小的样本权重和，在现行回归模型中，这个参数是指建立每个模型所需要的最小样本数。该成熟越大算法越conservative。即调大这个参数能够控制过拟合。
        'max_delta_step': 0,
        'silent': 0,  # 当这个参数值为1时，静默模式开启，不会输出任何信息。一般这个参数就保持默认的0，因为这样能帮我们更好地理解模型。
        'eta': 0.01,  # 权重衰减因子eta为0.01~0.2
        'seed': 123,  # 随机数的种子。缺省值为0。
        'scale_pos_weight': 1,  # 针对正负样本不均衡：sum(negative instances) / sum(positive instances)
        'tree_method': 'auto',
        'nthread': -1,
        'early_stopping_rounds': 50}
    watchlist = [(xgb_train, 'train'), (xgb_val, 'val')]
    num_boost_round = 2000
    plst = params_xgb.items()
    model_xgb = xgb.train(plst, xgb_train, num_boost_round, evals=watchlist, verbose_eval=100, maximize=1)
    return model_xgb


####################### 模型验证################################################
def model_validate(X_val, y_val, model, threshold):
    val = xgb.DMatrix(X_val)
    print('model.best_iteration:', model.best_iteration)
    # pred_xgb_1 = model.predict(val,ntree_limit = model.best_iteration)
    pred_xgb_1 = model.predict(val, ntree_limit=1500)
    y_pred_1 = [1 if i > threshold else 0 for i in pred_xgb_1]

    print('预测结果集：', len(y_pred_1))
    print('阈值>%s 为正样本' % threshold)
    print(classification_report(y_val, y_pred_1))
    print('Accracy:', accuracy_score(y_val, y_pred_1))
    print('AUC: %.4f' % metrics.roc_auc_score(y_val, y_pred_1))
    print('ACC: %.4f' % metrics.accuracy_score(y_val, y_pred_1))
    print('Accuracy: %.4f' % metrics.accuracy_score(y_val, y_pred_1))
    print('Recall: %.4f' % metrics.recall_score(y_val, y_pred_1))
    print('F1-score: %.4f' % metrics.f1_score(y_val, y_pred_1))
    print('Precesion: %.4f' % metrics.precision_score(y_val, y_pred_1))
    # 打印特征重要性
    #     feat_imp = pd.Series(model.get_fscore()).sort_values(ascending = True)
    #     feat_imp.plot(kind='barh',figsize=(12,50),title='Feature Importances')
    #     plt.xlabel('Feature Importance Score')
    return pred_xgb_1


######################## 保存模型 ################################################
def save_model(model, model_file):
    #     save_path = '{}/{}'.format(model_dir,model_file)
    with open(model_file, 'wb') as fout:
        pickle.dump(model, fout)
    print('训练模型保存至：', str(model_file))


######################## 读取模型 ################################################
def load_model(model_file):
    with open(model_file, 'rb') as fin:
        model = pickle.load(fin)
    return model


######################## 随机采样数据 ################################################
# 随机采样
def rand_sample(df, size=50000, pred_size=5000):
    df = shuffle(df)
    pos = df[df['label'] == 1]
    neg = df[df['label'] == 0]
    # 按比例抽取样本
    # size1 = int(len(pos)*0.9)
    # size2 = int(len(neg)*0.9)
    size1 = size
    size2 = size1
    train = pd.concat([pos.iloc[0:size1, :], neg.iloc[0:size2, :]])
    scale_pos_weight = len(train['label'] == 0) / len(train['label'] == 1)
    predict = pd.concat([pos.iloc[size1:int(size1 + pred_size), :], neg.iloc[size2:int(size2 + pred_size), :]])
    print('训练集：%s,验证集：%s' % (train.shape, predict.shape))
    # print('train:\n%s' % train['true_label'].value_counts())
    # print('predict:\n%s' % predict['true_label'].value_counts())
    return train, predict


######################## 绘制混淆矩阵 ################################################
def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    fig = plt.figure(figsize=(7, 6))
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)
    #     fmt = '.2f' if normalize else 'd'
    fmt = 'd'
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], fmt),
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.tight_layout()
    plt.show()


######################## 获取时间 ################################################
def clock():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('当前时间:', now)




clock()
%time data1 = pd.read_csv('../datasets/dom_inter_user_route_prediction_offline_data_20190421.csv.gz',sep='\t') # 新数据样本
data1.head()
data1.label.value_counts()
#将缺失值全部替换为-1,主要是用户画像分类的缺失数据
data=data1.fillna(-1)

#切分正负样本
df=data
df_pos=df[df.label==1].sample(n=50000,random_state=1)
df_neg=df[df.label==0].sample(n=50000,random_state=43)
## 验证集
df_val=df.drop(df_pos.index)
df_val=df_val.drop(df_neg.index)
print('正样本量：%d,负样本量:%d, 测试集量:%d'%(len(df_pos),len(df_neg),len(df_val)))
# 合并正负样本
dfv1=df_pos.append(df_neg)
len(dfv1)
#标签划分
df_x=dfv1.drop(['label','qunar_username','search_route'],axis=1) #删除'label','qunar_username','search_route'三列
df_y=dfv1['label']
print(df_x.shape)
print(df_y.shape)



#模型训练
clock()
x_train, x_test, y_train, y_test = train_test_split(df_x, df_y, test_size=0.25, random_state=30)
# 开始训练
start=time.clock()
model = model_train(x_train,y_train,x_test,y_test)
end=time.clock()
print('train time last:%d 分钟'%((end-start)/60))


#保存模型
model_name='../model/dom_inter_route_prediction_xgboost_v1.model'
save_model(model,model_name)


#模型验证
model_name='../model/dom_inter_route_prediction_xgboost_v1.model'
try:
    model
except NameError:
     model=load_model(model_name) #加载模型
curtime=time.strftime('%Y.%m.%d %H:%m:%S',time.localtime(time.time()))
print('#####训练集5万比5万，随机10万测试集效果：')
valset=df_val.sample(n=100000,random_state=90)
X_val=valset.drop(['label','qunar_username','search_route'],axis=1)
y_val=valset['label']
# 对数据进行标准化---
# X_val=preprocessing.scale(X_val)
%time y_pred = model_validate(X_val, y_val, model, threshold=0.5)
ret= pd.DataFrame(y_pred) #绘制结果分布图
sns.distplot(y_pred[y_val == 1],color='blue')
sns.distplot(y_pred[y_val == 0],color='red')
target_names=['class 0','class 1']
y_pred_cls=[1 if i>=0.5 else 0 for i in y_pred]
cm = confusion_matrix(y_val, y_pred_cls)
print('混淆矩阵-测试集效果（%s）：'%curtime)
plot_confusion_matrix(cm,target_names)
