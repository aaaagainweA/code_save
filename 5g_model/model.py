
# -*- coding: utf-8 -*-

#import sys
#sys.path.append('D:\\wangxingwei\\模型\\lsmodel\\lsmodel')
#import utils
#from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import confusion_matrix
from sklearn.linear_model import LogisticRegression
import pandas as pd
#from imblearn.combine import SMOTEENN
from imblearn.over_sampling import RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import AdaBoostClassifier
import time
from sklearn.cross_validation import train_test_split
from sklearn import svm
#预测分数分段
def predictToScoreLevel(x):
    if(x>=0 and x<10):
        return 0
    elif(x>=10 and x<20):
        return 10
    elif(x>=20 and x<30):
        return 20
    elif(x>=30 and x<40):
        return 30
    elif(x>=40 and x<50):
        return 40
    elif(x>=50 and x<60):
        return 50
    elif(x>=60 and x<70):
        return 60
    elif(x>=70 and x<80):
        return 70
    elif(x>=80 and x<90):
        return 80
    elif(x>=90 and x<=100):
        return 90
def getprocee_info(path,model_no):
    file=open(path+'/data/output/process_info_'+model_no+'.pkl','rb')
    try:
        pro_info=pickle.load(file)
    except EOFError:
        None   
    file.close()
    return pro_info
#模型结果分析
def model_rs(x,y,model):
    score_lr=pd.DataFrame({
    'score':model.predict_proba(x)[:,1]*100,'t_class':y},\
                           columns=['t_class','score'])
    score_lr['score_level']=score_lr['score'].map(predictToScoreLevel)
    xy=pd.crosstab(score_lr['score_level'],score_lr['t_class'],rownames='x',colnames='y')
    xy.columns=['y_'+str(i) for i in xy.columns]
    xy['total']=xy['y_1']+xy['y_0']
    xy['prob']=xy['y_1']*100/xy['total']
    xy['recall']=xy['y_1']*100/sum(xy['y_1'])
    xy['lift']=xy['prob']/(sum(xy['y_1'])*100/(sum(xy['y_1'])+sum(xy['y_0'])))
    return xy.sort_index(ascending=False)  
#模型保存
def savemodel(path,model_no,model):
    file=open(path+'model/ls_model_'+model_no+'.pkl','wb')
    pickle.dump(model,file)
    file.close()

#读取数据
def train_test(path,model_no,key,target,cols,flag=0):
    train=pd.read_csv(path+'resualt/ptrain_'+model_no+'.csv',encoding='gbk')
    test=pd.read_csv(path+'resualt/ptest_'+model_no+'.csv',encoding='gbk')
    x_train=train[cols]
    x_test=test[cols]
    y_train=train[target[0]]
    y_test=test[target[0]]
    if(flag==1):
        sample=RandomOverSampler(random_state=0)
    else:
        sample=RandomUnderSampler(random_state=0)
    xt,yt=sample.fit_sample(x_train[cols], y_train)
    return xt,yt, x_train,x_test,y_train,y_test,test,train

#模型分析结果输出    
def model_log_output(path,model_no,model,name,x_train,x_test,y_train,y_test):
    log=open(path+'/log/'+'model_log_'+model_no+'.txt','a')
    print('====================   模型__'+name+'__结果   ====================',file=log)
    print('程序运行时间为：{}'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))),file=log)
    print('\n',file=log)
    print('参数为:{}'.format(model.get_params()),file=log)
    print('训练集结果：',file=log)
    print('混淆矩阵为：',file=log)
    yp_train=model.predict(x_train)
    print(confusion_matrix(y_train,yp_train),file=log)
    print('查准率为{}'.format(precision_score(y_train,yp_train)),file=log)
    print('召回率为{}'.format(recall_score(y_train,yp_train)),file=log)
    print('F1-score为{}'.format(f1_score(y_train,yp_train)),file=log)
    print('测试集结果：',file=log)
    print('混淆矩阵为：',file=log)
    yp_test=model.predict(x_test)
    print(confusion_matrix(y_test,yp_test),file=log)
    print('查准率为{}'.format(precision_score(y_test,yp_test)),file=log)
    print('召回率为{}'.format(recall_score(y_test,yp_test)),file=log)
    print('F1-score为{}'.format(f1_score(y_test,yp_test)),file=log)
    print('\n',file=log)
    print('训练集模型结果分析：',file=log)
    print(model_rs(x_train,y_train,model),file=log)
    print('\n',file=log)
    print('测试集模型结果分析：',file=log)
    print(model_rs(x_test,y_test,model),file=log)
    print('\n',file=log)
    print('模型结构：',file=log)
    pd.set_option('display.max_rows', 500)#设置显示最大行数
    if(name in ['logit']):
        print(pd.DataFrame({'var':x_train.columns,'coef':model.coef_[0]},columns=['var','coef']).sort_values(by="coef",ascending=False),file=log)
    elif(name in ['dt','rfc','gbdt']):
        print(pd.DataFrame({'var':x_train.columns,'var_import':model.feature_importances_},columns=['var','var_import']).sort_values(by="var_import",ascending=False),file=log)
    else:
        pass
    print('\n',file=log)
    print('===============================================   end   ========================================================',file=log)
    log.close()
    
#逻辑回归模型
def log_model(x,y,x_train,x_test,y_train,y_test,penalty,c):
    lg=LogisticRegression(penalty=penalty,C=c,random_state=123)
    lg.fit(x,y)
    return lg
    
def rfc_model(x,y,x_train,x_test,y_train,y_test,n,m):
    rfc=RandomForestClassifier(random_state=123,n_estimators=n,max_features=0.7,max_depth=m,oob_score=True,n_jobs=16)
    rfc.fit(x,y)
    return rfc
    
def boost_model(x,y,x_train,x_test,y_train,y_test,n):
    boost=AdaBoostClassifier(random_state=123,n_estimators=n)
    boost.fit(x,y)
    return boost
 
def gbdt_model(x,y,x_train,x_test,y_train,y_test,n):
    gbdt=GradientBoostingClassifier(random_state=123,n_estimators=n,subsample=0.7)
    gbdt.fit(x,y)
    return gbdt    
def svm_model(x,y,x_train,x_test,y_train,y_test,kernel='rbf',l=1,degree=3):
    clf=svm.SVC(C=l,kernel=kernel,probability=True,degree=degree)
    clf.fit(x,y)
    return clf
if __name__=='__main__':
    pd.set_option('display.max_rows', 500)#设置显示最大行数
    path='D:/jiangong/0808/'
    model_no='t2g'
    print(model_no)
    target=['change_flag']
    key=['prd_inst_id']
    pro_info=getprocee_info(path,model_no)
    cols_corr=pro_info['var_model']
    x,y,x_train,x_test,y_train,y_test,test,train=train_test(path,model_no,key,target,cols_corr)  #数据获取  
    
    n=200
    m=15
    rfc=rfc_model(x,y,x_train,x_test,y_train,y_test,n,m)
    model_log_output(path,model_no,rfc,'rfc',x_train,x_test,y_train,y_test)
    savemodel(path,model_no,rfc)
    
    n=5
    boost=boost_model(x,y,x_train,x_test,y_train,y_test,n)
    model_log_output(path,model_no,boost,'boost',x_train,x_test,y_train,y_test)
    #savemodel(path,model_no,boost)
    n=5
    gbdt=gbdt_model(x,y,x_train,x_test,y_train,y_test,n)
    model_log_output(path,model_no,gbdt,'gbdt',x_train,x_test,y_train,y_test)
    
    #逻辑回归模型调参
    penalty='l1'
    c=0.001
    lg=log_model(x,y,x_train,x_test,y_train,y_test,penalty=penalty,c=c)  
    model_log_output(path,model_no,lg,'logit',x_train,x_test,y_train,y_test) 
    #model_log_output(path,model_no,lg,'logit',x_train,train.loc[train['bil_month']==201804,cols],y_train,train.loc[train['bil_month']==201804,target[0]])
    l=1
    kernel='rbf'
    degree=2
    clf=svm_model(x,y,x_train,x_test,y_train,y_test,kernel,l,degree)
    model_log_output(path,model_no,clf,'clf',x_train,x_test,y_train,y_test) 
    #savemodel(path,model_no,lg,train)
    
