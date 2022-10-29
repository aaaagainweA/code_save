# Python文件
# 三方库
import pandas as pd
import pymongo
import pymysql
import datetime
import time
from bson.objectid import ObjectId
import warnings
warnings.filterwarnings('ignore')
# 连接mongdb
conn = pymongo.MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'
                       .format("cda_epmall", "3PUkMsWCTu424Renrffb",
                               "dds-wz99d33fe2f72610a042-pub.mongodb.rds.aliyuncs.com",
                               "3717", "epmall"))
conn_epmall = conn["epmall"]


#筛选当月的数据
def object_id_from_datetime(from_datetime=None,span_days=0,span_hours=0,span_minutes=0,span_seconds=0,span_weeks=0):
    '''根据时间手动生成一个objectid，此id不作为存储使用'''
    if not from_datetime:
        from_datetime = datetime.datetime.now()
    from_datetime = from_datetime + datetime.timedelta(days=span_days,hours=span_hours,minutes=span_minutes,weeks=span_weeks)
    return ObjectId.from_datetime(generation_time=from_datetime)
x = datetime.date.today()
oneday = datetime.timedelta(days=1)
y = x - oneday
str_month=datetime.datetime(y.year, y.month, 1)
str_date=str(str_month)[:10]
en_date = str(y)
date=str(x)
print(str_date,en_date)
begin_date = str(time.mktime(time.strptime(str_date, "%Y-%m-%d")))
end_date = str(time.mktime(time.strptime(date, "%Y-%m-%d")))
date_list=[]
while True:
    date_list.append(str(x))
    if str(x) == str(str_month)[:10]:
        break
    x = x-oneday


col_order = conn_epmall['epmall_orders_100063']
df_order = pd.DataFrame(list(col_order.find({'create_date':{'$gt':begin_date,'$lt':end_date},'state':{'$gte':1,'$lte':9}},
                                             {'_id':0,'state':1,'order_no':1,'create_date':1,'user_id':1,'nick':1,'mobile':1,
                                              'isback':1,'commodity_id':1,'commodity_name':1,'accounted_money':1,'remark_cus':1,
                                              'recommend_store_name':1})))

col_make=conn_epmall['epmall_appointment_100063'] #找出预约门店
df_make=pd.DataFrame(list(col_make.find({},{'_id':0,'store_name':1,'phone':1})))


col_store=conn_epmall['epmall_store_100063'] #找出预约城市
df_store=pd.DataFrame(list(col_store.find({},{'_id':0,'store_name':1,'city':1})))

df_new=pd.merge(df_make,df_store,on='store_name') #以为store_name基准合并两表

col_put=conn_epmall['ccc_user']
df_put=pd.DataFrame(list(col_put.find({},{'_id':0,'phone':1,'province':1,'city':1})))
df_put=df_put.rename(columns={'city':'投流城市'})

col_app_user=conn_epmall['epmall_app_user_100063'] #小程序用户表
col_app_user=pd.DataFrame(list(col_app_user.find({},{'_id':0,'phone':1,'deliver_name':1})))

df_new3=pd.merge(df_put,df_new,on='phone',how='outer')

df_new4=pd.merge(df_new3,col_app_user,on='phone',how='outer')
df_new4.rename(columns={'store_name':'预约门店','city':'预约城市','phone':'电话','province':'投流省份','deliver_name':'客户渠道'},inplace=True)



# 将时间戳转化为时间格式
def time_t(x):
    if x=='':
        return ''
    elif x==' ':
        return ''
    elif str(x)=='nan':
        return ''
    elif str(x)=='NaN':
        return ''
    else:
        x=int(x)
        time_local = time.localtime(x/1000)
        create_date = time.strftime('%Y-%m-%d %H:%M:%S', time_local)
        return create_date
df1_order = df_order.loc[:,['order_no','create_date','user_id','nick','mobile','isback','commodity_id','commodity_name','accounted_money','remark_cus','recommend_store_name']]
df1_order['accounted_money'] = df1_order['accounted_money'] / 100
df1_order['create_date'] = df1_order['create_date'].apply(lambda x: time_t(x))
df1_order['create_time'] = df1_order['create_date'].apply(lambda x: str(x)[11:])
df1_order['create_date'] = df1_order['create_date'].apply(lambda x: str(x)[:10])

col_commodity=conn_epmall['epmall_commodity_100063'] # 商品表
df_commodity=pd.DataFrame(list(col_commodity.find({},
                                             {'_id':0,'commodity_id':1,
                                              'variety_id':1,'variety_name':1})))

variety_id=['1646038407932','1646209380573','1646491684554']
df_commodity = df_commodity[df_commodity['variety_id'].isin(variety_id)]
commodity_id_list=list(df_commodity['commodity_id'])
df2_order = df1_order[df1_order['commodity_id'].isin(commodity_id_list)]
df2_order=pd.merge(df2_order,df_commodity,on='commodity_id',how='left')

db=pymysql.connect(host='rm-wz9220210vr0pila6guy90110-pub.mysql.rds.aliyuncs.com',database='epmall',user='cda_epmall',password='3PUkMsWCTu424Renrffb',port=3306,charset='utf8')
sql='select * from user'
df_user=pd.read_sql(sql,db)
user1=df_user.loc[:,['username','dept_id','id']]
user1=user1.rename(columns={'id':'u_id'})
user1.loc[user1['dept_id'].isnull(),'dept_id']=0
user1['dept_id']=user1['dept_id'].apply(lambda x:int(x))
sql1='select * from dept'
df_dept=pd.read_sql_query(sql1,db)
dept=df_dept.loc[:,['id','name']]
user_dept=pd.merge(user1,dept,left_on='dept_id',right_on='id',how='left')
user_dept.fillna('0',inplace=True)

ccc_share=conn_epmall["epmall_share_order_detail_100063"]
df_share=pd.DataFrame(list(ccc_share.find()))
share_1=df_share.loc[(df_share['delete_flag']==0),['share_id','order_no','belong_user_name','belong_user_id']]
df1_user_dept = pd.merge(user_dept,share_1,left_on='u_id', right_on='belong_user_id',how='right')

df3_order = pd.merge(df2_order,df1_user_dept,on='order_no',how='left')

df4_order = df3_order.loc[:,
            ['order_no', 'user_id', 'nick', 'mobile', 'create_date', 'create_time', 'accounted_money', 'isback',
             'commodity_name','variety_name', 'remark_cus', 'recommend_store_name', 'belong_user_name', 'name']]
df4_order['isback'] = df4_order['isback'].apply(lambda x: '否' if  x==0 else '是')
df4_order['remark_cus']=df4_order['remark_cus'].apply(lambda x:str(x)[:10])
df4_order.columns = ['订单编号','用户ID','昵称','电话','订单创建日期','订单创建时间','实付金额','是否退款','套餐名称','套餐分类','订单来源','门店','营销客服','部门']
ccc_1=conn_epmall["ccc_user"]
ccc_2=pd.DataFrame(list(ccc_1.find({},{'_id':0,'phone':1,'create_time':1,'delete_flag':1})))
ep_ccc=ccc_2.loc[(ccc_2['delete_flag']==0),['phone','create_time']]
ep_ccc['create_time']=ep_ccc['create_time'].apply(lambda x:time_t(x))
ep_ccc2=ep_ccc.groupby('phone')['create_time'].min().reset_index()
result=pd.merge(df4_order,ep_ccc2,left_on='电话',right_on='phone',how='left')
result.rename(columns={'create_time':'首次导粉日期'},inplace=True)


result=pd.merge(result,df_new4,on='电话',how='left')
result['首次导粉时间'] = pd.to_datetime(result['首次导粉日期'],format='%Y-%m-%d %H:%M:%S').dt.time
result['首次导粉日期'] = pd.to_datetime(result['首次导粉日期'],format='%Y-%m-%d %H:%M:%S').dt.date

result_last=result.loc[:,['订单编号','用户ID','昵称','电话','订单创建日期','订单创建时间','实付金额','是否退款','套餐名称','套餐分类','订单来源','门店','首次导粉日期','营销客服','部门','预约门店','预约城市','投流省份','投流城市','客户渠道']]
result_last.drop_duplicates(subset='订单编号',keep='last',inplace=True) #去重
result_last.to_excel( './礼宾客服.xlsx', index=False, header=True)
conn.close()
