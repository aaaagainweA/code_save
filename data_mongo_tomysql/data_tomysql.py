import pymongo
import pandas as pd
import time
from sqlalchemy import create_engine

print('开始时间{}'.format(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime())))

conn = pymongo.MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'
                           .format("cda_epmall", "3PUkMsWCTu424Renrffb",
                                   "dds-wz99d33fe2f72610a042-pub.mongodb.rds.aliyuncs.com",
                                   "3717", "epmall"))

conn2 = pymongo.MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'
                            .format("cda_kfdb", "3PUkMsWCTu424Renrffb",
                                    "dds-wz99d33fe2f72610a042-pub.mongodb.rds.aliyuncs.com",
                                    "3717", "kfdb"))
conn_epmall = conn['epmall']
conn_kfdb = conn2['kfdb']
print('1')
ENGINE = create_engine('mysql+pymysql://root:root123@192.168.0.10:3306/to_davinci')
print('2')

'''服务记录(会员档案)表'''
col_member_service = conn_epmall['member_service_record_100063']
df_member_service = pd.DataFrame(list(col_member_service.find({},{'_id':1,'user_id':1,'user_name':1,'create_time':1,'store_id':1,'delete_flag':1,'technician_id':1,'parts':1,'perimetering':1,'perimetered':1,'reduce':1,'qualified':1,'after_photo':1,'before_photo':1,'weight':1,'weight_after':1,'weight_front':1,'store_name':1})))
df_member_service = df_member_service.set_index("_id")
df_member_service['after_photo'] = df_member_service['after_photo'].map(str)
df_member_service['before_photo'] = df_member_service['before_photo'].map(str)

pd.io.sql.to_sql(df_member_service, "member_service_record_100063",
                 ENGINE, if_exists="replace", index=False)
print('member_service_record_100063')

'''会员档案'''
col_member_profile = conn_epmall["epmall_member_profile_100063"]
df_member_profile = pd.DataFrame(list(col_member_profile.find({},{'_id':1,'delete_flag':1,'create_time':1,'user_id':1,'user_name':1,'phone':1,'sex':1,'birthday':1,'area':1,'address':1,'profession':1,'marry':1,'height':1,'weight':1,'sports':1,'liposuction':1,'menstrual':1,'channel_id':1})))
df_member_profile = df_member_profile.set_index("_id")
# def change_str(long_str):
#     return str(long_str)[:50]
# df_member_profile["signature"] = df_member_profile["signature"].map(change_str)

pd.io.sql.to_sql(df_member_profile, "epmall_member_profile_100063",
                 ENGINE, if_exists="replace", index=False)
print('epmall_member_profile_100063')

'''订单表'''
col_orders = conn_epmall["epmall_orders_100063"]
df_orders = pd.DataFrame(list(col_orders.find({},{'_id':1,'order_no':1,'create_date':1,'payment_date':1,'order_type':1,'user_id':1,'nick':1,'name':1,'mobile':1,'payment':1,'trade_no':1,'state':1,'amount_total':1,'accounted_money':1,'recharge_balance':1,'totalDiscount':1,'send_amount':1,'isback':1,'back_money':1,'comefrom':1,'third_platform':1,'score':1,'remark_cus':1,'orderItemList':1,'technician_id':1,'commodity_id':1,'commodity_name':1,'plan_id':1,'plan_name':1,'channel_id':1,'channel_name':1,'sku_code':1,'is_selfpick':1,'store_id':1,'store_name':1,'recommend_store_id':1,'recommend_store_name':1,'pick_store_id':1,'pick_store_name':1,'channel_belong_name':1,'pnickname':1,'province':1,'city':1,'level':1})))
df_orders = df_orders.set_index("_id")

df_orders['orderItemList'] = df_orders["orderItemList"].map(str)
df_orders['channel_name'] = df_orders['channel_name'].map(str)
df_orders['channel_belong_name'] = df_orders['channel_belong_name'].map(str)

pd.io.sql.to_sql(df_orders, "epmall_orders_100063",
                 ENGINE, if_exists="replace", index=False)
print('epmall_orders_1000063')

'''充值表'''
col_recharge = conn_epmall["epmall_recharge_log_100063"]
df_recharge = pd.DataFrame(list(col_recharge.find({},{'_id':1,'recharge_id':1,'user_id':1,'phone':1,'pay_state':1,'amount_pay':1,'send_amount':1,'recharge_date':1,'payment':1,'deal_no':1,'activity_store_id':1,'store_id':1,'store_name':1,'member_name':1,'m_name':1})))
df_recharge = df_recharge.set_index("_id")
df_recharge.rename(columns={'phone': 'phonex'}, inplace=True)
df_recharge.rename(columns={'store_name': 'restore_name'}, inplace=True)
pd.io.sql.to_sql(df_recharge, "epmall_recharge_log_100063",
                 ENGINE, if_exists="replace", index=False)
print('epmall_recharge_log_100063')

'''服务明细表'''
col_service = conn_epmall["epmall_service_record_100063"]
df_service = pd.DataFrame(list(col_service.find({},{'_id':1,'delete_flag':1,'commodity_id':1,'item_category':1,'item_name':1,'create_time':1,'order_no':1,'user_id':1,'phone':1,'name':1,'technician_id':1,'technician_name':1,'store_id':1,'store_name':1,'amount_total':1,'afterDiscount':1,'amount_pay':1,'totalDiscount':1,'channel_id':1,'channel_name':1,'channel_belong_name':1})))
df_service = df_service.set_index("_id")

pd.io.sql.to_sql(df_service, "epmall_service_record_100063",
                 ENGINE, if_exists="replace", index=False)
print('epmall_service_record_100063')

""" epmall_store_100063"""
col_store = conn_epmall["epmall_store_100063"]
df_store = pd.DataFrame(list(col_store.find({},{'_id':1,'delete_flag':1,'store_id':1,'store_name':1,'phone':1,'province':1,'city':1,'address':1,'store_number':1,'state':1})))
df_store = df_store.set_index("_id")

pd.io.sql.to_sql(df_store, "epmall_store_100063",
                 ENGINE, if_exists="replace", index=False)
print('epmall_store_100063')

"""epmall_service_percentage_detail_100063"""
col_service_detail = conn_epmall["epmall_service_percentage_detail_100063"]
df_service_detail = pd.DataFrame(list(col_service_detail.find({},{'_id':1,'id':1,'create_time':1,'time':1,'order_no':1,'delete_flag':1,'user_id':1,'phone':1,'item_category':1,'afterDiscount':1,'type':1,'back_money':1,'back_state':1,'commission':1,'store_id':1,'technician_id':1})))
df_service_detail = df_service_detail.set_index("_id")

pd.io.sql.to_sql(df_service_detail, "epmall_service_percentage_detail_100063", ENGINE, if_exists="replace",
         index=False)
print('epmall_service_percentage_detail_100063')

"""epmall_app_user_100063"""
col_app_user = conn_epmall["epmall_app_user_100063"]
df_app_user = pd.DataFrame(list(col_app_user.find({},{'_id':1,'delete_flag':1,'create_time':1,'phone':1,'user_id':1,'nickname':1,'sex':1,'age':1,'birthday':1,'city':1,'comefrom':1,'job':1,'unionid':1,'recharge_balance':1,'first_order_time':1,'last_buy_time':1,'channel_belong_type':1,'channel_id':1,'channel_belong_id':1,'deliver_id':1,'channel_name':1})))
df_app_user = df_app_user.set_index("_id")

pd.io.sql.to_sql(df_app_user, "epmall_app_user_100063",
         ENGINE, if_exists="replace", index=False)
print('epmall_app_user_100063')

"""epmall_deliver_plan_100063"""
col_deliver_plan = conn_epmall["epmall_deliver_plan_100063"]
df_deliver_plan = pd.DataFrame(list(col_deliver_plan.find({},{'_id':1,'delete_flag':1,'create_time':1,'deliver_id':1,'deliver_name':1,'channel_id':1,'channel_name':1,'belong_id':1,'belong':1,'store_id':1,'store_name':1})))
df_deliver_plan = df_deliver_plan.set_index("_id")

pd.io.sql.to_sql(df_deliver_plan, "epmall_deliver_plan_100063",
         ENGINE, if_exists="replace", index=False)
print('epmall_deliver_plan_100063')

"""epmall_technician_100063"""
col_technician = conn_epmall["epmall_technician_100063"]
df_technician = pd.DataFrame(list(col_technician.find({},{'_id':1,'delete_flag':1,'technician_id':1,'name':1,'sex':1,'item_category':1,'item_name':1,'job_list':1,'store_id':1,'store_name':1})))
df_technician = df_technician.set_index("_id")

df_technician["item_category"] = df_technician["item_category"].map(str)
df_technician["item_name"] = df_technician["item_name"].map(str)
df_technician["job_list"] = df_technician["job_list"].map(str)

pd.io.sql.to_sql(df_technician, "epmall_technician_100063",
         ENGINE, if_exists="replace", index=False)
print('epmall_technician_100063')

"""epmall_back_order"""
col_back_order = conn_epmall["epmall_back_order"]
df_back_order = pd.DataFrame(list(col_back_order.find({},{'_id':1,'order_no':1,'trade_no':1,'state':1,'user_id':1,'mobile':1,'return_date':1,'create_date':1,'cashier_data':1,'back_result':1,'orderItemList':1,'amount_total':1,'amount_money':1,'amount_refund':1,'technician_id':1,'order_type':1})))
df_back_order = df_back_order.set_index("_id")

df_back_order["orderItemList"] = df_back_order["orderItemList"].map(str)

pd.io.sql.to_sql(df_back_order, "epmall_back_order",
         ENGINE, if_exists="replace", index=False)
print('epmall_back_order')

'''epmall_appointment_100063'''
col_app=conn_epmall['epmall_appointment_100063']
df_appoint=pd.DataFrame(list(col_app.find({},{'_id':1,'phone':1,'customer_id':1,'customer_name':1,'dept_name':1,'appointed_date':1,'store_name':1,'deliver_id':1,'deliver_name':1,'state':1,'source':1,'user_id':1,'create_time':1,'id':1,'nickname':1,'wx_name':1,'item_name':1,'arrival_date':1,'modify_time':1,'audit_state':1,'remark':1,'audit_name':1,'audit_time':1,'audit_one_type_name':1})))
df_appoint = df_appoint.set_index("_id")

pd.io.sql.to_sql(df_appoint, "epmall_appointment_100063",
                 ENGINE, if_exists="replace", index=False)
print('epmall_appointment_100063')

'''ccc_user'''
col_ccc_user=conn_epmall['ccc_user']
df_ccc_user=pd.DataFrame(list(col_ccc_user.find({},{'_id':1,'user_id':1,'address':1,'group_name':1,'phone':1,'create_time':1,'belong_customer_name':1,'user_name':1,'drainage_channel':1,'delete_flag':1,'drainage_channel_id':1,'area':1,'province':1,'city':1,'customer_group_name':1,'channen_name':1,'channel_id':1,'media_name':1,'media_id':1,'company_id':1,'customer_info_create_time':1})))
df_ccc_user = df_ccc_user.set_index("_id")

pd.io.sql.to_sql(df_ccc_user, "ccc_user",
                 ENGINE, if_exists="replace", index=False)
print('ccc_user')

'''ccc_userinfo'''
col_ccc_userinfo=conn_epmall['ccc_userinfo']
df_ccc_userinfo=pd.DataFrame(list(col_ccc_userinfo.find({},{'_id':1,'phone':1,'customer_id':1,'display_name':1,'create_time':1,'delete_flag':1,'skil_group_name':1})))
df_ccc_userinfo = df_ccc_userinfo.set_index("_id")

pd.io.sql.to_sql(df_ccc_userinfo, "ccc_userinfo",
         ENGINE, if_exists="replace", index=False)
print('ccc_userinfo')


"""epmall_technician_audit_rank_100063"""
col_technician_audit = conn_epmall["epmall_technician_audit_rank_100063"]
df_technician_audit = pd.DataFrame(list(col_technician_audit.find()))
df_technician_audit = df_technician_audit.set_index("_id")

df_technician_audit['change_voucher'] = df_technician_audit['change_voucher'].map(str)
df_technician_audit['skill_ids'] = df_technician_audit['skill_ids'].map(str)

pd.io.sql.to_sql(df_technician_audit, "epmall_technician_audit_rank_100063",
         ENGINE, if_exists="replace", index=False)
print('technician_audit')

"""epmall_technician_rank_config_100063"""
col_technician_rank_config = conn_epmall["epmall_technician_rank_config_100063"]
df_technician_rank_config = pd.DataFrame(list(col_technician_rank_config.find()))
df_technician_rank_config = df_technician_rank_config.set_index("_id")

pd.io.sql.to_sql(df_technician_rank_config, "epmall_technician_rank_config_100063",
         ENGINE, if_exists="replace", index=False)


conn.close()
conn2.close()
print('结束时间{}'.format(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime())))
