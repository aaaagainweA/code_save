
import datetime
import jsonpath
import pandas as pd
import  re

def get_clue_list(starttime,endtime,adv_list,token,p):
    import requests
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "2/tools/clue/get/"
    url = open_api_url_prefix + uri
    data = {
        'advertiser_ids': adv_list,
        'start_time': starttime,
        'end_time': endtime,
        'page':p,
        'page_size':100
    }
    header={'access-token':token}
    rsp = requests.get(url, json=data,headers=header)
    rsp_data = rsp.json()
    return rsp_data

if __name__ == '__main__':
    now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday

    # 今天14点到今天16点
    start = str(today)+' 14:00:00'
    end = str(today)+' 16:00:00'

    df_token = pd.read_excel(r'D:/python_project/projects/region_adverting/token_save.xlsx')
    df_token=df_token.loc[:,['邮箱','token']]
    df_info=pd.read_excel(r'D:/python_project/projects/region_adverting/account_info.xlsx')
    df_info=df_info.loc[:,['邮箱','广告主ID']]
    df_info.drop_duplicates(subset=None,keep='last',inplace=True)
    final = pd.DataFrame({})
    for m in range(len(df_token)):
        account_l = df_token.values[m][0]
        token = df_token.values[m][1]
        df_list=df_info.loc[df_info['邮箱']==account_l,'广告主ID']
        adv_list=df_list.values.tolist()
        res=pd.DataFrame({})
        for p in range(1,10):
            d=get_clue_list(start,end,adv_list,token,p)
            datalist=jsonpath.jsonpath(d,'$..list')[0]
            print(d)
            df = pd.DataFrame(datalist)
            res=pd.concat([res,df])
        final=pd.concat([final,res])

    df_finall1 = final.loc[:,
                 ['name', 'telephone', 'clue_id', 'follow_state_name', 'clue_owner_name', 'system_tags',
                  'clue_type', 'app_name', 'convert_status', 'external_url', 'create_time_detail', 'location',
                  'advertiser_id', 'advertiser_name', 'clue_state_name', 'weixin', 'qq', 'gender', 'age', 'email',
                  'date', 'address', 'remark', 'remark_dict', 'ad_id', 'ad_name', 'module_id', 'module_name'
                     , 'allocation_status']]

    df_finall1.columns = ['姓名', '电话', '线索ID', '通话状态', '所属人', '标签', '线索类型', '流量来源', '转化状态',
                          '落地页链接', '线索创建时间'
        , '自动定位城市', '广告主ID', '广告主名称', '线索阶段', '微信', 'QQ号', '性别', '年龄', '邮箱', '日期',
                          '详细地址', '备注',
                          '补充信息', '广告计划ID', '广告计划名称', '组件ID', '组件名称', '分配状态']

    df_finall1 = df_finall1.astype(
        {'电话': 'str', '线索ID': 'str', '广告主ID': 'str', '广告计划ID': 'str', '组件ID': 'str'})
    df_finall1.to_excel(f'D:/导线数据/{now_time}(14-16).xlsx',index=False)

    # 市
    def extract_city(x: str):
        if str(x) != 'nan':
            y = str(x).split('+')
            return y[1]


    final['citys'] = final['location'].apply(lambda x: extract_city(x))


    # 省
    def extract_province(x: str):
        if str(x) != 'nan':
            y = str(x).split('+')
            return y[0]


    final['provinces'] = final['location'].apply(lambda x: extract_province(x))

    df_all = final.loc[:,
             ['name', 'telephone', 'location', 'create_time_detail', 'provinces', 'citys', 'remark_dict',
              'advertiser_name']]



    df_all.rename(columns={df_all.columns[0]: '姓名', df_all.columns[1]: '电话号码', df_all.columns[2]: '详细地址',
                           df_all.columns[3]: '客户填表时间', df_all.columns[4]: '省', df_all.columns[6]: '所属产品',
                           df_all.columns[7]: '用户名', df_all.columns[5]: '市'}, inplace=True)

    dfx = pd.read_excel(r'D:/python_project/projects/region_adverting/account_info.xlsx')
    dfx1 = dfx.loc[:, ['用户名', '投放地域']]
    dfx1['市'] = dfx1['投放地域'].apply(lambda x: re.findall(r'[\u4e00-\u9fa5]+', x)[0])
    df_merge = pd.merge(df_all, dfx1, on=['用户名', '市'], how='left')

    df_merge['客户填表时间'] = df_merge['客户填表时间'].apply(lambda x: x[0:16])
    # df_merge['详细地址'] = df_merge['详细地址'].str.replace('+', '-')

    df_merge['区/县'] = ''
    df_merge['客服手机号'] = ''

    df_merge.rename(columns={df_merge.columns[8]: '投放渠道'}, inplace=True)

    df_merge = df_merge.loc[:, ['姓名', '电话号码', '详细地址', '投放渠道', '客户填表时间', '省', '市', '区/县', '客服手机号', '所属产品']]

    # import ast
    #
    #
    # def extract(x):
    #     if str(x) != 'nan' and x != '{}':
    #         transdict = ast.literal_eval(x)
    #         for key in transdict.keys():
    #             key1 = key
    #         for value in transdict.values():
    #             value1 = value
    #         x = key1 + ':' + value1
    #     else:
    #         x = ''
    #     return x

    # df_merge['所属产品'] = df_merge['所属产品'].apply(lambda x: extract(x))

    group1 = df_merge[df_merge['市'].isin(['武汉', '郑州', '长沙', '广州', '深圳', '东莞', '佛山', '珠海', '成都'])]
    group2 = df_merge[
        df_merge['市'].isin(['杭州', '温州', '北京', '台州', '丽水', '金华', '绍兴', '宁波', '无锡', '南京', '苏州', '衢州', '湖州', '贵阳', '西安'])]
    group1.to_excel(f'D:/导线数据/{now_time}(14-16)组一.xlsx', index=False)
    group2.to_excel(f'D:/导线数据/{now_time}(14-16)组二.xlsx', index=False)
