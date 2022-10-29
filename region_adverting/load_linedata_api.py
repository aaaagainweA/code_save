
import datetime
import jsonpath
import pandas as pd

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

    # 昨天16点到今天9点
    start = str(yesterday)+' 00:00:00'
    end = str(today)+' 00:00:00'

    df_token = pd.read_excel('token_save.xlsx')
    df_token=df_token.loc[:,['邮箱','token']]
    df_info=pd.read_excel('account_info.xlsx')
    df_info=df_info.loc[:,['邮箱','广告主ID']]
    df_info.drop_duplicates(subset=None,keep='last',inplace=True)
    final = pd.DataFrame({})
    for m in range(len(df_token)):
        account_l = df_token.values[m][0]
        token = df_token.values[m][1]
        df_list=df_info.loc[df_info['邮箱']==account_l,'广告主ID']
        adv_list=df_list.values.tolist()
        print(adv_list)
        res=pd.DataFrame({})
        for p in range(1,15):
            d=get_clue_list(start,end,adv_list,token,p)
            print(d)
            datalist=jsonpath.jsonpath(d,'$..list')[0]
            df = pd.DataFrame(datalist)
            res=pd.concat([res,df])
        final=pd.concat([final,res])
    final.to_excel('导线数据(0-24).xlsx',index=False)

