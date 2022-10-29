#刷新token,24小时有效
import pandas as pd
import jsonpath
import os
def refresh_access_token(appid,secret,refresh):
    import requests
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "oauth2/refresh_token/"
    refresh_token_url = open_api_url_prefix + uri
    data = {
        "appid": appid,
        "secret": secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh,
    }
    header={'Content-Type':'application/json'}
    rsp = requests.post(refresh_token_url, json=data,headers=header)
    rsp_data = rsp.json()
    return rsp_data

if __name__ == '__main__':

    df_token=pd.read_excel('token_save.xlsx')
    df=df_token.loc[:,['邮箱','refresh_token']]

    df_acc=pd.read_excel('account_info.xlsx')
    df_acc=df_acc.loc[:,['邮箱','app_id','secret']]
    df_acc.drop_duplicates(subset=None,keep='last',inplace=True)

    for w in range(len(df)):
        account_l = df.values[w][0]
        refresh_token = df.values[w][1]
        df_rec = df_acc.loc[df_acc['邮箱'] == account_l, ['app_id', 'secret']]
        app_id = df_rec.values[0][0]
        secret_ = df_rec.values[0][1]
        data_dict=refresh_access_token(app_id, secret_, refresh_token)
        tokenlist = jsonpath.jsonpath(data_dict, '$..access_token')
        tokenlist1 = jsonpath.jsonpath(data_dict, '$..refresh_token')
        token_ = tokenlist[0]
        refresh_ = tokenlist1[0]
        data = {'邮箱': account_l, 'token': token_, 'refresh_token': refresh_}
        df_tokens = pd.DataFrame([data])
        df_token_save = pd.concat([df_token, df_tokens])
        df_token_save.drop_duplicates(subset=['邮箱'], keep='last', inplace=True)
        df_token_save.to_excel('token_save.xlsx',index=False)
