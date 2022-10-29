import json
import requests
import datetime
import jsonpath
import pandas as pd
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa

PATH = "/open_api/2/report/advertiser/get/"

def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "ad.oceanengine.com"
    return urlunparse((scheme, netloc, path, "", query, ""))


def get(json_str):
    # type: (str) -> dict
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": token,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()


if __name__ == '__main__':
    now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    start = str(yesterday)
    end_date = start
    # order_field = ORDER_FIELD
    # page_size = PAGE_SIZE
    start_date = start

    df_token = pd.read_excel('token_save.xlsx')
    df_token = df_token.loc[:, ['邮箱', 'token']]
    df_info = pd.read_excel('account_info.xlsx')
    df_info = df_info.loc[:, ['邮箱', '广告主ID']]
    df_info.drop_duplicates(subset=None, keep='last', inplace=True)
    final = pd.DataFrame({})
    for m in range(len(df_token)):
        account_l = df_token.values[m][0]
        token = df_token.values[m][1]
        df_list = df_info.loc[df_info['邮箱'] == account_l, '广告主ID']
        adv_list = df_list.values.tolist()
        res = pd.DataFrame({})
        for  advertiser_id in adv_list:
            # group_by_list = GROUP_BY
            # group_by = json.dumps(group_by_list)
            # time_granularity = TIME_GRANULARITY
            page = 1
            page_size=100
            # order_type = ORDER_TYPE
            # Args in JSON format
            # my_args = "{\"end_date\": \"%s\", \"order_field\": \"%s\", \"page_size\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"group_by\": %s, \"time_granularity\": \"%s\", \"page\": \"%s\", \"order_type\": \"%s\"}" % (
            # end_date, order_field, page_size, start_date, advertiser_id, group_by, time_granularity, page, order_type)
            my_args = "{\"end_date\": \"%s\",\"page_size\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\"}" % (
            end_date,page_size, start_date,advertiser_id)
            dict1=get(my_args)
            # print(dict1)
            datalist = jsonpath.jsonpath(dict1, '$..list')[0]

            df1 = pd.DataFrame(datalist)
            res = pd.concat([res, df1])
        final = pd.concat([final, res])
    final=final.astype({'advertiser_id':'str'})

    final.to_excel('消耗.xlsx', index=False)
