import pymongo
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import  By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
import random
from selenium.webdriver.chrome.options import Options
import warnings
import json
import datetime
from fake_useragent import UserAgent

def get_stacks(distance):
    distance += 20
    # 初速度
    v0 = 0
    # 加减速度列表
    a_list = [50, 65, 80]
    # 时间
    t = 0.1
    # 初始位置
    s = 0
    # 向前滑动轨迹
    forward_stacks = []
    mid = distance * 3 / 5
    while s < distance:
        if s < mid:
            a = a_list[random.randint(0, 2)]
        else:
            a = -a_list[random.randint(0, 2)]
        v = v0
        stack = v * t + 0.5 * a * (t ** 2)
        # 每次拿到的位移
        stack = round(stack)
        s += stack
        v0 = v + a * t
        forward_stacks.append(stack)
    # 往后返回20距离，因为之前distance向前多走了20
    back_stacks = [-10, -5, -5,-8]
    return {'forward_stacks': forward_stacks, 'back_stacks': back_stacks}

def login(account,password):
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH,
                                                             '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[3]/div/span[1]/label/input'))
    driver.find_element(By.XPATH,
                        '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[3]/div/span[1]/label/input').send_keys(
        account)
    driver.find_element(By.XPATH,
                        '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[3]/div/span[2]/span[1]/label/input').send_keys(
        password)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[3]/label/span[1]'))).click()
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[3]/button'))).click()


def slide():
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH, '//*[@id="captcha-verify-image"]'))
    # 找到大图并截图
    img_back = driver.find_element(By.XPATH, '//*[@id="captcha-verify-image"]')
    img_back.screenshot('a.png')
    btn = driver.find_element(By.XPATH, '//*[@id="secsdk-captcha-drag-wrapper"]/div[1]')

    # img_url，图片存放路径
    # 读取图片，并获取图片的base64数据
    # 将base64发给第三方
    import base64, requests
    api_post_url = "http://www.bingtop.com/ocr/upload/"
    with open('a.png', 'rb') as pic_file:
        img64 = base64.b64encode(pic_file.read())
    params = {
        "username": "useragain",
        "password": "yf931213",
        "captchaData": img64,
        "captchaType": 1318
    }
    response = requests.post(api_post_url, data=params)
    dictdata = json.loads(response.text)
    # dictdata: {"code":0, "message":"", "data":{"captchaId":"1001-158201918112812","recognition":"RESULT"}}
    # 获取包含移动距离的字典
    # print(dictdata)
    # 字典中提取距离
    distance = int(dictdata['data']['recognition'])

    stacks = get_stacks(distance)

    forward_stacks = stacks['forward_stacks']
    back_stacks = stacks['back_stacks']

    time.sleep(0.2)
    ActionChains(driver).click_and_hold(btn).perform()
    time.sleep(0.2)

    for forward_stack in forward_stacks:
        ActionChains(driver).move_by_offset(xoffset=forward_stack, yoffset=0).perform()
        time.sleep(0.1)
    for back_stack in back_stacks:
        ActionChains(driver).move_by_offset(xoffset=back_stack, yoffset=0).perform()
        time.sleep(0.1)

    time.sleep(0.2)
    ActionChains(driver).release().perform()

def loc(temp):
    WebDriverWait(driver, 10).until(
        lambda x: x.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/span/label/input'))
    driver.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/span/label/input').send_keys(temp)
    time.sleep(0.5)
    if temp == '武汉洪山医普茂医疗美容门诊部有限公司-TM-生活服务-1':
        locator = (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[2]')
        WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element(locator, temp))
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[2]'))).click()
    else:
        locator = (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[1]')
        WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element(locator, temp))
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[1]'))).click()

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="primary-sales"]'))).click()
    time.sleep(0.5)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/span[1]'))).click()
    time.sleep(0.5)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@class="feiyu-byted-select-popover-panel-inner"]/div[6]/div'))).click()


def loc2(temp1):

    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH,
                                                             '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/span[2]/label/input'))

    driver.find_element(By.XPATH,
                        '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/span[2]/label/input').send_keys(Keys.CONTROL,'a')

    driver.find_element(By.XPATH,
                        '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/span[2]/label/input').send_keys(Keys.DELETE)
    time.sleep(1)

    driver.find_element(By.XPATH,
                        '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/span[2]/label/input').send_keys(
        temp1)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/button'))).click()

    time.sleep(1)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[5]/div[1]/div[2]/table/tbody/tr/*/div/a'))).click()

    time.sleep(1)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@class="master-1aeimV6uW7DfUyOVwa0L4k"]/div[1]/div/div[4]'))).click()


    time.sleep(1)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@class="i-icon i-icon-close"]'))).click()
    # try:
    #     WebDriverWait(driver, 2).until(
    #         EC.element_to_be_clickable((By.XPATH,
    #                                     '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[5]/div[1]/div[2]/table/tbody/tr[2]/*/div/a'))).click()
    #     time.sleep(1)
    #     WebDriverWait(driver, 2).until(
    #         EC.element_to_be_clickable((By.XPATH,
    #                                     '//*[@class="master-1aeimV6uW7DfUyOVwa0L4k"]/div[1]/div/div[4]'))).click()
    #     time.sleep(1)
    #     WebDriverWait(driver, 2).until(
    #         EC.element_to_be_clickable((By.XPATH,
    #                                     '//*[@class="i-icon i-icon-close"]'))).click()
    # except:
    #     pass


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    df_last7days = pd.read_excel('D:/python_project/projects/region_adverting/last7days.xlsx')
    df_last7days = df_last7days.loc[df_last7days['线索阶段']=='新线索', ['电话', '广告主名称']]
    df_last7days.rename(columns={df_last7days.columns[0]: 'phone'}, inplace=True)
    df_last7days=df_last7days.astype({'phone':'str'})

    today = datetime.date.today()
    # 昨天时间
    yesterday = today - datetime.timedelta(days=1)
    # 昨天开始时间戳
    yesterday_start_time = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d'))*1000)
    # 昨天结束时间戳
    yesterday_end_time = int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))*1000)

    # 连接mongdb
    conn = pymongo.MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'
                               .format("cda_epmall", "3PUkMsWCTu424Renrffb",
                                       "dds-wz99d33fe2f72610a042-pub.mongodb.rds.aliyuncs.com",
                                       "3717", "epmall"))
    conn_epmall = conn["epmall"]
    col_make = conn_epmall['ccc_user']
    df_phone = pd.DataFrame(list(col_make.find({}, {'_id': 0, 'phone': 1, 'user_label': 1})))
    df_phone= df_phone.astype({'user_label': 'str'})
    df_phone=df_phone.loc[df_phone['user_label'].str.contains('1660787015271|1605508280387'),['phone','user_label']]
    df_phone=df_phone.loc[:,['phone']]

    df_adv2phone=pd.merge(df_phone,df_last7days,on='phone',how='inner')

    df_account2phone=pd.read_excel('account_log.xlsx')

    df_finall=pd.merge(df_adv2phone,df_account2phone,on='广告主名称',how='left')
    df_finall.drop_duplicates(subset=None, keep='first', inplace=True)

    df_finall.sort_values(by='广告主名称',ascending=True,inplace=True)
    df_finall.to_excel('已加微信详细.xlsx',index=False)



    df_log = df_finall.loc[:, ['邮箱', '密码']]
    df_log.drop_duplicates(inplace=True)
    for w in range(len(df_log)):
        chrome_options = Options()
        s = Service()
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", 'False')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument(f'user-agent={UserAgent().random}')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--window-size=1920x1080")
        driver = webdriver.Chrome(options=chrome_options, service=s)
        time.sleep(1)
        driver.get("https://feiyu.oceanengine.com/pc/login")  # 网址
        time.sleep(1)
        account_l = df_log.values[w][0]
        password_l = df_log.values[w][1]
        print(account_l+':')
        while True:
            try:
                login(account_l, password_l)
                break
            except:
                driver.refresh()
        while True:
            slide()
            try:
                WebDriverWait(driver, 10).until(
                    lambda x: x.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/span/label/input'))
                break
            except Exception as e:
               pass

        df_temp_all=df_finall.loc[(df_finall['邮箱']==account_l)&(df_finall['密码']),['广告主名称']]
        df_temp_all.drop_duplicates(subset=None, inplace=True)
        df_temp_list=df_temp_all['广告主名称'].values.tolist()
        for temp in df_temp_list:
            time.sleep(1)
            print(temp)
            js = 'window.open("https://feiyu.oceanengine.com/pc/login");'
            driver.execute_script(js)
            allhandles = driver.window_handles  # 获取当前窗口句柄
            if driver.current_window_handle == allhandles[1]:
                pass
            else:
                driver.switch_to.window(allhandles[1])  # 切换窗口
            while True:
                try:
                    loc(temp)
                    break
                except Exception as e:
                    driver.close()
                    allhandles1 = driver.window_handles
                    driver.switch_to.window(allhandles1[0])
                    time.sleep(0.5)
                    js1 = 'window.open("https://feiyu.oceanengine.com/pc/login");'
                    driver.execute_script(js1)
                    allhandles2 = driver.window_handles  # 获取当前窗口句柄
                    if driver.current_window_handle == allhandles2[1]:
                        pass
                    else:
                        driver.switch_to.window(allhandles2[1])  # 切换窗口
            df_phone_part=df_finall.loc[df_finall['广告主名称']==temp,['phone']]
            df_phone_list=df_phone_part['phone'].values.tolist()
            for s in df_phone_list:
                print(s)
                while True:
                    try:
                        loc2(s)
                        break
                    except Exception as e:
                        driver.refresh()
                        time.sleep(1)
                        while True:
                            try:
                                WebDriverWait(driver, 10).until(
                                    EC.element_to_be_clickable((By.XPATH,
                                                                '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/span/span[1]'))).click()
                                time.sleep(0.5)
                                WebDriverWait(driver, 10).until(
                                    EC.element_to_be_clickable((By.XPATH,
                                                                '/html/body/div[2]/div/div/div/div/div/div[6]/div'))).click()
                                break
                            except :
                                driver.refresh()
                                time.sleep(1)
                time.sleep(1)
            driver.close()
            driver.switch_to.window(allhandles[0])
        driver.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[4]/span').click()
        time.sleep(1)
        driver.quit()