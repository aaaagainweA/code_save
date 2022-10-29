import jsonpath
import pandas as pd

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import  By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
import time
import random
from selenium.webdriver.chrome.options import Options
import json
from fake_useragent import UserAgent

def get_stacks(distance):
    distance += 20
    '''
        拿到移动轨迹，模仿人的滑动行为，先匀加速后匀减速
        变速运动基本公式：
        ① v=v0+at       匀加速\减速运行
        ② s=v0t+½at²    位移
        ③ v²-v0²=2as    
     '''
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
    back_stacks = [-5, -5, -5,-5]
    return {'forward_stacks': forward_stacks, 'back_stacks': back_stacks}

def login(account,password):
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH,
                                                             '//*[@id="register-login-register-login"]/section/div[3]/div[1]/div[2]/div/input'))

    # 账号
    driver.find_element(By.XPATH,
                        '//*[@id="register-login-register-login"]/section/div[3]/div[1]/div[2]/div/input').send_keys(
        account)
    # 密码
    driver.find_element(By.XPATH,
                        '//*[@id="register-login-register-login"]/section/div[3]/div[2]/div/div/input').send_keys(
        password)
    # 勾选同意用户协议和隐私协议
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="register-login-register-login"]/section/div[4]/div/div/span'))).click()
    # 点击登录
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="register-login-register-login"]/section/div[6]/button'))).click()


def slide():
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH, '//*[@id="captcha-verify-image"]'))
    # 找到大图并截图
    img_back = driver.find_element(By.XPATH, '//*[@id="captcha-verify-image"]')
    img_back.screenshot('a.png')
    btn = driver.find_element(By.XPATH, '//*[@id="secsdk-captcha-drag-wrapper"]/div[2]')

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
    print(dictdata)
    # 字典中提取距离
    distance = int(dictdata['data']['recognition'])-8

    # 调用函数，拖拽滑块模拟人为滑动轨迹
    stacks = get_stacks(distance)

    # 根据滑动轨迹进行滑动
    forward_stacks = stacks['forward_stacks']
    back_stacks = stacks['back_stacks']

    # 找到滑动按钮，并点击与hole住
    time.sleep(0.2)
    ActionChains(driver).click_and_hold(btn).perform()
    time.sleep(0.2)

    # 开始循环向前滑动
    for forward_stack in forward_stacks:
        ActionChains(driver).move_by_offset(xoffset=forward_stack, yoffset=0).perform()
        time.sleep(0.1)
    # 开始循环向后滑动20
    for back_stack in back_stacks:
        ActionChains(driver).move_by_offset(xoffset=back_stack, yoffset=0).perform()
        time.sleep(0.1)

    time.sleep(0.2)
    ActionChains(driver).release().perform()


def get_access_token(auth,app_id,secret):
    import requests
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "oauth2/access_token/"
    url = open_api_url_prefix + uri
    data = {
        "app_id": app_id,
        "secret": secret,
        "grant_type": "auth_code",
        "auth_code": auth
    }
    header1={'Content-Type':'application/json'}
    rsp = requests.post(url, json=data,headers=header1)
    rsp_data = rsp.json()
    return rsp_data

if __name__ == '__main__':
    df_info=pd.read_excel(r'D:/python_project/projects/region_adverting/account_info.xlsx')
    df_account=df_info.loc[:,['邮箱','密码']]
    df_account.drop_duplicates(inplace=True)
    for w in range(len(df_account)):
        chrome_options = Options()
        s = Service()
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option("useAutomationExtension", 'False')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument(f'user-agent={UserAgent().random}')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--window-size=1920x1080")
        driver = webdriver.Chrome(options=chrome_options, service=s)
        time.sleep(1)
        driver.get("https://open.oceanengine.com/audit/oauth.html?app_id=1742583032734764&state=your_custom_params&scope=%5B2%2C4%2C10%2C12%2C691%5D&material_auth=1&redirect_uri=https%3A%2F%2Fwww.feiyulinedownload.cn&rid=6sv598m3xau")  # 网址
        account_l = df_account.values[w][0]
        password_l = df_account.values[w][1]
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
                    lambda x: x.find_element(By.XPATH, '/html/body/div[2]/div[2]/div/div[2]/button'))
                break
            except Exception as e:
                pass
        time.sleep(2)
        WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[2]/div/div[2]/button'))).click()

        try:
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/div[5]/div/div/div[3]/button[2]'))).click()
        except:
            pass
        time.sleep(5)
        new_url=driver.current_url
        auth_code=new_url.split('=')[-1]
        driver.quit()
        df_rec=df_info.loc[df_info['邮箱']==account_l,['app_id','secret']]
        app_id=df_rec.values[0][0]
        secret=df_rec.values[0][1]
        data_dict = get_access_token(auth_code,app_id,secret)
        print(data_dict)
        tokenlist = jsonpath.jsonpath(data_dict, '$..access_token')
        tokenlist1 = jsonpath.jsonpath(data_dict, '$..refresh_token')
        token = tokenlist[0]
        print(token)
        refresh_token=tokenlist1[0]
        df_tokens = pd.read_excel('D:/python_project/projects/region_adverting/token_save.xlsx')
        data={'邮箱':[account_l],'token':[token],'refresh_token':[refresh_token]}
        df_token=pd.DataFrame.from_dict(data)
        df_token_save=pd.concat([df_tokens,df_token])
        df_token_save.drop_duplicates(subset=['邮箱'],keep='last',inplace=True)
        df_token_save.to_excel('D:/python_project/projects/region_adverting/token_save.xlsx',index=False)