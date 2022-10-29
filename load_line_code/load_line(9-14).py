from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import  By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import json
import random
import os
from selenium.webdriver.chrome.options import Options
import datetime
import re
import requests
from urllib3 import encode_multipart_formdata
from copy import copy
import warnings
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
    back_stacks = [-10, -5, -5,-8]
    return {'forward_stacks': forward_stacks, 'back_stacks': back_stacks}

def login(account,password):
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH,
                                                             '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[3]/div/span[1]/label/input'))
    # 账号
    driver.find_element(By.XPATH,
                        '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[3]/div/span[1]/label/input').send_keys(
        account)
    # 密码
    driver.find_element(By.XPATH,
                        '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[3]/div/span[2]/span[1]/label/input').send_keys(
        password)
    # 勾选同意用户协议和隐私协议
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[3]/label/span[1]'))).click()
    # 点击登录
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[2]/div[3]/button'))).click()


def slide():
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH, '//*[@id="captcha-verify-image"]'))
    # 找到大图并截图
    img_back = driver.find_element(By.XPATH, '//*[@id="captcha-verify-image"]')
    # small_img=driver.find_element(By.XPATH,'//*[@id="captcha_container"]/div/div[2]/img[2]')
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
    print(dictdata)
    # 字典中提取距离
    distance = int(dictdata['data']['recognition'])

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
    # ActionChains(driver).move_by_offset(xoffset=distance, yoffset=0).perform()
    ActionChains(driver).release().perform()

def loc(temp):
    WebDriverWait(driver, 10).until(
        lambda x: x.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/span/label/input'))
    # 搜索框输入租户
    driver.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/span/label/input').send_keys(temp)
    time.sleep(2)
    if temp == '武汉洪山医普茂医疗美容门诊部有限公司-TM-生活服务-1':
        locator = (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[2]')
        WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element(locator, temp))
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[2]'))).click()
    else:
        locator = (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[1]')
        WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element(locator, temp))
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[1]'))).click()
    # 点击选择的租户
    # locator = (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[1]')
    # WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element(locator, temp))
    # WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable(
    #         (By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[3]/div/div/div[1]'))).click()

    # 点击销售
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="primary-sales"]'))).click()
    time.sleep(1)
    # 点击近30天下拉框
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@id="jupiter_sub_app_container_sales"]/main/div/div[1]/div[2]/div/div[1]/div/span/span/span/label'))).click()

    # 选择近7天数据
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//div[@data-log-value="Last7Days"]'))).click()

    time.sleep(1)

    # 选中导处全部线索
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//button[@data-log-name="导出全部线索"]'))).click()
    try :
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH,'//button[@class="feiyu-byted-btn feiyu-byted-btn-size-md feiyu-byted-btn-type-primary feiyu-byted-btn-shape-angle feiyu-byted-can-input-grouped"]'))).click()
    except:
        pass


def download():
    #下载文件路径
    path = "D:/导线下载数据/"
    dfn = dfl.loc[(dfl['邮箱'] == account_l), ['用户名']]
    dfn.drop_duplicates(inplace=True)
    temp_l = []
    for username in dfn.values:
        if username[0] not in temp_l:
            temp_l.append(username[0])
    for i in temp_l:
        # 记录开始时下载文件数量
        dirlist1 = os.listdir(path)
        start = []
        for m in dirlist1:
            if m.endswith('.xlsx'):
                start.append(m)
        count1 = len(start)
        # 搜索内容拼接
        temp = i
        # 通过执行js，开启一个新的窗口
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
            except :
                print(temp+':定位失败----->>>页面刷新中')
                driver.close()
                driver.switch_to.window(allhandles[0])
                # 切换到第一个窗口
                time.sleep(0.5)
                # 通过执行js，开启一个新的窗口
                js = 'window.open("https://feiyu.oceanengine.com/pc/login");'
                driver.execute_script(js)
                allhandles = driver.window_handles  # 获取当前窗口句柄
                if driver.current_window_handle == allhandles[1]:
                    pass
                else:
                    driver.switch_to.window(allhandles[1])  # 切换窗口

        # 记录下载完成时文件数量
        count2 = 0
        while count2 <= count1:
            dirlist = os.listdir(path)
            end = []
            for n in dirlist:
                if n.endswith('.xlsx'):
                    end.append(n)
            count2 = len(end)
            # 下载完成后，关闭当前窗口
        print(temp+'----->>>下载完成')
        driver.close()
        driver.switch_to.window(allhandles[0])
        # 切换到第一个窗口
    while True:
        try:
            driver.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div[1]/div[4]/span').click()
            break
        except:
            driver.refresh()
    time.sleep(1)
    driver.quit()

def upload_file(file_path, wx_upload_url):
    file_name = file_path.split("/")[-1]
    with open(file_path, 'rb') as f:
        length = os.path.getsize(file_path)
        data = f.read()
    headers = {"Content-Type": "application/octet-stream"}
    params = {
        "filename": file_name,
        "filelength": length,
    }
    file_data = copy(params)
    file_data['file'] = (file_path.split('/')[-1:][0], data)
    encode_data = encode_multipart_formdata(file_data)
    file_data = encode_data[0]
    headers['Content-Type'] = encode_data[1]
    r = requests.post(wx_upload_url, data=file_data, headers=headers)
    media_id = r.json()['media_id']
    return media_id


def qi_ye_wei_xin_file(wx_url, media_id):
    headers = {"Content-Type": "text/plain"}
    data = {
        "msgtype": "file",
        "file": {
            "media_id": media_id
        }
    }
    r = requests.post(
        url=wx_url,
        headers=headers, json=data)

if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    #读取账号密码所在的excel
    dfl = pd.read_excel('account_detail.xlsx')
    dfx = dfl.loc[:, ['邮箱', '密码']]
    dfx.drop_duplicates(inplace=True)
    for w in range(len(dfx)):
        chrome_options = Options()
        s = Service(executable_path=r'C:\Program Files\Google\Chrome\Application\chromedriver.exe')
        prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'D:\\导线下载数据\\'}
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option("useAutomationExtension", 'False')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument(f'user-agent={UserAgent().random}')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--window-size=1920x1080")
        driver = webdriver.Chrome(options=chrome_options, service=s)
        time.sleep(1)
        driver.get("https://feiyu.oceanengine.com/pc/login")  # 网址
        time.sleep(1)
        account_l = dfx.values[w][0]
        password_l = dfx.values[w][1]
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
        download()


    now_time = datetime.datetime.now().strftime('%Y-%m-%d')

    path1 = "D:/导线下载数据/"
    allfile = os.listdir(path1)

    allfiles = []
    for i in allfile:
        if i.endswith('.xlsx'):
            allfiles.append(i)

    result = pd.DataFrame({})
    for file in allfiles:
        df = pd.read_excel(path1 + file)
        result = pd.concat([df, result])


    result.drop_duplicates(inplace=True)

    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    # 输出

    # 今天9点到今天14点
    start = str(yesterday) + ' 09:00:00'
    end = str(today) + ' 14:00:00'

    dfzz = result.loc[(result['线索创建时间'] >= start) & (result['线索创建时间'] < end), :]

    dfzz1 = dfzz.astype({'电话': str, '线索ID': str, '广告主ID': str, '广告计划ID': str, '组件ID': str})

    dfzz1.to_excel(f'D:/导线数据/{now_time}(9-14).xlsx', index=False)

    df1 = result.loc[
        (result['线索创建时间'] >= start) & (result['线索创建时间'] < end), ['姓名', '电话', '自动定位城市', '广告主名称', '线索创建时间', '补充信息']]

    df1.insert(5, '省', '')
    df1.insert(6, '市', '')
    df1.insert(7, '区/县', '')
    df1.insert(8, '客服手机号', '')


    # 省
    def extract_province(x: str):
        if str(x) != 'nan':
            y = str(x).split('-')
            return y[0]


    df1['省'] = df1['自动定位城市'].apply(lambda x: extract_province(x))


    # 市
    def extract_city(x: str):
        if str(x) != 'nan':
            y = str(x).split('-')
            return y[1]


    df1['市'] = df1['自动定位城市'].apply(lambda x: extract_city(x))

    df1['线索创建时间'] = df1['线索创建时间'].apply(lambda x: x[0:16])

    df1.rename(columns={df1.columns[1]: '电话号码', df1.columns[2]: '详细地址', df1.columns[3]: '用户名', df1.columns[4]: '客户填表时间',
                        df1.columns[9]: '所属产品'}, inplace=True)

    dfx = pd.read_excel('account_detail.xlsx')

    dfx1 = dfx.loc[:, ['用户名', '投放地域']]

    dfx1['市'] = dfx1['投放地域'].apply(lambda x: re.findall(r'[\u4e00-\u9fa5]+', x)[0])

    df_merge = pd.merge(df1, dfx1, on=['用户名', '市'], how='left')

    df_res = df_merge.loc[:, ['姓名', '电话号码', '详细地址', '投放地域', '客户填表时间', '省', '市', '区/县', '客服手机号', '所属产品']]

    df_res.rename(columns={'投放地域': '投放渠道'}, inplace=True)

    group1 = df_res[df_res['市'].isin(['武汉', '郑州', '长沙', '广州', '深圳', '东莞', '佛山', '珠海', '成都'])]

    group2 = df_res[
        df_res['市'].isin(['杭州', '温州', '北京', '台州', '丽水', '金华', '绍兴', '宁波', '无锡', '南京', '苏州', '衢州', '湖州', '贵阳', '西安'])]

    group1.to_excel(f'D:/导线数据/{now_time}(9-14)组一.xlsx', index=False)

    group2.to_excel(f'D:/导线数据/{now_time}(9-14)组二.xlsx', index=False)

    # 删除文件
    def del_file(path):
        for j in os.listdir(path):
            file_data = path + "\\" + j
            if  j.endswith('.xlsx'):
                os.remove(file_data)
    del_file(path1)


    test_report = f'D:/导线数据/{now_time}(9-14).xlsx'
    test_report1 = f'D:/导线数据/{now_time}(9-14)组一.xlsx'
    test_report2 = f'D:/导线数据/{now_time}(9-14)组二.xlsx'

    wx_api_key = 'a26d64bd-9768-471c-8579-aabec1195d16'
    wx_upload_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={}&type=file".format(wx_api_key)
    wx_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={}'.format(wx_api_key)

    media_id = upload_file(test_report, wx_upload_url)
    qi_ye_wei_xin_file(wx_url, media_id)

    media_id1 = upload_file(test_report1, wx_upload_url)
    qi_ye_wei_xin_file(wx_url, media_id1)

    media_id2 = upload_file(test_report2, wx_upload_url)
    qi_ye_wei_xin_file(wx_url, media_id2)
