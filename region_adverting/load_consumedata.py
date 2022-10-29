from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import  By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import random
import json
import os
import pandas as pd
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
                                                             '//*[@id="login"]/section/div[4]/div[1]/div/div/span/input'))


    # 账号
    driver.find_element(By.XPATH,
                        '//*[@id="login"]/section/div[4]/div[1]/div/div/span/input').send_keys(account)
    # 密码
    driver.find_element(By.XPATH,
                        '//*[@id="login"]/section/div[4]/div[2]/div/div/span/input').send_keys(password)
    # 勾选同意用户协议和隐私协议

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="login"]/section/div[8]/div/div'))).click()
    # 点击登录
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="login"]/section/div[7]/button'))).click()

def slide():
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.XPATH, '//*[@id="captcha-verify-image"]'))
    #找到大图并截图
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

def loc():
    #点击推广
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="bp-app"]/div[1]/div[2]/div[2]/div'))).click()

    # 点击日历
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="content"]/div/div[2]/div/div[2]/div/div[1]/div/span'))).click()
    time.sleep(0.2)
    # 选择昨天
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@id="content"]/div/div[2]/div/div[2]/div/div[2]/div[1]/div/a[2]'))).click()
    time.sleep(0.2)
    # 选中导处全部线索
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH,
                                    '//*[@id="content"]/div/div[3]/div[2]/div[1]/div/div[2]/button[1]'))).click()

def download():
    #下载文件路径
    path = "D:/下载数据/"

    # 记录开始时下载文件数量
    dirlist1 = os.listdir(path)
    start = []
    for m in dirlist1:
        if m.endswith('.xls'):
            start.append(m)
    count1 = len(start)

    while True:
        try:
            loc()
            break
        except:
            pass

    # 记录下载完成时文件数量
    count2 = 0
    while count2 <= count1:
        dirlist = os.listdir(path)
        end = []
        for n in dirlist:
            if n.endswith('.xls'):
                end.append(n)
        count2 = len(end)
    driver.close()

if __name__ == '__main__':

    chrome_options = Options()
    #读取账号密码所在的excel
    dfl = pd.read_excel('account_detail.xlsx')
    dfx = dfl.loc[:, ['邮箱', '密码']]
    dfx.drop_duplicates(inplace=True)
    for w in range(len(dfx)):
        prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'D:\\下载数据\\','download.prompt_for_download': False,
         'download.directory_upgrade': True,
         'safebrowsing.enabled': False,
         'safebrowsing.disable_download_protection': True}
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", 'False')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument(f'user-agent={UserAgent().random}')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--window-size=1920x1080")
        s = Service()
        driver = webdriver.Chrome(options=chrome_options, service=s)
        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': 'D:\\下载数据\\'}}
        command_result = driver.execute("send_command", params)
        print("response from browser:")
        for key in command_result:
            print("result:" + key + ":" + str(command_result[key]))
        time.sleep(1)
        driver.get("https://business.oceanengine.com/site/login")  # 网址
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
                    lambda x: x.find_element(By.XPATH, '//*[@id="bp-app"]/div[1]/div[2]/div[2]/div'))
                break
            except Exception as e:
                pass
        download()
        time.sleep(1)