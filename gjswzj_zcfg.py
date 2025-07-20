"""
全量采集国家税务总局网站-政策法规文件栏目数据
    web: 国家税务总局
    column: 通知通告
    url: https://fgk.chinatax.gov.cn/zcfgk/c100006/listflfg.html
    Collect date range: 全量
"""
import base64
import logging
import random
import re
import shutil
import threading
import warnings
from datetime import datetime
from fileinput import close
from threading import active_count
from urllib.parse import quote

from docutils.nodes import title
from selenium.common import ElementNotInteractableException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
import uuid
#配置日志信息
logging.basicConfig(filename='logs/gjswzj.log', level=logging.INFO, format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',encoding='utf-8')
datasource = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'dbtype': 'mysql',
    'db': 'student',
    'contentTable': 't_supply_zx_zfzcwj',
    'dataType': '',
    'cjwz': '国家税务总局',
    "charset":"utf8"
}
# 驱动路径
chrome_driver = r"D:\download\chromedriver.exe"
# 采集网页地址,政策导航部分栏目
public_url = [
    {
        "cjwy_lanmu":'法律', #73  ok
        "url":'https://fgk.chinatax.gov.cn/zcfgk/c100009/listflfg_fg.html',
    },
    {
        "cjwy_lanmu": '行政法规',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100010/listflfg_fg.html',
    },
    {
        "cjwy_lanmu": '国务院文件',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c102440/listflfg.html',
    },
    {
        "cjwy_lanmu": '财税文件',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c102416/listflfg.html',
    },
    {
        "cjwy_lanmu": '税务规范性文件',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100012/listflfg.html',
    },
    {
        "cjwy_lanmu": '其他文件',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100013/listflfg.html',
    },
    {
        "cjwy_lanmu": '工作通知',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100013/listflfg.html',
    },
    {
        "cjwy_lanmu": '税务部门规章',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100011/list_guizhang.html',
    },
]

base_url = 'https://fgk.chinatax.gov.cn'

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
]

headers = {
    # "host":"www.chinatax.gov.cn",
    # "origin":"https://fgk.chinatax.gov.cn",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "connection": "keep-alive",
    "referer":"https://fgk.chinatax.gov.cn/",
    "user-agent":random.choice(user_agents),
}
#
path_wkhtmltopdf = r'D:\download\wkhtmltopdf\bin\wkhtmltopdf.exe'
# 64位ID的划分
WORKER_ID_BITS = 5
DATACENTER_ID_BITS = 5
SEQUENCE_BITS = 12

# 最大取值计算
MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS)  # 2**5-1 0b11111
MAX_DATACENTER_ID = -1 ^ (-1 << DATACENTER_ID_BITS)

# 移位偏移计算
WOKER_ID_SHIFT = SEQUENCE_BITS
DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS

# 序号循环掩码
SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

# Twitter元年时间戳
TWEPOCH = 1288834974657

logger = logging.getLogger('flask.app')

class IdWorker(object):
    """
    用于生成IDs
    """

    def __init__(self, datacenter_id, worker_id, sequence=0):
        """
        初始化
        :param datacenter_id: 数据中心（机器区域）ID
        :param worker_id: 机器ID
        :param sequence: 其实序号
        """
        # sanity check
        if worker_id > MAX_WORKER_ID or worker_id < 0:
            raise ValueError('worker_id值越界')

        if datacenter_id > MAX_DATACENTER_ID or datacenter_id < 0:
            raise ValueError('datacenter_id值越界')

        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = sequence

        self.last_timestamp = -1  # 上次计算的时间戳

    def _gen_timestamp(self):
        """
        生成整数时间戳
        :return:int timestamp
        """
        return int(time.time() * 1000)

    def get_id(self):
        """
        获取新ID
        :return:
        """
        timestamp = self._gen_timestamp()

        # 时钟回拨
        if timestamp < self.last_timestamp:
            logging.error('clock is moving backwards. Rejecting requests until{}'.format(self.last_timestamp))
            raise Exception

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK
            if self.sequence == 0:
                timestamp = self._til_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        new_id = ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT) | (self.datacenter_id << DATACENTER_ID_SHIFT) | \
                 (self.worker_id << WOKER_ID_SHIFT) | self.sequence
        return new_id

    def _til_next_millis(self, last_timestamp):
        """
        等到下一毫秒
        """
        timestamp = self._gen_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._gen_timestamp()
        return timestamp

idwork = IdWorker(1, 1, 0)

import pymysql
import datetime
#创建chrom浏览器实例，同时传入 chromoptions
def init_driver(folder_name):
    chrome_options = Options()
    #Dom加载完就采集
    chrome_options.page_load_strategy = 'eager'
    # # 启用无头模式，后台运行
    # chrome_options.add_argument("--headless")
    # 创建 Service 对象
    service = Service(chrome_driver)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # 禁用自动化控制标志
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    prefs = {
        "download.default_directory": folder_name,  # 设置下载路径
        #允许多个文件同时下载而不会弹出提示框
        'profile.default_content_settings.popups': 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
        # "plugins.plugins_disabled": ["Chrome PDF Viewer"],# 禁用 PDF 预览插件
        "plugins.always_open_pdf_externally": True,  # 强制 PDF 下载
        "download.prompt_for_download": False,  # 禁用下载提示
        "download.directory_upgrade": True,  # 启用新下载目录
        "safebrowsing.enabled": True  # 启用安全浏览
    }
    chrome_options.add_experimental_option("prefs", prefs)
    #创建 chrom 浏览器实例，同时传入chromeOptions
    return webdriver.Chrome(service=service,options=chrome_options)

def insertGgContent(newsData):
    # 创建mysql链接
    conn = pymysql.connect(host=datasource.get('host'),
                           port=datasource.get('port'),
                           user=datasource.get('user'),
                           password=datasource.get('password'),
                           db=datasource.get('db'))
    # 光标对象
    cur = conn.cursor()

    # 执行连接数据库
    cur.execute('USE ' + datasource.get('db'))

    newsData.setdefault('createTime', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    insertSql = (
            "INSERT INTO " + datasource.get("contentTable") +
            " (id, title, ly, lx, fbrq, fj, wjnr, cjwz, cjwy_lanmu, cjwydz, cjwy_title, cjwy_keywords, cjwy_description, cjwy_publishdate, zbdw, fwjg, zcwh, syh, cjwy_mbx, create_time) " +
            "VALUES (%(id)s, %(title)s, %(ly)s, %(lx)s, %(fbrq)s, %(fj)s, %(wjnr)s, %(cjwz)s, %(cjwy_lanmu)s, %(cjwydz)s, %(cjwy_title)s, %(cjwy_keywords)s, %(cjwy_description)s, %(cjwy_publishdate)s, %(zbdw)s, %(fwjg)s, %(zcwh)s, %(syh)s, %(cjwy_mbx)s, %(createTime)s)"
    )
    # 执行插入操作
    cur.execute(insertSql,newsData)

    #提交事务并关闭文件连接
    conn.commit()
    cur.close()
    conn,close()
#插入文件关联表
def insertFileInfo(fileInfo):
    # 创建mysql链接
    conn = pymysql.connect(host=datasource.get('host'),
                           port=datasource.get('port'),
                           user=datasource.get('user'),
                           password=datasource.get('password'),
                           db=datasource.get('db'))
    # 光标对象
    cur = conn.cursor()


    # 执行连接数据库
    cur.execute('USE ' + datasource.get('db'))
    fileInfo.setdefault('id', idwork.get_id())
    insertSql = ("INSERT INTO " + 't_supply_zx_acq_file_zfzcwj'+
                 " (id, table_name, table_id, file_type, file_name,chinese_file_name,chinese_file_path,is_main_file,file_path,href,create_time) " +
                 "VALUES (%(id)s, %(tableName)s, %(tableId)s,%(fileType)s,%(fileName)s,%(chinese_file_name)s,%(chinese_file_path)s,%(isMainFile)s,%(filePath)s,%(href)s,%(createTime)s)")

    # 执行插入操作
    cur.execute(insertSql, fileInfo)

    # 提交事务并关闭连接
    conn.commit()
    cur.close()
    conn.close()

def is_exists_data(query_param):
    is_exists = False
    #创建mysql连接
    conn = pymysql.connect(host=datasource.get('host'),port=datasource.get('port'),user=datasource.get('user'),
                           passwd=datasource.get('password'),db=datasource.get('db'))
    #光标对象
    cur = conn.cursor()
    #执行连接数据库
    cur.execute('USE ' + datasource.get('db'))
    queryOne = ("SELECT COUNT(*) FROM " + datasource.get("contentTable") +
                " WHERE title = %(title)s and fbrq = %(fbrq)s and cjwydz = %(cjwydz)s")
    cur.execute(queryOne,query_param)
    #检查数据库查询结果是否存在符合条件的记录
    if cur.fetchone()[0] !=0:
        is_exists = True
    return is_exists

def save_file(mainId:int,file_name:str,chinese_file_name:str,isMainFile:str,file_url:str,file_type:str,chinese_file_path:str):

   #下载文件并存储至t_supply_zx_acq_file_zcwj表中
   # 遇到：------替换/为-
       #清洗file_name
   retries = 0
   max_retries = 3
   while retries < max_retries:
        try:
            if isMainFile=='0':
                # 图片类型
                file_path = os.path.join(folder_name, file_name)
                time.sleep(5)
                session = requests.Session()
                response = session.get(file_url, stream=True,headers=headers)
                response.raise_for_status()
                # 下载文件
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                logging.info(f"成功图片: {file_name}")
                # 例：图片：file_name:./97274949258.jpg
            fileInfo = {
                'tableName': datasource.get("contentTable"),
                'tableId': mainId,
                'fileType': file_type,
                'fileName': file_name,
                'chinese_file_name': chinese_file_name,
                'isMainFile': isMainFile,
                'href': './'+file_name,
                'filePath': file_name,
                'chinese_file_path':chinese_file_path,
                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            insertFileInfo(fileInfo)
            retries = 3
            return fileInfo
        except Exception as e:
            retries += 1
            if retries < max_retries:
                time.sleep(5)
            logging.info(f"发生错误: {e}. 重试次数: {retries + 1}/{max_retries}")


def save_title_file_floder(title_path,file,file_name):
    """额外存入文章文件夹当中"""
    try:
        #清洗标题
        chinese_file_name = re.sub(r'[<>:"/\\|?*;\'\[\]{}(),;:·]', '_', file_name)
        # 复制到文章文件夹，并且修改为中文名称
        #下载的路径
        file_path = os.path.join(folder_name, file)
        #需要复制过去的路径
        chinese_file_folder = os.path.join(title_path, file)
        #复制文件到相应路径
        shutil.copy(file_path, chinese_file_folder)
        #更改为中文名
        #中文名称路径
        chinese_file_path = os.path.join(title_path, chinese_file_name)
        os.rename(chinese_file_folder, chinese_file_path)
        logging.info(f"文件 {file_name} 已复制完成复制并改名成功")
        return chinese_file_path,chinese_file_name
    except Exception as e:
        logging.error(e)
        return None, None

# 方法1：重定向 stderr 到空设备（跨平台）
# def silence_stderr():
#     # Windows
#     if os.name == 'nt':
#         import sys
#         sys.stderr = open(os.devnull, 'w')
#     # Linux/macOS
#     else:
#         import sys
#         sys.stderr = open('/dev/null', 'w')


import pdfkit
# 获取文章详情内容
def get_content(driver, url,title, save_folder):
    global fj_count
    content_data = {
        'id': '',  # 主键id
        'wjnr': '',  # 文件内容
        'lx': '',  # 主题分类
        'ly': '',  # 来源
        'fj': '',  # 附件
        'cjwy_title': '',  # 采集网页标题
        'cjwy_keywords': '',  # 采集网页keywords
        'cjwy_description': '',  # 采集网页description
        'cjwy_publishdate': '',  # 采集网页publishdate
        'zbdw': '',  # 主办单位
        'fwjg': '',  # 发文机构
        'syh': '',  # 索引号
        'cjwy_mbx': ''  # 采集网页面包屑
    }
    # 确保保存文件夹存在
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    try:
        #打开新窗口并切换到新窗口
        original_window = driver.current_window_handle
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)
        time.sleep(2)  # 等待页面加载

        # 更新 content_data 中的相关信息
        mainId = idwork.get_id()
        content_data['id'] = mainId
        # # 配置 wkhtmltopdf 的选项
        # options = {
        #     'page-size': 'A3',  # 设置页面大小为 A4
        #     'margin-top': '0.75in',
        #     'margin-right': '0.75in',
        #     'margin-bottom': '0.75in',
        #     'margin-left': '0.75in',
        #     'encoding': "UTF-8",
        #     'no-outline': None,
        #     'quiet': None,
        #     'viewport-size': '1920x1080',  # 设置视口大小为 1920x1080
        # }
        # config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
        try:
            time.sleep(2)
            #请求下载
            # pdfkit.from_url(url, f'{folder_name}\\{title}.pdf', configuration=config,options=options)
            # 保存为PDF
            pdf = driver.execute_cdp_cmd('Page.printToPDF', {
                'landscape': False,  # 纵向模式（True=横向）
                'displayHeaderFooter': False,  # 不显示页眉页脚
                'printBackground': True,  # 打印背景
                'preferCSSPageSize': False,  # 必须设为False才能自定义尺寸
                'paperWidth': 11.69,  # A3 宽度（英寸，297mm ≈ 11.69in）
                'paperHeight': 16.54,  # A3 高度（英寸，420mm ≈ 16.54in）
                'marginTop': 0,  # 上边距（英寸）
                'marginBottom': 0,
                'marginLeft': 0,
                'marginRight': 0,
            })
            with open(f'{folder_name}\\{title}.pdf', 'wb') as f:
                f.write(base64.b64decode(pdf['data']))
        except Exception as e:
            if "ContentNotFoundError" in str(e):
                pass
            else:
                logging.info(f"请检查URL{e}")
            logging.info(f'成功下载正文pdf：{title}.pdf')
        # 创建文章文件夹
        title_path = os.path.join(folder_name2, title)
        if not os.path.exists(title_path):
            os.makedirs(title_path)
        #保存到文章文件夹
        title_chinese_file_path,title_chinese_file_name=save_title_file_floder(title_path,f'{title}.pdf',f'{title}.pdf')
        save_file(mainId=mainId,file_name=title+'.pdf',chinese_file_name=title_chinese_file_name,
                  isMainFile='1',file_url='',file_type='pdf',chinese_file_path=title_chinese_file_path)
        # 面包屑
        mbx = driver.find_elements(By.CSS_SELECTOR,'.position a')
        content_data['cjwy_mbx'] = '>'.join([
            elem.get_attribute("textContent").strip() for elem in mbx if elem.get_attribute("textContent").strip()])
        #来源
        content_data['ly'] = ''

        #采集网页发布日期
        content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="PubDate"]').get_attribute("content")
        content = driver.find_element(By.CLASS_NAME, 'arc_cont')
        #存储完整的html
        content_data['wjnr'] = content.get_attribute('outerHTML')
        # 采集网页title
        content_data['cjwy_title'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="ArticleTitle"]').get_attribute("content")
        # 采集网址
        content_data['cjwydz'] = url
        # print(url)
        # # 网页title
        # content_data['cjwy_title'] = driver.find_element(By.CSS_SELECTOR,'li.first > div > p').text
        #主办单位
        content_data['zbdw'] = ''
        # 网页keywords
        try:
            content_data['cjwy_keywords'] = driver.find_element(By.NAME, 'ColumnKeywords').get_attribute('content')
        except NoSuchElementException:
            content_data['cjwy_keywords'] = ''
        # 网页description
        try:

            content_data['cjwy_description'] = driver.find_element(By.NAME, 'ColumnDescription').get_attribute('content')
        except NoSuchElementException:
            content_data['cjwy_description'] = ''
        fj =[]
        try:
            file_elements=driver.find_elements(By.XPATH,'//*[@class="arc_cont"]//a')
            if file_elements:
                for file in file_elements:
                    file_url = file.get_attribute('href')
                    file_name = file.text
                    if file_url:
                        if file_url.startswith('http'):
                            file_url=file_url
                        else:
                            split_url = url[:url.rfind('/') + 1]
                            file_url = split_url + file_url
                        if file_name:
                            file_name = ''.join(file_name)
                        else:
                            file_name = file_url.split("/")[-1]
                        if file_url.endswith('.tml') or file_url.endswith('.htm') or file_url.endswith('/') or '@' in file_url or 'javascript:void(0)' in file_url:
                            logging.info('该链接为文章跳转页面，已跳过')
                            continue
                        #附件为图片的情况：
                        if file_url.endswith('.jpg') or file_url.endswith('.jpeg') or file_url.endswith('.png'):
                            img_url = file_url.replace('https', 'http')
                            img_base_name=file_url.split("/")[-1]
                            img_name= re.sub(r'[<>:"/\\|?*\x00-\x1f]', '-', file_name).strip()
                            file_type = img_name.split(".")[-1]
                            if not img_name.endswith(file_type):
                                img_name = img_name+'.'+file_type
                                #先下载中文名称的图片后改为
                            chinese_file_path, chinese_file_name = save_title_file_floder(title_path,img_base_name,img_name)
                            save_file(mainId, file_name=img_base_name,chinese_file_name=chinese_file_name,isMainFile='0',
                                      file_url=img_url,file_type=file_type,chinese_file_path=chinese_file_path)
                            #改为中文名称
                            os.rename(os.path.join(title_path, img_base_name),
                                      os.path.join(title_path, img_name))
                            fj_count += 1
                        else:
                            try:
                                # 获取未下载前的文件名列表
                                existing_files = set(os.listdir(folder_name))
                                content_window = driver.current_window_handle
                                # 新窗口打开
                                driver.execute_script("arguments[0].setAttribute('target', '_blank');", file)
                                driver.execute_script("arguments[0].click();", file)
                                time.sleep(2)
                                # 记录文章窗口
                                file_windows = driver.window_handles
                                # 切换到最新窗口
                                driver.switch_to.window(file_windows[-1])
                                if driver.title== '404':
                                    logging.info(f'{file_name}文件不存在!')
                                    driver.close()
                                    driver.switch_to.window(content_window)
                                else:
                                    # 切回窗口
                                    driver.switch_to.window(content_window)
                                    # 等待文件下载完成，获取下载后文件名列表
                                    while True:
                                        current_files = set(os.listdir(folder_name))  # 重新获取文件列表
                                        downloading_files = {f for f in current_files if
                                                             f.endswith(".crdownload")}  # 找出正在下载的文件
                                        if not downloading_files:  # 如果没有正在下载的文件，说明下载完成
                                            break
                                        time.sleep(2)  # 等待2秒后再次检查
                                    # 找出新下载的文件
                                    current_files = set(os.listdir(folder_name))
                                    new_files = current_files - existing_files
                                    if len(new_files) ==0:
                                        logging.info(f'文章：{url}文件不存在或为其他链接:{file_url}')
                                    else:
                                        downloaded_file = new_files.pop()
                                        file_type=downloaded_file.split('.')[-1]
                                        # 重命名文件,原文件名为中文
                                        new_file_name =str(uuid.uuid4())[:8]+'.'+file_type # 重命名文件名
                                        os.rename(os.path.join(folder_name, downloaded_file),
                                                  os.path.join(folder_name, new_file_name))
                                        #复制到文章文件夹并且修改为中文名称
                                        if not file_name.endswith(file_type):
                                            chinese_file_name = file_name + '.' + file_type
                                        else:
                                            base_chinese_file_name=file_name
                                            chinese_file_name=re.sub(r'[<>:"/\\|?*\x00-\x1f]', '-', base_chinese_file_name).strip()
                                        chinese_file_path,chinese_file_name=save_title_file_floder(title_path, new_file_name,chinese_file_name)
                                        if chinese_file_path != None:
                                            fileInfo = {
                                                'tableName': datasource.get("contentTable"),
                                                'tableId': mainId,
                                                'fileType': file_type,
                                                'fileName': file_name,
                                                'chinese_file_name':chinese_file_name,
                                                'isMainFile': '0',
                                                'href': f'./{new_file_name}',
                                                'filePath': new_file_name,
                                                'chinese_file_path':chinese_file_path,

                                                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            }
                                            insertFileInfo(fileInfo)

                                            logging.info(f"成功下载文件: {file_name}")
                                            fj.append(file_name)
                                            fj_count += 1
                            except requests.RequestException as e:
                                logging.info(f"下载出错: {e}")

                content_data['fj'] = ';'.join(fj)
        except NoSuchElementException:
            pass
        #关闭新窗口并切换回原来的窗口
        time.sleep(1)
        driver.close()
        time.sleep(1)
        driver.switch_to.window(original_window)
        return content_data

    except NoSuchElementException:
        logging.error(f"未找到文章内容或页面结构有变化，请检查选择器。",exc_info=True)
        # 确保在异常情况下也能切换回原窗口
        try:
            driver.close()
            driver.switch_to.window(original_window)
        except Exception:
            pass
        return content_data
    except Exception as e:
        logging.error(f"Error when fetching article content from {url}: {e}",exc_info=True)
        # 确保在异常情况下也能切换回原窗口
        try:
            driver.close()
            driver.switch_to.window(original_window)
        except Exception:
            pass
        return content_data




def domain(url,lanmu,folder_name):
    try:
        global total_count, fj_count, lanmu_count
        lanmu_count=0
        driver = init_driver(folder_name)
        driver.get(url)
        logging.info(f'开始采集网站：栏目为：{lanmu}———{url}')
        #退出循环变量
        stop_crawling = False
        while not stop_crawling:
            #强制等待加显式等待
            time.sleep(5)
            active_report_tag = WebDriverWait(driver,10).until(
                EC.presence_of_element_located((By.CLASS_NAME,'list'))
            )
            # time.sleep(1)
            # page = driver.find_element(By.XPATH,'//*[@class="layui-laypage-curr"]/em[2]').text
            # logging.info(f'开始采集第————{page}页内容')
            #获取数据列表
            li_tags = active_report_tag.find_elements(By.XPATH,"./ul/li")
            for li_tag in li_tags:
                time.sleep(1)
                title_tag = li_tag.find_element(By.XPATH,'./p[2]/a')
                try:
                    fbrq_tag = li_tag.find_element(By.CLASS_NAME,'cwrq')
                except NoSuchElementException:
                    try:
                        fbrq_tag = li_tag.find_element(By.XPATH, './p[2]/span')
                    except NoSuchElementException:
                        pass
                try:
                    zcwh=li_tag.find_element(By.CLASS_NAME,'fwzh ')
                    # 政策文号
                    zcwh = zcwh.text
                except NoSuchElementException:
                    zcwh=None
                    pass
                newsData = {}
                #文章href
                href = title_tag.get_attribute('href')
                if href and href.startswith('http'):
                    cjwydz=href
                else:
                    cjwydz=base_url+href
                #文章title
                base_title = title_tag.text

                title=re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', base_title)
                #文章发布日期
                fbrq = fbrq_tag.text
                #查看数据是否存在
                query_param = {
                    'title': title,
                    'fbrq': fbrq,
                    'cjwydz':cjwydz,
                }
                flag = is_exists_data(query_param)

                #存在则跳过
                if flag:
                    continue
                stop_crawling = False
                logging.info(f'开始采集栏目————{lanmu}————标题————{title}————内容,日期{fbrq},url:{cjwydz}')
                newsData['title'] = title
                newsData['fbrq'] = fbrq
                save_folder = os.path.join(os.path.dirname(__file__), folder_name)
                content_data = get_content(driver, href,title, save_folder)

                newsData['id'] = content_data.get('id')
                newsData['ly'] = content_data.get('ly')
                newsData['lx'] = content_data.get('lx')
                newsData['fj'] = content_data.get('fj')
                newsData['cjwz'] = datasource.get('cjwz')
                newsData['cjwy_lanmu'] = lanmu
                newsData['cjwydz'] = content_data.get('cjwydz')
                newsData['cjwy_title'] = content_data.get('cjwy_title')
                newsData['cjwy_keywords'] = content_data.get('cjwy_keywords')
                newsData['cjwy_description'] = content_data.get('cjwy_description')
                newsData['cjwy_publishdate'] = content_data.get('cjwy_publishdate')
                newsData['wjnr'] = content_data.get('wjnr')

                newsData['zbdw'] = content_data.get('zbdw')

                newsData['fwjg'] = content_data.get('fwjg')
                if zcwh is not None:
                    newsData['zcwh'] = zcwh
                else:
                    newsData['zcwh'] =content_data.get('zcwh')
                newsData['syh'] = content_data.get('syh')
                newsData['cjwy_mbx'] = content_data.get('cjwy_mbx')
                # 插入数据表
                insertGgContent(newsData)
                lanmu_count += 1
                total_count += 1
            # 遇到2016年前的数据就终止外层循环
            if stop_crawling:
                break

            if not stop_crawling:
                try:
                    #点击下一页
                    next_page_button = driver.find_element(By.XPATH,"//a[text()='下一页']")
                    # 检查是否包含禁用类（layui-disabled）
                    if "layui-disabled" in next_page_button.get_attribute("class"):
                        logging.info(f"{lanmu}已到末页，检索终止")
                        break
                    else:
                        next_page_button.click()
                except (NoSuchElementException, ElementNotInteractableException):
                    logging.info(f"{lanmu}已到末页，检索终止")
                    break
    finally:
        driver.quit()
if __name__ == "__main__":
    global total_content, total_count, fj_count,lanmu_count
    total_content = ''
    total_count = 0
    fj_count = 0
    try:
        #调用主函数
        logging.info("国家税务总局信息采集开始")
        for item in public_url[3:]:
            total_lanmu_content = ''
            lanmu = item['cjwy_lanmu']
            url=item['url']
            folder_name = os.path.join(os.path.dirname(__file__),'国家税务总局', lanmu)
            folder_name2 = os.path.join(os.path.dirname(__file__), '国家税务总局2', lanmu)
            domain(url, lanmu, folder_name)
            total_lanmu_content = f'栏目:{lanmu},采集{lanmu_count}条数据'
            logging.info(f"{total_content},采集结束")
        total_content=f'国家税务总局共采集{total_count}条数据,附件{fj_count}个'
        logging.info(total_content)
    except Exception as e:
        logging.error(f"任务执行失败，错误信息：{e}",exc_info=True)