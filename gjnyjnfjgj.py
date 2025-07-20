"""
国家能源局南方监管局政策文件数据
    web: 国家能源局南方监管局
    column: 通知公示，政策法规，政策解读
    url: https://nfj.nea.gov.cn/xwzx/tzgg/，https://nfj.nea.gov.cn/xxgk/zcfg/，https://nfj.nea.gov.cn/xxgk/zcjd/
    Collect date range: 2022年至今
"""
import config
import logging
import re
import shutil
import base64
import threading
import uuid
import urllib
import hmac
import hashlib
import common
from datetime import datetime
from requests import request
from fileinput import close
from threading import active_count

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
# from sqlalchemy import false
from urllib.parse import unquote
#配置日志信息
logging.basicConfig(filename='logs/gjnyjnfjgj.log', level=logging.INFO, format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',encoding='utf-8')
datasource = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'dbtype': 'mysql',
    'db': 'wbsj',
    'contentTable': 't_supply_zx_zfzcwj_copy2',
    'dataType': '',
    'cjwz': '国家能源局南方监管局'
}
# 驱动路径
chrome_driver = r"D:\programfiles\chromedriver-win64\chromedriver-win64\chromedriver.exe"
cjwz = '国家能源局南方监管局'

current_dir = os.path.dirname(__file__)
# 文件存放文件夹
# folder_name = os.path.join(current_dir, "政策文件")

folder_name = f"政策文件"
# 增量开关
is_incremental = True
# 增量采集结果
total_content = ''
# 增量值
total_count = 0

# 采集网页地址
# basicUrl = 'https://nfj.nea.gov.cn/xwzx/tzgg/'

#采集网页栏目
column_list = [
    {
        "cjwy_lanmu": '通知公示',
        "url": 'https://nfj.nea.gov.cn/xwzx/tzgg/'
    },
    {
        "cjwy_lanmu": '政策法规',
        "url": 'https://nfj.nea.gov.cn/xxgk/zcfg/'
    },
    {
        "cjwy_lanmu": '政策解读',
        "url": 'https://nfj.nea.gov.cn/xxgk/zcjd/'
    }
]


# syy服务器文件存放路径
syy_file_path = '/hlpt/pmds/LawData/ldjhjqbyw/'

# 文件存放文件夹
folder_name = f"政策文件"

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

def increment_title(title):
    # 使用正则表达式提取标题中的编号
    match = re.search(r'\((\d+)\)$', title)
    if match:
        # 如果找到编号，提取编号并递增
        counter = int(match.group(1)) + 1
        title = re.sub(r'\(\d+\)$', f'({counter})', title)
    else:
        # 如果没有找到编号，添加编号
        title = f"{title}(1)"
    return title
# 获取文章详情内容
def get_content(driver,url,title,save_folder):
    content_data = {
        'id': '',  # 主键id
        'wjnr': '',  # 文件内容
        'title': '',  #标题
        'last_title': '',  # 标题
        'ly': '',    #来源
        'lx': '',  #主题分类
        'fbrq': '',  #发布时间
        # 'content': '',  #正文
        'fj': '', # 附件
        'zbdw': '',  #主办单位
        'fwjg': '',  #发文机构
        'zcwh': '',  #政策文号
        'syh': '', #索引号
        'cjwydz': '',  # 采集网页地址
        'cjwy_title': '',   #采集网页title
        'cjwy_keywords': '',  # 采集网页keywords
        'cjwy_description': '',  # 采集网页description
        'cjwy_publishdate': '',  # 采集网页publishdate
        'cjwy_mbx': ''  # 采集网页面包屑
    }
    # # 确保保存文件夹存在
    # if not os.path.exists(folder_name2):
    #     os.makedirs(folder_name2)

    # # 确保保存文件夹存在
    # if not os.path.exists(folder_name):
    #     os.makedirs(folder_name)
    # # 确保保存文件夹存在
    # if not os.path.exists(folder_name2):
    #     os.makedirs(folder_name2)

    # 创建文章文件夹，处理标题相同的情况
    # title_path = os.path.join(folder_name2, title)
    # if not os.path.exists(title_path):
    #     os.makedirs(title_path, exist_ok=True)
    file_infos = []
    # 确保保存文件夹存在
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # 更新 content_data 中的相关信息
    mainId = config.idwork.get_id()
    content_data['id'] = mainId
    # 采集网址
    content_data['cjwydz'] = url
    try:
        #打开新窗口并切换到新窗口
        original_window = driver.current_window_handle
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)
        time.sleep(2)  # 等待页面加载
        # 下载正文pdf
        try:
            time.sleep(1)
            #请求下载
            # pdfkit.from_url(url, f'{folder_name}\\{title}.pdf', configuration=config,options=options)
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
            # 初始化文件名和路径
            file_namepdf = f"{title}.pdf"
            # file_path1 = os.path.join(folder_name, file_namepdf)

            file_id = config.idwork.get_id()
            # common.deal_with_file(file_url, save_folder, file_namepdf, file_id)
            # 获取当前时间并格式化为所需的字符串
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
            base_name, extension = os.path.splitext(file_namepdf)
            # 为文件名添加时间戳后缀
            current_time_file_name = f"{base_name}_{timestamp}{extension}"
            file_path = os.path.join(save_folder, current_time_file_name)

            with open(file_path, 'wb') as f:
                f.write(base64.b64decode(pdf['data']))
            # 复制到第二个文件夹
            # file_path2 = os.path.join(title_path,file_namepdf)
            # shutil.copy(file_path1,file_path2)
            chinese_file_path = os.path.join('政策文件', cjwz, lanmu, file_namepdf)
            logging.info(f'成功下载正文pdf：{file_namepdf}')
            # 设置默认键值对（如果键不存在）
            fileInfo = {
                'id': file_id,
                'tableName': datasource.get("contentTable"),
                'tableId': mainId,
                'fileType': 'pdf',
                'fileName': file_namepdf,
                'chinese_file_name': file_namepdf,
                'isMainFile': 1,
                'href': './'+ file_namepdf,
                'filePath': file_namepdf,
                'chinese_file_path':chinese_file_path,
                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            common.insertFileInfo(fileInfo,datasource)

        except Exception as e:
            logging.info(f"下载正文pdf出错：{e}")

        # 面包屑
        try:
            mbx = driver.find_element(By.CLASS_NAME, 'now-site').text
            content_data['cjwy_mbx'] = mbx.replace('您当前位置：', '').strip()
        except NoSuchElementException:
            pass
        # 文件内容（html）
        try:
            content = driver.find_element(By.CLASS_NAME, 'wrapper_detail_text')
        except NoSuchElementException:
            try:
                content = driver.find_element(By.CLASS_NAME, 'detail')
            except NoSuchElementException:
                try:
                    content = driver.find_element(By.ID, 'detail')
                except NoSuchElementException:
                    content = driver.find_element(By.CLASS_NAME, 'article-content')
        #存储完整的html
        content_data['wjnr'] = content.get_attribute('outerHTML')

        # 来源
        try:
            deatil_date = driver.find_element(By.CSS_SELECTOR,'.wrapper_deatil_date').text
            content_data['ly'] = deatil_date.split('来源：')[-1].strip()
        except NoSuchElementException:
            try:
                content_data['ly'] = driver.find_element(By.CSS_SELECTOR, '.author').text.split('来源：')[-1].strip()
            except NoSuchElementException:
                content_data['ly'] = driver.find_element(By.NAME, 'ContentSource').get_attribute('content')

        #采集网页生成日期
        try:
            deatil_date = driver.find_element(By.CSS_SELECTOR,'.wrapper_deatil_date').text
            content_data['cjwy_publishdate'] = deatil_date.split('来源：')[0].strip()
        except NoSuchElementException:
            try:
                content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="PubDate"]').get_attribute('content')
            except NoSuchElementException:
                content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="publishdate"]').get_attribute('content')
                if not content_data['cjwy_publishdate']:
                    content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, '.times').text.split('发布时间：')[-1].strip()
        # 发文机构
        content_data['fwjg'] = ''

        #政策文号
        content_data['zcwh'] = ''
        #主题分类
        content_data['lx'] = ''
        #索引号
        try:
            content_data['syh'] = driver.find_element(By.CSS_SELECTOR,'table.table1 > tbody > tr:nth-child(2) > td:nth-child(2)').text.strip()
        except NoSuchElementException:
            content_data['syh'] = ''

        # 采集网页title
        try:
            content_data['cjwy_title'] = driver.find_element(By.CSS_SELECTOR,'.wrapper_deatil_title').text.strip()
        except NoSuchElementException:
            try:
                content_data['cjwy_title'] = driver.find_element(By.CSS_SELECTOR,'tbody > tr:nth-child(1) > td.STYLE9').text.strip()
            except NoSuchElementException:
                content_data['cjwy_title'] = driver.find_element(By.CSS_SELECTOR,'.titles').text.strip()
        #主办单位
        try:
            content_data['zbdw'] = driver.find_element(By.CSS_SELECTOR,'table.table1 > tbody > tr:nth-child(2) > td:nth-child(4)').text.strip()
        except NoSuchElementException:
            content_data['zbdw'] = ''
        # 网页keywords
        try:
            content_data['cjwy_keywords'] = driver.find_element(By.NAME, 'keywords').get_attribute('content')
        except NoSuchElementException:
            pass
        # 网页description
        try:
            content_data['cjwy_description'] = driver.find_element(By.NAME, 'description').get_attribute('content')
        except NoSuchElementException:
            pass

        file_elements = content.find_elements(By.TAG_NAME, 'a')
        fj = ''
        #下载附件
        if file_elements:
            for file in file_elements:
                file_url = file.get_attribute('href')
                basic_filename = file.text
                if not basic_filename:
                    basic_filename = file.get_attribute('title')
                # 中文文件名称
                chinese_filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', basic_filename).strip()
                #判断是否需要下载文件
                if file.get_attribute('needdownload') == 'false':
                    continue
                if not file_url or 'javascript:void(0)' in file_url or '.htm' in file_url:
                    continue

                if file_url:

                    # 生成的文件名
                    origin_file_name = file_url.split('/')[-1]
                    base_name, ext = os.path.splitext(origin_file_name)

                    # 处理没后缀中文文件名
                    if '.' in chinese_filename:
                        ext_part = chinese_filename.split('.')[-1]
                        # 检查扩展名是否纯英文
                        if not ext_part.isascii() or not ext_part.isalpha():
                            chinese_filename = chinese_filename + ext
                    else:
                        chinese_filename = chinese_filename + ext

                    file_id = config.idwork.get_id()
                    common.deal_with_file(file_url, save_folder, chinese_filename, file_id)
                    # chinese_file_path = os.path.join('政策文件\\', cjwz, lanmu, file_namepdf)
                    filePath = os.path.join('政策文件', cjwz, lanmu, chinese_filename)
                #     #设置默认键值对（如果键不存在）
                    fileInfo = {
                        'id': file_id,
                        'tableName': datasource.get("contentTable"),
                        'tableId': mainId,
                        'fileType': chinese_filename.split('.')[-1],
                        'fileName': basic_filename,
                        'chinese_file_name': chinese_filename,
                        'isMainFile': 0,
                        'href': './' + chinese_filename,
                        'filePath': chinese_filename,
                        'chinese_file_path': filePath,
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    fj += chinese_filename + ';'
                    #插入文件信息
                    common.insertFileInfo(fileInfo,datasource)
                    # try:
                    #     # 发送请求获取文件内容
                    #     response = requests.get(file_url, stream=True)
                    #     response.raise_for_status()
                    #     try:
                    #         # 生成的文件名
                    #         origin_file_name = file_url.split('/')[-1]
                    #         # 保存文件夹1
                    #         file_path1 = os.path.join(folder_name, origin_file_name)
                    #         base_name, ext = os.path.splitext(origin_file_name)
                    #
                    #         #处理没后缀中文文件名
                    #         if '.' in chinese_filename:
                    #             ext_part = chinese_filename.split('.')[-1]
                    #             # 检查扩展名是否纯英文
                    #             if not ext_part.isascii() or not ext_part.isalpha():
                    #                 chinese_filename = chinese_filename + ext
                    #         else:
                    #             chinese_filename = chinese_filename + ext
                    #         # 保存文件夹2
                    #         file_path2 = os.path.join(title_path, chinese_filename)
                    #         # 用数字递增处理文件夹1同名文件
                    #         counter = 1
                    #         while os.path.exists(file_path1):
                    #             file_path1 = os.path.join(folder_name, f"{base_name}_{counter}{ext}")
                    #             origin_file_name = file_path1.split('\\')[-1]
                    #             counter += 1
                    #
                    #         # 将内容写入本地文件
                    #         with open(file_path1, 'wb') as file:
                    #             #从网络逐块读取数据并写入本地文件，避免内存不足
                    #             for chunk in response.iter_content(chunk_size=8192):
                    #                 if chunk:
                    #                     file.write(chunk)
                    #
                    #     except FileNotFoundError:
                    #         # 如果路径无效，生成随机文件名重试
                    #         origin_file_name = file_url.split('/')[-1]
                    #         file_ext = os.path.splitext(origin_file_name)[1] if '.' in origin_file_name else ''
                    #         origin_file_name = f"{uuid.uuid4().hex}{file_ext}"
                    #         file_path1 = os.path.join(folder_name, origin_file_name)
                    #
                    #         with open(file_path1, 'wb') as file:
                    #             for chunk in response.iter_content(chunk_size=8192):
                    #                 if chunk:
                    #                     file.write(chunk)
                    #     #用数字递增处理文件夹2同名文件
                    #     counter = 1
                    #     base_name, ext = os.path.splitext(chinese_filename)
                    #     while os.path.exists(file_path2):
                    #         file_path2 = os.path.join(title_path, f"{base_name}_{counter}{ext}")
                    #         chinese_filename = file_path2.split('\\')[-1]
                    #         counter += 1
                    #     # 拷贝重命名原始文件
                    #     shutil.copy(file_path1,file_path2)  # 保留原始副本
                    #     #设置默认键值对（如果键不存在）
                    #     fileInfo = {
                    #         'tableName': datasource.get("contentTable"),
                    #         'tableId': mainId,
                    #         'fileType': origin_file_name.split('.')[-1],
                    #         'fileName': chinese_filename,
                    #         'chinese_file_name': chinese_filename,
                    #         'isMainFile': 0,
                    #         'href': './' + origin_file_name,
                    #         'filePath': origin_file_name,
                    #         'chinese_file_path': filePath,
                    #         "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    #     }
                    #     fj += chinese_filename + ';'
                    #     #插入文件信息
                    #     common.insertFileInfo(fileInfo,datasource)
                    #     logging.info(f"成功下载文件: {chinese_filename}")
                    # except requests.RequestException as e:
                    #     logging.info(f"下载文件时出错: {e}")
        content_data['fj'] = fj
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

def is_exists_data(query_param,datasource,check_title=True):
    is_exists = False

    # 创建mysql连接
    conn, cur = common.connect_to_mysql_with_retry(datasource)


    #执行连接数据库
    cur.execute('USE ' + datasource.get('db'))
    queryOne = ("SELECT COUNT(*) FROM " + datasource.get("contentTable") +
                " WHERE fbrq = %(fbrq)s and cjwydz = %(cjwydz)s and cjwy_lanmu = %(cjwy_lanmu)s")
    if check_title:
        queryOne += " AND title = %(title)s"
    cur.execute(queryOne,query_param)
    #检查数据库查询结果是否存在符合条件的记录
    if cur.fetchone()[0] !=0:
        is_exists = True
    return is_exists


#创建chrom浏览器实例，同时传入 chromoptions
def init_driver():
    chrome_options = Options()
    chrome_options.page_load_strategy = "eager"

    #启用无头模式，后台运行
    # chrome_options.add_argument("--headless")
    # 创建 Service 对象
    # === 无头模式优化配置 ===
    chrome_options.add_argument("--headless=new")  # Chrome 109+推荐使用new模式
    chrome_options.add_argument("--window-size=1920,1080")  # 必须设置窗口大小
    chrome_options.add_argument("--disable-gpu")  # 规避部分GPU渲染问题

    # === 反自动化检测配置 ===
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    service = Service(chrome_driver)

    #创建 chrom 浏览器实例，同时传入chromeOptions
    return webdriver.Chrome(service=service,options=chrome_options)

def domain(lanmu,url):
    global add_count, lanmu_count, total_content
    total_content = ''
    lanmu_count = 0
    add_count = 0
    # 文件存放文件夹
    # 获取当前脚本所在目录
    # current_dir = os.path.dirname(__file__)
    # folder_name = os.path.join(current_dir, "政策文件1", f"{datasource.get('cjwz')}", lanmu)
    # folder_name2 = os.path.join(current_dir, "政策文件1", f"{datasource.get('cjwz')}2", lanmu)
    # 确保保存文件夹存在
    # if not os.path.exists(folder_name):
    #     os.makedirs(folder_name)
    # # 确保保存文件夹存在
    # if not os.path.exists(folder_name2):
    #     os.makedirs(folder_name2)
    driver = init_driver()
    driver.get(url)
    # logging.info(f'开始采集网站————{url}')
    stop_crawling = False
    # 总页数
    while not stop_crawling:
        #强制等待加显式等待
        time.sleep(1)
        active_report_tag = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR,'.wrapper'))
        )
        time.sleep(1)
        page = driver.find_element(By.CSS_SELECTOR,'.active').text
        page = int(page)
        logging.info(f'开始采集第————{page}页内容')

        li_tags = active_report_tag.find_elements(By.CSS_SELECTOR,'.wrapper_overview_right li')
        if not li_tags:
            li_tags = active_report_tag.find_elements(By.CSS_SELECTOR, '.wrapper_item_con4_ul li')
        lanmu_count += len(li_tags)
        for li_tag in li_tags:
            li_a = li_tag.find_element(By.CSS_SELECTOR,'a')
            title_tag = li_tag.find_element(By.CSS_SELECTOR,'a > span:nth-child(1)')
            fbrq_tag = li_tag.find_element(By.CSS_SELECTOR,'a > span:nth-child(2)')

            newsData = {}
            href = li_a.get_attribute('href')
            base_title = title_tag.text
            #清洗title特殊字符
            title = re.sub(r'[<>:"/\\|?*\x00-\x1f]|\.{2,}', '_', base_title)
            fbrq = fbrq_tag.text


            # 查看数据是否存在
            query_param = {
                'title': title,
                'fbrq': fbrq,
                'cjwydz': href,
                'cjwy_lanmu': lanmu,
            }
            flag = is_exists_data(query_param,datasource,check_title=True)

            # 存在则跳过
            if flag:
                if is_incremental:
                    stop_crawling = True
                    break
                else:
                    continue

            #处理标题相同的情况
            # title_path = os.path.join(folder_name2, title)
            # if os.path.exists(title_path):
            #     # 检查文件夹是否存在，如果存在则添加编号
            #     while os.path.exists(title_path):
            #         title = increment_title(title)
            #         title_path = os.path.join(folder_name2, title)
            # query_param = {
            #     'fbrq': fbrq,
            #     'cjwydz': href,
            #     'cjwy_lanmu': lanmu
            # }
            # # 再次检查更是否已存在
            # flag = is_exists_data(query_param,datasource,check_title=False)
            # if flag:
            #     # 如果更新后的 title 也存在，则跳过
            #
            #     break
            # 提取年份（fbrq格式为"2022-07-13"）
            year = int(fbrq.split('-')[0])
            # 遇到2016年前的数据就终止
            if year < 2022:

                stop_crawling = True
                logging.info(f"{year + 1}年后的数据采集完毕，采集终止")
                break
            stop_crawling = False
            #页面页面的li非可下载链接
            logging.info(f'开始采集栏目————{lanmu}————标题————{title}————内容,日期{fbrq}————网址————{href}')

            save_folder = os.path.join(os.path.dirname(__file__),folder_name)

            content_data = get_content(driver,href,title,save_folder)
            newsData['id'] = content_data.get('id')
            newsData['title']  = title
            newsData['title'] = title
            newsData['ly'] = content_data.get('ly')
            newsData['lx'] = content_data.get('lx')
            newsData['fbrq']  = fbrq
            newsData['fj'] = content_data.get('fj')
            newsData['wjnr'] = content_data.get('wjnr')

            newsData['cjwz'] = datasource.get('cjwz')
            newsData['cjwy_lanmu'] = lanmu
            newsData['cjwydz'] = content_data.get('cjwydz')
            newsData['cjwy_title'] = content_data.get('cjwy_title')
            newsData['cjwy_keywords'] = content_data.get('cjwy_keywords')
            newsData['cjwy_description'] = content_data.get('cjwy_description')
            newsData['cjwy_publishdate'] = content_data.get('cjwy_publishdate')

            newsData['zbdw'] = content_data.get('zbdw')

            newsData['fwjg'] = content_data.get('fwjg')
            newsData['zcwh'] = content_data.get('zcwh')
            newsData['syh'] = content_data.get('syh')
            newsData['cjwy_mbx'] = content_data.get('cjwy_mbx')

            common.insertGgContent(newsData,datasource)
            add_count += 1
        # 如果为2022年前数据跳出外循环
        if stop_crawling:
            total_content = f'国家能源局南方监管局{lanmu},新增{add_count}条数据\n'
            break
        if not stop_crawling:
            try:
                next_page_button = driver.find_element(By.XPATH,"//a[text()='下一页']")
                if not next_page_button.get_attribute('href'):
                    logging.info(f"线程 {threading.current_thread().name}————下一页按钮无有效链接，检索终止")
                    break
                next_page_button.click()
            except (NoSuchElementException, ElementNotInteractableException):
                logging.info(f"线程 {threading.current_thread().name}————无下一页，检索终止")
                break
    # 彻底退出浏览器
    driver.quit()
    return add_count, lanmu_count, total_content

# 发送钉钉消息
def send_dingding_msg(content, is_at_all, mobiles):
    my_secret = 'SECb6ac95c3f36f9dac635bcb5d21387188c56a4f4247118f910b4aa20988f61ab3'
    my_url = 'https://oapi.dingtalk.com/robot/send?access_token=94fb8f11393b109d4fab1bf876583c3d7653da1d5e4b3a1713a1ec8c5f53938c'
    if my_secret:
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f'{timestamp}\n{my_secret}'.encode('utf-8')
        hmac_code = hmac.new(
            my_secret.encode('utf-8'),
            string_to_sign,
            digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        my_url = my_url + f'&timestamp={timestamp}&sign={sign}'  # 最终url，url+时间戳+签名

    headers = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }

    if mobiles:
        if isinstance(mobiles, list):
            payload = {
                "msgtype": "text",
                "text": {
                    "content": content
                },
                "at": {
                    "atMobiles": mobiles,
                    "isAtAll": False
                }
            }
            for mobile in mobiles:
                payload["text"]["content"] += f"@{mobile}"
        else:
            raise TypeError("mobiles类型错误 不是list类型.")
    else:
        payload = {
            "msgtype": "text",
            "text": {
                "content": content
            },
            "at": {
                "atMobiles": "",
                "isAtAll": is_at_all
            }
        }
    response = request(url=my_url, json=payload, headers=headers, method="POST")
    if response.json().get("errcode") == 0:
        logging.debug(f"send_text发送钉钉消息成功：{response.json()}")
        return True
    else:
        logging.debug(f"send_text发送钉钉消息失败：{response.text}")
        return False
    pass


if __name__ == "__main__":
    try:

        resault_content = ''
        #调用主函数
        logging.info("国家能源局南方监管局政策文件采集开始")
        for column in column_list:
            lanmu = column.get("cjwy_lanmu")
            url = column.get("url")
            logging.info(f"------------------------------栏目：{lanmu} 网址：{url}采集开始------------------------------")
            add_count, lanmu_count,total_content = domain(lanmu, url)
            logging.info(f"------------------------------栏目：{lanmu} 网址：{url}采集结束------------------------------\n\n")
            # logging.info(f"栏目——————{lanmu}————————共{lanmu_count}条数据,新增————{add_count}条数据")
            resault_content += total_content
        # logging.info(f"国家能源局南方监管局政策文件采集结束————————————共{web_total}条数据")
        print(f"{resault_content}")
        send_dingding_msg(f"{resault_content}", False, [])
    except Exception as e:
        logging.error(f"任务执行失败，错误信息：{e}",exc_info=True)
    # driver = init_driver()
    # # 文件存放文件夹
    # folder_name = os.path.join(os.path.dirname(__file__), '国家能源局南方监管局', '政策法规')
    # folder_name2 = os.path.join(os.path.dirname(__file__), '国家能源局南方监管局2', '政策法规')
    # content_data = get_content(driver, 'https://www.nea.gov.cn/20241227/60972468f7f24739b3d939f1c97a3931/c.html', '国家能源局公告 2024年 第4号', folder_name, folder_name2)