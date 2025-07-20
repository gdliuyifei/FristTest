"""
北极星电力信息网政策文件数据
    web: 北极星电力信息网
    column: 政策
    url: https://news.bjx.com.cn/zc/
    Collect date range: 2022年至今
"""

import logging
import re
import shutil
import base64
import threading
import uuid
from datetime import datetime
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
from sqlalchemy import false
from urllib.parse import unquote
from PyPDF2 import PdfMerger
#配置日志信息
logging.basicConfig(filename='logs/bjxdlxxw.log', level=logging.INFO, format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',encoding='utf-8')
datasource = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'dbtype': 'mysql',
    'db': 'wbsj',
    'contentTable': 't_supply_zx_zfzcwj',
    'dataType': '',
    'cjwz': '北极星电力信息网'
}
# 驱动路径
chrome_driver = r"D:\programfiles\chromedriver-win64\chromedriver-win64\chromedriver.exe"
# 采集网页地址
# basicUrl = 'https://nfj.nea.gov.cn/xwzx/tzgg/'

# 初始化 PDF 合并工具
pdf_merger = PdfMerger()

#采集网页栏目
column_list = [
    {
        "cjwy_lanmu": '政策',
        "url": 'https://news.bjx.com.cn/zc/'
    }
]


# syy服务器文件存放路径
syy_file_path = '/hlpt/pmds/LawData/ldjhjqbyw/'

# 文件存放文件夹
folder_name = f"北极星电力信息网"

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
    insertSql = ("INSERT INTO " + 't_supply_zx_acq_file_zfzcwj' +
                 " (id, table_name, table_id, file_type, file_name,chinese_file_name,chinese_file_path,is_main_file,file_path,href,create_time) " +
                 "VALUES (%(id)s, %(tableName)s, %(tableId)s,%(fileType)s,%(fileName)s,%(chinese_file_name)s,%(chinese_file_path)s,%(isMainFile)s,%(filePath)s,%(href)s,%(createTime)s)")

    # 执行插入操作
    cur.execute(insertSql, fileInfo)

    # 提交事务并关闭连接
    conn.commit()
    cur.close()
    conn.close()

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
def get_content(driver,url,title,folder_name,folder_name2):
    content_data = {
        'id': '',  # 主键id
        'wjnr': '',  # 文件内容
        'title': '',  #标题
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

    # 确保保存文件夹存在
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    # 确保保存文件夹存在
    if not os.path.exists(folder_name2):
        os.makedirs(folder_name2)

    # 创建文章文件夹，处理标题相同的情况
    title_path = os.path.join(folder_name2, title)
    if not os.path.exists(title_path):
        os.makedirs(title_path, exist_ok=True)

    # 更新 content_data 中的相关信息
    mainId = idwork.get_id()
    content_data['id'] = mainId
    # 采集网址
    content_data['cjwydz'] = url
    try:
        #打开新窗口并切换到新窗口
        original_window = driver.current_window_handle
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)
        # 等待页面加载
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#article_cont'))
        )
        #处理正文里面可以翻页的情况
        pdf_merger = PdfMerger()
        all_temp_files = []  # 记录所有临时文件
        final_pdf_name = f"{title}.pdf"
        final_pdf_path = os.path.join(folder_name, final_pdf_name)
        content = ''
        try:
            while True:
                try:
                    # 1. 生成当前页PDF
                    pdf_data = driver.execute_cdp_cmd('Page.printToPDF', {
                        'landscape': False,
                        'displayHeaderFooter': False,
                        'printBackground': True,
                        'preferCSSPageSize': False,
                        'paperWidth': 11.69,
                        'paperHeight': 16.54,
                        'marginTop': 0,
                        'marginBottom': 0,
                        'marginLeft': 0,
                        'marginRight': 0,
                    })

                    # 2. 保存临时PDF
                    temp_pdf_name = f"temp_{title}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"
                    temp_pdf_path = os.path.join(folder_name, temp_pdf_name)

                    with open(temp_pdf_path, 'wb') as f:
                        f.write(base64.b64decode(pdf_data['data']))
                    all_temp_files.append(temp_pdf_path)
                    pdf_merger.append(temp_pdf_path)

                    # 3. 获取当前页的 HTML 内容并追加
                    try:
                        current_page_content = driver.find_element(By.ID, 'article_cont').get_attribute('outerHTML')
                        content_data['wjnr'] += current_page_content  # 追加到 wjnr
                    except Exception as e:
                        logging.error(f'未找到正文内容或页面结构有变化{e}')

                    # 3. 检查分页按钮
                    paging = driver.find_element(By.XPATH, '//*[@class="cc-paging"]')
                    next_btn = paging.find_element(By.XPATH, ".//a[text()='下一页']")

                    if "disable" in next_btn.get_attribute("class"):
                        logging.info("文章里存在分页按钮----已采集到达最后一页（按钮被禁用）")
                        break

                    next_btn.click()
                    time.sleep(1)

                except NoSuchElementException:
                    # logging.info("找不到下一页按钮，终止分页")

                    break
                except Exception as e:
                    logging.error(f"处理页面时出错: {e}")
                    break

            # 4. 保存最终PDF（循环结束后处理）
            if all_temp_files:
                pdf_merger.write(final_pdf_path)
                shutil.copy(final_pdf_path, os.path.join(title_path, final_pdf_name))

                # 记录元数据
                insertFileInfo({
                    'tableName': datasource.get("contentTable"),
                    'tableId': mainId,
                    'fileType': 'pdf',
                    'fileName': final_pdf_name,
                    'chinese_file_name': final_pdf_name,
                    'isMainFile': 1,
                    'href': './' + final_pdf_name,
                    'filePath': final_pdf_name,
                    'chinese_file_path': os.path.join(title_path, final_pdf_name),
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })
                # logging.info(f"PDF生成成功: {final_pdf_path}")
                logging.info(f'成功下载正文pdf：{final_pdf_name}')
        except Exception as e:
            logging.error(f"下载正文pdf失败: {e}")
        finally:
            # 5. 资源清理（最后执行）
            pdf_merger.close()
            for temp_file in all_temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except Exception as e:
                    logging.warning(f"删除临时文件失败: {temp_file} - {e}")

        # 面包屑
        try:
            mbx=  " > ".join([em.text for em in driver.find_elements(By.CSS_SELECTOR, 'div.cc-crumbs em')])
            content_data['cjwy_mbx'] = mbx
        except NoSuchElementException:
            pass
        # 文件内容（html）
        content = driver.find_element(By.ID, 'article_cont')

        #存储完整的html
        # content_data['wjnr'] = content.get_attribute('outerHTML')

        headline = driver.find_element(By.CSS_SELECTOR,'div.cc-headline')
        content_data['ly'] = headline.find_element(By.CSS_SELECTOR,'span:nth-child(2)').text.split('来源：')[-1].strip()

        #采集网页生成日期
        try:
            content_data['cjwy_publishdate'] = headline.find_element(By.CSS_SELECTOR,'span:nth-child(1)').text.strip()
        except NoSuchElementException:
            try:
                content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="PubDate"]').get_attribute('content')
            except NoSuchElementException:
                content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="publishdate"]').get_attribute('content')
        # 发文机构
        content_data['fwjg'] = ''

        #政策文号
        content_data['zcwh'] = ''
        #主题分类
        content_data['lx'] = ''
        #索引号

        content_data['syh'] = ''

        # 采集网页title

        content_data['cjwy_title'] = driver.find_element(By.CSS_SELECTOR,'div.cc-headline > div > h1').text.strip()
        #主办单位

        content_data['zbdw'] = ''
        # 网页keywords
        try:
            content_data['cjwy_keywords'] = driver.find_element(By.NAME, 'Keywords').get_attribute('content')
        except NoSuchElementException:
            pass
        # 网页description
        try:
            content_data['cjwy_description'] = driver.find_element(By.NAME, 'description').get_attribute('content')
        except NoSuchElementException:
            pass
        #下载正文图片
        # img_elements = content.find_elements(By.TAG_NAME,'img')
        # if img_elements:
        #     for img in img_elements:
        #         fileInfo = {}
        #         #获取图片src属性，即图片的url
        #         img_url = img.get_attribute('src')
        #         #过滤不需要下载的文件
        #         if img.get_attribute('needdownload') == 'false' or '.gif' in img_url:
        #             continue
        #         img_name = img_url.split("/")[-1]
        #         if img_url:
        #             try:
        #                 #发送请求图片内容
        #                 headers = {
        #                     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        #                 }
        #                 response = requests.get(img_url,headers=headers,stream=True)
        #                 response.raise_for_status()
        #
        #                 #生成图片文件名
        #                 file_path = os.path.join(save_folder, img_name)
        #
        #                 #将图片写入本地文件
        #                 with open(file_path,'wb') as file:
        #                     #分块下载文件，避免一次性加载大文件导致内存占用过高
        #                     for chunk in response.iter_content(chunk_size=8192):
        #                         if chunk:
        #                             file.write(chunk)
        #                     fileInfo.setdefault('tableName',datasource.get("contentTable"))
        #                     fileInfo.setdefault('tableId', mainId)
        #                     fileInfo.setdefault('fileType', 'images')
        #                     fileInfo.setdefault('fileName', img_name)
        #                     fileInfo.setdefault('isMainFile', '0')
        #                     fileInfo.setdefault('filePath', img_name)
        #                     fileInfo.setdefault("createTime", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        #                     fileInfo.setdefault('href', '')
        #                     insertFileInfo(fileInfo)
        #                 logging.info(f"成功下载图片：{img_name}")
        #             except requests.RequestException as e:
        #                 logging.error(f"下载图片时出错: {e}", exc_info=True)


        #正文里的链接
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
                if not file_url or '.bjx.com.cn/topics/' in file_url  or 'javascript:' in file_url or  'htm' in file_url:
                    continue

                if file_url:
                    try:
                        # 发送请求获取文件内容
                        response = requests.get(file_url, stream=True)
                        response.raise_for_status()
                        try:
                            # 生成的文件名
                            origin_file_name = file_url.split('/')[-1]
                            # 保存文件夹1
                            file_path1 = os.path.join(folder_name, origin_file_name)
                            base_name, ext = os.path.splitext(origin_file_name)
                            #处理没后缀中文文件名
                            if '.' in chinese_filename:
                                ext_part = chinese_filename.split('.')[-1]
                                # 检查扩展名是否纯英文
                                if not ext_part.isascii() or not ext_part.isalpha():
                                    chinese_filename = chinese_filename + ext
                            else:
                                chinese_filename = chinese_filename + ext
                            # 保存文件夹2
                            file_path2 = os.path.join(title_path, chinese_filename)
                            # 用数字递增处理文件夹1同名文件
                            counter = 1

                            while os.path.exists(file_path1):
                                file_path1 = os.path.join(folder_name, f"{base_name}_{counter}{ext}")
                                origin_file_name = file_path1.split('\\')[-1]
                                counter += 1

                            # 将内容写入本地文件
                            with open(file_path1, 'wb') as file:
                                #从网络逐块读取数据并写入本地文件，避免内存不足
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        file.write(chunk)

                        except FileNotFoundError:
                            # 如果路径无效，生成随机文件名重试
                            origin_file_name = file_url.split('/')[-1]
                            file_ext = os.path.splitext(origin_file_name)[1] if '.' in origin_file_name else ''
                            origin_file_name = f"{uuid.uuid4().hex}{file_ext}"
                            file_path1 = os.path.join(folder_name, origin_file_name)

                            with open(file_path1, 'wb') as file:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        file.write(chunk)
                        #用数字递增处理文件夹2同名文件
                        counter = 1
                        base_name, ext = os.path.splitext(chinese_filename)
                        while os.path.exists(file_path2):
                            file_path2 = os.path.join(title_path, f"{base_name}_{counter}{ext}")
                            chinese_filename = file_path2.split('\\')[-1]
                            counter += 1
                        # 拷贝重命名原始文件
                        shutil.copy(file_path1,file_path2)  # 保留原始副本
                        #设置默认键值对（如果键不存在）
                        fileInfo = {
                            'tableName': datasource.get("contentTable"),
                            'tableId': mainId,
                            'fileType': origin_file_name.split('.')[-1],
                            'fileName': chinese_filename,
                            'chinese_file_name': chinese_filename,
                            'isMainFile': 0,
                            'href': './' + origin_file_name,
                            'filePath': origin_file_name,
                            'chinese_file_path': file_path2,
                            "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        }
                        fj += chinese_filename + ';'
                        #插入文件信息
                        insertFileInfo(fileInfo)
                        logging.info(f"成功下载文件: {chinese_filename}")
                    except requests.RequestException as e:
                        logging.info(f"下载文件时出错: {e}")
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

def is_exists_data(query_param,check_title=True):
    is_exists = False
    #创建mysql连接
    conn = pymysql.connect(host=datasource.get('host'),port=datasource.get('port'),user=datasource.get('user'),
                           passwd=datasource.get('password'),db=datasource.get('db'))
    #光标对象
    cur = conn.cursor()
    #执行连接数据库
    cur.execute('USE ' + datasource.get('db'))
    queryOne = ("SELECT COUNT(*) FROM " + datasource.get("contentTable") +
                " WHERE fbrq = %(fbrq)s and cjwydz = %(cjwydz)s")
    if check_title:
        queryOne += " AND title = %(title)s"
    cur.execute(queryOne,query_param)
    #检查数据库查询结果是否存在符合条件的记录
    if cur.fetchone()[0] !=0:
        is_exists = True
    return is_exists


def check_data_exists(query_param, check_title=True):
    """通用的数据存在性检查函数"""
    query = ("SELECT COUNT(*) FROM " + datasource.get("contentTable") +
             " WHERE fbrq = %(fbrq)s AND cjwydz = %(cjwydz)s")
    if check_title:
        query += " AND title = %(title)s"

    try:
        with pymysql.connect(
                host=datasource.get('host'),
                port=datasource.get('port'),
                user=datasource.get('user'),
                passwd=datasource.get('password'),
                db=datasource.get('db')
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query, query_param)
                return cur.fetchone()[0] > 0
    except Exception as e:
        logging.error(f"数据库查询失败: {e}")
        return True

#创建chrom浏览器实例，同时传入 chromoptions
def init_driver():
    chrome_options = Options()

    #启用无头模式，后台运行
    chrome_options.add_argument("--headless")
    # 创建 Service 对象
    service = Service(chrome_driver)

    #创建 chrom 浏览器实例，同时传入chromeOptions
    return webdriver.Chrome(service=service,options=chrome_options)


def domain(lanmu,url):
    global total_count, lanmu_count
    lanmu_count = 0
    total_count = 0
    # 文件存放文件夹
    folder_name = os.path.join(os.path.dirname(__file__), f"{datasource.get('cjwz')}", lanmu)
    folder_name2 = os.path.join(os.path.dirname(__file__), f"{datasource.get('cjwz')}2.", lanmu)
    # 确保保存文件夹存在
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    # 确保保存文件夹存在
    if not os.path.exists(folder_name2):
        os.makedirs(folder_name2)
    driver = init_driver()
    driver.get(url)
    # logging.info(f'开始采集网站————{url}')
    stop_crawling = False
    # 总页数
    while not stop_crawling:
        #强制等待加显式等待
        time.sleep(1)
        active_report_tag = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR,'.center'))
        )
        time.sleep(1)
        page = driver.find_element(By.CSS_SELECTOR,'.active').text
        page = int(page)
        logging.info(f'开始采集第————{page}页内容')

        li_tags = active_report_tag.find_elements(By.CSS_SELECTOR,'div.cc-list-content > ul > li')
        lanmu_count += len(li_tags)
        for li_tag in li_tags:
            li_a = li_tag.find_element(By.CSS_SELECTOR,'a')
            title_tag = li_tag.find_element(By.CSS_SELECTOR,'a')
            fbrq_tag = li_tag.find_element(By.CSS_SELECTOR,'span')

            newsData = {}
            href = li_a.get_attribute('href')
            base_title = title_tag.text
            #清洗title特殊字符
            # title = re.sub(r'[<>:"/\\|?*\x00-\x1f\u200B]|\.{2,}', '_', base_title)
            title = re.sub(r'[<>:"/\\|?*\x00-\x1f\u200B-\u200F\u2002-\u200A\u2028\u2029\uFEFF]+|\.{2,}', '_', base_title).strip()
            fbrq = fbrq_tag.text

            query_param = {
                'title': title,
                'fbrq': fbrq,
                'cjwydz':href
            }
            flag = is_exists_data(query_param, check_title=True)

            # 存在则跳过
            if flag:
                continue

            #处理标题相同的情况
            title_path = os.path.join(folder_name2, title)
            if os.path.exists(title_path):
                # 检查文件夹是否存在，如果存在则添加编号
                while os.path.exists(title_path):
                    title = increment_title(title)
                    title_path = os.path.join(folder_name2, title)
            query_param = {
                'fbrq': fbrq,
                'cjwydz': href
            }
            # 再次检查更是否已存在
            flag = is_exists_data(query_param, check_title=False)
            if flag:
                # 如果更新后的 title 也存在，则跳过
                break
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

            # save_folder = os.path.join(os.path.dirname(__file__),folder_name)

            content_data = get_content(driver,href,title,folder_name,folder_name2)
            newsData['id'] = content_data.get('id')
            newsData['title']  = title
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

            insertGgContent(newsData)
            total_count += 1
        # 如果为2022年前数据跳出外循环
        if stop_crawling:
            break
        if not stop_crawling:
            try:
                next_page_button = driver.find_element(By.XPATH,"//a[text()='下一页']")
                if next_page_button.get_attribute('class') == 'disable':
                    logging.info(f"线程 {threading.current_thread().name}————下一页按钮无有效链接，检索终止")
                    break
                next_page_button.click()
            except (NoSuchElementException, ElementNotInteractableException):
                logging.info(f"线程 {threading.current_thread().name}————无下一页，检索终止")
                break
    return total_count,lanmu_count

if __name__ == "__main__":
    try:
        web_total = 0
        #调用主函数
        logging.info("北极星电力信息网政策文件采集开始")
        for column in column_list:
            lanmu = column.get("cjwy_lanmu")
            url = column.get("url")
            logging.info(f"------------------------------栏目：{lanmu} 网址：{url}采集开始------------------------------")
            add_count, lanmu_count = domain(lanmu, url)
            logging.info(f"------------------------------栏目：{lanmu} 网址：{url}采集结束------------------------------\n\n")
            logging.info(f"栏目——————{lanmu}————————共{lanmu_count}条数据,新增————{add_count}条数据")
            web_total += lanmu_count
        logging.info(f"北极星电力信息网政策文件采集结束————————————共{web_total}条数据")
    except Exception as e:
        logging.error(f"任务执行失败，错误信息：{e}",exc_info=True)
    # driver = init_driver()
    # # 文件存放文件夹
    # folder_name = os.path.join(os.path.dirname(__file__), '北极星', '政策')
    # folder_name2 = os.path.join(os.path.dirname(__file__), '北极星2', '政策')
    # content_data = get_content(driver, 'https://news.bjx.com.cn/html/20250612/1445878.shtml', '辽宁：阜新等市在风电光伏领域探索人工智能应用', folder_name, folder_name2)