"""
    能源要闻
"""

import logging
import re
import threading
import datetime
import pymysql
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, \
    ElementNotInteractableException
import config_1

# 配置日志记录
# logging.basicConfig(filename='logs/nyyw.log', level=logging.INFO, format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


datasource = {
    'host': '218.19.148.220',
    'port': 19201,
    'user': 'zxkj',
    'password': 'Zxkj__##2024',
    'dbtype': 'mysql',
    'db': 'external_data',
    'contentTable': 't_supply_zx_ldjhjqbyw',
    'dataType': '',
    'cjwz': '国家能源局'
}

datasource_local = {
    'host': '127.0.0.1',
    'port': 3399,
    'user': 'wbsj',
    'password': 'Abc123#456@zx',
    'dbtype': 'mysql',
    'db': 'wbsj',
    'contentTable': 't_supply_zx_ldjhjqbyw',
    'dataType': '',
    'cjwz': '国家能源局'
}


# 采集网页地址
basicUrl = 'https://www.nea.gov.cn/xwzx/nyyw.htm'


# 增量采集结果
total_content = ''
# 增量值
total_count = 0


def insertGgContent(newsData):
    try:
        conn, cur = config.connect_to_mysql_with_retry(datasource)

        newsData.setdefault('createTime', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        insertSql = (
                "INSERT INTO " + datasource.get("contentTable") +
                " (id, title, ly, lx, fbrq, wjnr, fj, cjwz, cjwy_lanmu, cjwydz, cjwy_title,cjwy_keywords, cjwy_description, cjwy_publishdate,cjwy_mbx, create_time) " +
                "VALUES (%(id)s, %(title)s, %(ly)s, %(lx)s, %(fbrq)s, %(wjnr)s, %(fj)s, %(cjwz)s, %(cjwy_lanmu)s, %(cjwydz)s, %(cjwy_title)s, %(cjwy_keywords)s,%(cjwy_description)s,%(cjwy_publishdate)s, %(cjwy_mbx)s, %(createTime)s)"
        )
        # 执行插入操作
        cur.execute(insertSql, newsData)

        # 提交事务并关闭连接
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"插入数据时出现错误: {e}")

def insertGgContent_local(newsData):
    try:
        conn, cur = config.connect_to_mysql_with_retry(datasource_local)

        newsData.setdefault('createTime', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        insertSql = (
                "INSERT INTO " + datasource.get("contentTable") +
                " (id, title, ly, lx, fbrq, wjnr, fj, cjwz, cjwy_lanmu, cjwydz, cjwy_title,cjwy_keywords, cjwy_description, cjwy_publishdate,cjwy_mbx, create_time) " +
                "VALUES (%(id)s, %(title)s, %(ly)s, %(lx)s, %(fbrq)s, %(wjnr)s, %(fj)s, %(cjwz)s, %(cjwy_lanmu)s, %(cjwydz)s, %(cjwy_title)s, %(cjwy_keywords)s,%(cjwy_description)s,%(cjwy_publishdate)s, %(cjwy_mbx)s, %(createTime)s)"
        )
        # 执行插入操作
        cur.execute(insertSql, newsData)

        # 提交事务并关闭连接
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"插入数据时出现错误: {e}")


# 插入文件关联表
def insertFileInfo(fileInfo):
    try:
        conn, cur = config.connect_to_mysql_with_retry(datasource)

        fileInfo.setdefault('id', config.idwork.get_id())
        insertSql = ("INSERT INTO " + 't_supply_zx_acq_file' +
                     " (id, table_name, table_id, file_type, file_name, is_main_file,file_path,href,create_time) " +
                     "VALUES (%(id)s, %(tableName)s, %(tableId)s,%(fileType)s,%(fileName)s,%(isMainFile)s,%(filePath)s,%(href)s,%(createTime)s)")

        # 执行插入操作
        cur.execute(insertSql, fileInfo)

        # 提交事务并关闭连接
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"插入数据时出现错误: {e}")

def insertFileInfo_local(fileInfo):
    try:
        conn, cur = config.connect_to_mysql_with_retry(datasource_local)

        fileInfo.setdefault('id', config.idwork.get_id())
        insertSql = ("INSERT INTO " + 't_supply_zx_acq_file' +
                     " (id, table_name, table_id, file_type, file_name, is_main_file,file_path,href,create_time) " +
                     "VALUES (%(id)s, %(tableName)s, %(tableId)s,%(fileType)s,%(fileName)s,%(isMainFile)s,%(filePath)s,%(href)s,%(createTime)s)")

        # 执行插入操作
        cur.execute(insertSql, fileInfo)

        # 提交事务并关闭连接
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"插入数据时出现错误: {e}")

# 获取文章详情内容
def get_content(driver, url, save_folder):
    content_data = {
        'id': '',  # 主键id
        'wjnr': '',  # 文件内容
        'ly': '',    # 来源
        'fj': '', # 附件
        'cjwydz': '',  # 采集网页地址
        'cjwy_title': '',  # 采集网站title
        'cjwy_keywords': '',  # 采集网页keywords
        'cjwy_description': '',  # 采集网页description
        'cjwy_publishdate': '', # 采集网页publishdate
        'cjwy_mbx': ''  # 采集网页面包屑
    }

    # 确保保存文件夹存在
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    try:
        # 打开新窗口并切换到新窗口
        original_window = driver.current_window_handle
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)
        time.sleep(2)  # 等待页面加载

        # 更新 content_data 中的相关信息
        mainId = config.idwork.get_id()
        content_data['id'] = mainId

        # 面包屑
        mbx = driver.find_element(By.CLASS_NAME,'now-site')
        content_data['cjwy_mbx'] = mbx.text.replace('您当前位置：','')
        # 正文html结构
        content = driver.find_element(By.CLASS_NAME, 'article-box')
        content_data['wjnr'] = content.get_attribute('outerHTML')
        # 来源
        ly = driver.find_element(By.CLASS_NAME, 'author')
        content_data['ly'] = ly.text.replace('来源：','')
        # 采集网址
        content_data['cjwydz'] = url
        # 网页title
        content_data['cjwy_title'] = driver.title
        # 网页keywords
        content_data['cjwy_keywords'] = driver.find_element(By.NAME, 'keywords').get_attribute('content')
        # 网页description
        content_data['cjwy_description'] = driver.find_element(By.NAME, 'description').get_attribute('content')
        try:
            # 网页publishdate
            content_data['cjwy_publishdate'] = driver.find_element(By.NAME, 'publishdate').get_attribute('content')
        except NoSuchElementException:
            pass

        img_elements = content.find_elements(By.TAG_NAME, 'img')
        if img_elements:
            for img in img_elements:
                fileInfo = {}
                # 获取图片的 src 属性，即图片的 URL
                img_url = img.get_attribute('src')
                img_name = img_url.split("/")[-1]
                if img_url:
                    try:
                        # 发送请求获取图片内容
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                        }
                        response = requests.get(img_url, headers=headers,stream=True)
                        response.raise_for_status()

                        # 生成图片的文件名
                        file_path = os.path.join(save_folder, img_name)

                        # 将图片内容写入本地文件
                        with open(file_path, 'wb') as file:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    file.write(chunk)
                            fileInfo.setdefault('tableName', datasource.get("contentTable"))
                            fileInfo.setdefault('tableId', mainId)
                            fileInfo.setdefault('fileType', 'images')
                            fileInfo.setdefault('fileName', img_name)
                            fileInfo.setdefault('isMainFile', '0')
                            fileInfo.setdefault('filePath', img_name)
                            fileInfo.setdefault("createTime",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            fileInfo.setdefault('href','')
                            insertFileInfo(fileInfo)
                            insertFileInfo_local(fileInfo)
                        logging.info(f"成功下载图片: {img_name}")
                    except requests.RequestException as e:
                        logging.error(f"下载图片时出错: {e}",exc_info=True)
        # 关闭新窗口并切换回原来的窗口
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


def is_exists_data(query_param):
    is_exists = False
    # 创建mysql链接
    conn = pymysql.connect(host=datasource.get('host'), port=datasource.get('port'), user=datasource.get('user'),
                           passwd=datasource.get('password'), db=datasource.get('db'))
    # 光标对象
    cur = conn.cursor()
    # 执行连接数据库
    cur.execute('USE ' + datasource.get('db'))
    queryOne = ("SELECT COUNT(*) FROM " + datasource.get("contentTable") +
                " WHERE title = %(title)s and fbrq = %(fbrq)s and cjwy_lanmu = %(cjwy_lanmu)s")
    cur.execute(queryOne, query_param)
    if cur.fetchone()[0] != 0:
        is_exists = True
    return is_exists

# 创建 Chrome 浏览器实例，同时传入 ChromeOptions
def init_driver():
    chrome_options = Options()

    # 启用无头模式，后台运行
    chrome_options.add_argument("--headless")
    # 加载完dom算加载完成
    chrome_options.page_load_strategy = 'eager'
    # 创建 Service 对象
    service = Service(config.chrome_driver)

    # 创建 Chrome 浏览器实例，同时传入 ChromeOptions
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

def domain(local_save_folder):
    global total_content
    global total_count
    total_count = 0
    driver = init_driver()
    url = basicUrl
    driver.get(url)
    logging.info(f"开始采集网站————{url}")
    outer_loop = False
    while True:
        time.sleep(1)
        active_report_tag = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'showData0'))
        )
        time.sleep(1)
        page = driver.find_element(By.CSS_SELECTOR,'.page.spanPagerStyle.active').text
        logging.info(f'开始采集第————{page}页内容')
        li_tags = active_report_tag.find_elements(By.TAG_NAME, 'li')
        for li_tag in li_tags:
            title_tag = li_tag.find_element(By.TAG_NAME, 'a')
            fbrq_tag = li_tag.find_element(By.CLASS_NAME, 'sj')

            newsData = {}
            href = title_tag.get_attribute('href')
            title = title_tag.text
            fbrq = fbrq_tag.text

            query_param = {
                'title': title,
                'fbrq': fbrq,
                'cjwy_lanmu': '能源要闻'
            }
            flag = is_exists_data(query_param)
            # 存在则跳过
            if flag:
                outer_loop = True
                break
            logging.info(f'开始采集标题————{title}————内容')
            newsData['title'] = title
            newsData['fbrq'] = fbrq
            save_folder = os.path.join(os.path.dirname(__file__), local_save_folder)
            content_data = get_content(driver, href, save_folder)

            newsData['id'] = content_data.get('id')
            newsData['ly'] = content_data.get('ly')
            newsData['wjnr'] = content_data.get('wjnr')
            newsData['fj'] = content_data.get('fj')
            newsData['cjwz'] = datasource.get('cjwz')
            newsData['cjwy_lanmu'] = '能源要闻'
            newsData['cjwydz'] = content_data.get('cjwydz')
            newsData['cjwy_title'] = content_data.get('cjwy_title')
            newsData['cjwy_keywords'] = content_data.get('cjwy_keywords')
            newsData['cjwy_description'] = content_data.get('cjwy_description')
            newsData['cjwy_publishdate'] = content_data.get('cjwy_publishdate')
            newsData['cjwy_mbx'] = content_data.get('cjwy_mbx')
            # 类型字段特殊处理
            newsData['lx'] = ''
            insertGgContent(newsData)
            insertGgContent_local(newsData)
        try:
            next_page_button = driver.find_element(By.XPATH, "//span[text()='下一页']")
            next_page_button.click()
        except (NoSuchElementException, ElementNotInteractableException):
            logging.info(f"线程 {threading.current_thread().name}————无下一页，检索终止")
            break
        if outer_loop:
                break
    total_content = f"能源要闻,新增 {total_count}条数据\n"
    pass


# 多线程任务分配
def run_threaded_tasks():
    # 初始化
    global total_content
    total_content = ''

    try:
        domain(config.local_save_folder)
        return total_content
    except Exception as e:
        logging.error(f"能源要闻增量采集出错——{e}",exc_info=True)



