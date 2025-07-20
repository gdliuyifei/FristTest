"""
全量采集国家税务总局网站-政策法规文件栏目数据
    web: 国家税务总局
    column: 政策文件
    url: https://fgk.chinatax.gov.cn/zcfgk/c100006/listflfg.html
    Collect date range: 增量
"""
import base64
import logging
import random
import re
import shutil

from datetime import datetime

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

from 财务智能问答.增量.config.config import *
from 财务智能问答.增量.config.insert_sql import *

fj_count = 0
total_count = 0
#配置日志信息
logging.basicConfig(filename='crawler/logs/gjswzj_zcwj.log', level=logging.INFO, format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8')

#本地数据库
datasource = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'dbtype': 'mysql',
    'db': 'new_text1',
    'contentTable': 't_supply_zx_zfzcwj',
    'dataType': '',
    'cjwz': '国家税务总局',
    "charset":"utf8"
}

#远程数据库
syy_datasource = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'dbtype': 'mysql',
    'db': 'new_text2',
    'contentTable': 't_supply_zx_zfzcwj',
    'dataType': '',
    'cjwz': '国家税务总局',
    "charset":"utf8"
}

#主网站
base_url = 'https://fgk.chinatax.gov.cn'


# 采集网页地址,政策导航部分栏目
public_url = [
    {
        "cjwy_lanmu": '法律',  # 73  ok
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100009/listflfg_fg.html',
    },
    {
        "cjwy_lanmu": '行政法规',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100010/listflfg_fg.html',
    },
    # {
    #     "cjwy_lanmu": '国务院文件',
    #     "url": 'https://fgk.chinatax.gov.cn/zcfgk/c102440/listflfg.html',
    # },
    # #
    # {
    #     "cjwy_lanmu": '财税文件',
    #     "url": 'https://fgk.chinatax.gov.cn/zcfgk/c102416/listflfg.html',
    # },
    {
        "cjwy_lanmu": '税务部门规章',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100011/list_guizhang.html',
    },
    # {
    #     "cjwy_lanmu": '税务规范性文件',
    #     "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100012/listflfg.html',
    # },
    # {
    #     "cjwy_lanmu": '其他文件',
    #     "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100013/listflfg.html',
    # },
    # {
    #     "cjwy_lanmu": '工作通知',
    #     "url": 'https://fgk.chinatax.gov.cn/zcfgk/c102424/listflfg.html',
    # },
    {
        "cjwy_lanmu": '图片政策解读',  # 73  ok
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100016/list_zcjd.html',
    },
    {
        "cjwy_lanmu": '文字政策解读',
        "url": 'https://fgk.chinatax.gov.cn/zcfgk/c100015/list_zcjd.html',
    },
]

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
# path_wkhtmltopdf = r'D:\download\wkhtmltopdf\bin\wkhtmltopdf.exe'

import pymysql
import datetime



#创建chrom浏览器实例，同时传入 chromoptions
def init_driver():
    global temp_dir
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
        "download.default_directory": temp_dir,  # 设置下载路径
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


def save_title_file_folder(title_path,file,file_name):
    """额外存入文章文件夹当中"""
    global temp_dir
    try:
        #清洗标题
        chinese_file_name = re.sub(r'[<>:"/\\|?*\x00-\x1f\u200B-\u200F\u2002-\u200A\u2028\u2029\uFEFF]+', '_', file_name).strip()
        base_name, ext = os.path.splitext(chinese_file_name)  # 分离主文件名和扩展名
        # 复制到文章文件夹，并且修改为中文名称
        #下载的路径
        # 原始文件路径（要复制的文件）
        source_file_path = os.path.join(temp_dir, file)
        # 目标文件夹路径
        target_folder_path = title_path

        # 初始化最终的文件名和路径
        final_file_name = chinese_file_name
        final_file_path = os.path.join(target_folder_path, final_file_name)

        # 检查文件是否已存在，如果存在则添加序号 (2), (3)...
        counter = 0
        while os.path.exists(final_file_path):
            counter += 1
            final_file_name = f"{base_name}({counter}){ext}"
            final_file_path = os.path.join(target_folder_path, final_file_name)


        # 移动文件到目标路径
        shutil.move(source_file_path, final_file_path)

        logging.info(f"文件 {file_name} 已成功移动并重命名为 {final_file_name}")

        return final_file_path, final_file_name
    except Exception as e:
        logging.error(f"文件保存失败: {e}")
        return None, None

def save_file(mainId:int,file_name:str,isMainFile:str,file_url:str,file_type:str,title_path,folder_name):
    # 获取当前线程的临时文件夹路径
   global temp_dir
   #下载文件并存储至t_supply_zx_acq_file_zcwj表中
   # 遇到：------替换/为-
       #清洗file_name
   retries = 0
   max_retries = 3

   while retries < max_retries:
        try:
            if isMainFile=='0':
                img_name = file_url.split('/')[-1]
                file_path = os.path.join(temp_dir, img_name)
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
                shutil.copy(file_path, os.path.join(folder_name, img_name))
                chinese_file_path, chinese_file_name = save_title_file_folder(title_path, img_name, file_name)
                # 例：图片：file_name:./97274949258.jpg
                # 提取从“政策文件”开始的部分
                fileInfo = {
                    'tableName': datasource.get("contentTable"),
                    'tableId': mainId,
                    'fileType': file_type,
                    'fileName': file_name,
                    'chinese_file_name': chinese_file_name,
                    'isMainFile': isMainFile,
                    'href': './'+img_name,
                    'filePath': img_name,
                    'chinese_file_path': chinese_file_path,
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
            else:
                fileInfo = {
                    'tableName': datasource.get("contentTable"),
                    'tableId': mainId,
                    'fileType': file_type,
                    'fileName': file_name,
                    'chinese_file_name': file_name,
                    'isMainFile': isMainFile,
                    'href': './'+file_name,
                    'filePath': file_name,
                    'chinese_file_path':os.path.join(title_path,file_name),
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
            insertFileInfo(datasource,fileInfo)
            insertFileInfo(syy_datasource, fileInfo)
            retries = 3
            return fileInfo
        except Exception as e:
            retries += 1
            if retries < max_retries:
                time.sleep(5)
            logging.info(f"发生错误: {e}. 重试次数: {retries + 1}/{max_retries}")






# 获取文章详情内容
def get_content(driver, url,title):
    global fj_count,temp_dir,folder_name,folder_name2
    # 获取当前线程的临时文件夹路径
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
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

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
            final_file_path=os.path.join(folder_name, f"{title}.pdf")
            counter = 0
            while os.path.exists(final_file_path):
                counter += 1
                final_file_name = f"{title}({counter}).pdf"
                final_file_path = os.path.join(folder_name, final_file_name)

            base_final_file_path = os.path.join(folder_name, f"{title}.pdf")
            if os.path.exists(base_final_file_path):
                title = title + f'({counter})'
            # 创建文章文件夹
            title_path = os.path.join(folder_name2, title)
            if not os.path.exists(title_path):
                os.makedirs(title_path)
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
            with open(f'{title_path}\\{title}.pdf', 'wb') as f:
                f.write(base64.b64decode(pdf['data']))
        except Exception as e:
            if "ContentNotFoundError" in str(e):
                pass
            else:
                logging.info(f"请检查URL{e}")
            logging.info(f'成功下载正文pdf：{title}.pdf')

        #保存到文章文件夹
        save_file(mainId=mainId,file_name=title+'.pdf',
                  isMainFile='1',file_url='',file_type='pdf',title_path=title_path,folder_name=folder_name)
        # 面包屑
        mbx = driver.find_elements(By.CSS_SELECTOR,'.position a')
        content_data['cjwy_mbx'] = '>'.join([
            elem.get_attribute("textContent").strip() for elem in mbx if elem.get_attribute("textContent").strip()])
        #来源
        try:
            ly= driver.find_element(By.CLASS_NAME, 'dw')
            content_data['ly']=ly.text
        except Exception as e:
            content_data['ly']=None

        content_data['title']=title
        #采集网页发布日期
        try:
            content_data['cjwy_publishdate'] = driver.find_element(By.CSS_SELECTOR, 'meta[name="PubDate"]').get_attribute("content")
        except NoSuchElementException:
            pass
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
                        if file_name.split('.')[-1] in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'tiff']:
                            file_type=file_name.split('.')[-1]
                        else:
                            file_type = file_url.split('.')[-1]
                        if file_type not in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'jpg',
                                             'jpeg', 'png', 'gif', 'bmp', 'svg', 'tiff']:
                            continue

                        #附件为图片的情况：
                        if file_type in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                            img_url = file_url.replace('https', 'http')
                            img_base_name=file_url.split("/")[-1]
                            img_name= re.sub(r'[<>:"/\\|?*\x00-\x1f\u200B-\u200F\u2002-\u200A\u2028\u2029\uFEFF]+', '_', file_name).strip()
                            if not img_name.endswith(file_type):
                                img_name = img_name+'.'+file_type
                                #先下载中文名称的图片后改为

                            save_file(mainId, file_name=img_name,isMainFile='0',
                                      file_url=img_url,file_type=file_type,title_path=title_path,folder_name=folder_name)
                            fj.append(file_name)
                            fj_count += 1
                        else:
                            try:
                                # 获取未下载前的文件名列表
                                existing_files = set(os.listdir(temp_dir))
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
                                        current_files = set(os.listdir(temp_dir))  # 重新获取文件列表
                                        downloading_files = {f for f in current_files if
                                                             f.endswith(".crdownload")}  # 找出正在下载的文件
                                        if not downloading_files:  # 如果没有正在下载的文件，说明下载完成
                                            break
                                        time.sleep(2)  # 等待2秒后再次检查
                                    # 找出新下载的文件
                                    current_files = set(os.listdir(temp_dir))
                                    new_files = current_files - existing_files
                                    if len(new_files) ==0:
                                        time.sleep(5)
                                        #点击无法下载，使用请求下载
                                        file_url=file_url.replace('https', 'http')
                                        if file_type not in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'tiff']:
                                            logging.info(f'文章：{url}文件不存在或为其他链接:{file_url}')
                                            # 关闭新窗口并切换回原来的窗口
                                            continue
                                        else:
                                            new_file_name = str(uuid.uuid4())[:8] + '.' + file_type  # 重命名文件名
                                            file_path = os.path.join(temp_dir, new_file_name)
                                            time.sleep(5)
                                            session = requests.Session()
                                            response = session.get(file_url, stream=True, headers=headers)
                                            response.raise_for_status()
                                            # 下载文件
                                            with open(file_path, 'wb') as file:
                                                for chunk in response.iter_content(chunk_size=8192):
                                                    if chunk:
                                                        file.write(chunk)
                                            logging.info(f'请求！{file_name}下载成功！')
                                            shutil.copy(file_path,os.path.join(folder_name, new_file_name))
                                            # 复制到文章文件夹并且修改为中文名称
                                            if not file_name.endswith(file_type):
                                                chinese_file_name = file_name + '.' + file_type
                                            else:
                                                chinese_file_name = file_name
                                            chinese_file_path, chinese_file_name = save_title_file_folder(title_path,
                                                                                                          new_file_name,
                                                                                                          chinese_file_name)
                                            if chinese_file_path != None:
                                                fileInfo = {
                                                    'tableName': datasource.get("contentTable"),
                                                    'tableId': mainId,
                                                    'fileType': file_type,
                                                    'fileName': file_name,
                                                    'chinese_file_name': chinese_file_name,
                                                    'isMainFile': '0',
                                                    'href': f'./{new_file_name}',
                                                    'filePath': new_file_name,
                                                    'chinese_file_path': chinese_file_path,

                                                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                }
                                                insertFileInfo(datasource, fileInfo)
                                                insertFileInfo(syy_datasource, fileInfo)
                                                logging.info(f"成功插入文件: {file_name}")
                                                fj.append(file_name)
                                                fj_count += 1

                                    else:
                                        downloaded_file = new_files.pop()
                                        # 重命名文件,原文件名为中文
                                        new_file_name =str(uuid.uuid4())[:8]+'.'+file_type # 重命名文件名
                                        os.rename(os.path.join(temp_dir, downloaded_file),
                                                  os.path.join(temp_dir, new_file_name))
                                        logging.info(f'{file_name}下载成功！')
                                        shutil.copy(os.path.join(temp_dir, new_file_name),os.path.join(folder_name, new_file_name))
                                        #复制到文章文件夹并且修改为中文名称
                                        if not file_name.endswith(file_type):
                                            chinese_file_name = file_name + '.' + file_type
                                        else:
                                            chinese_file_name=file_name
                                        chinese_file_path,chinese_file_name=save_title_file_folder(title_path, new_file_name,chinese_file_name)
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
                                            insertFileInfo(datasource,fileInfo)
                                            insertFileInfo(syy_datasource, fileInfo)
                                            logging.info(f"成功插入文件: {file_name}")
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




def domain(url,lanmu):
    global fj_count, total_count,temp_dir,folder_name2,folder_name
    try:
        fj_count=0
        lanmu_count=0
        driver = init_driver()
        driver.get(url)
        logging.info(f'开始采集网站：栏目为：{lanmu}———{url}')
        time.sleep(4)
        #退出循环变量
        stop_crawling = False
        while not stop_crawling:
            #强制等待加显式等待
            time.sleep(5)
            active_report_tag = WebDriverWait(driver,10).until(
                EC.presence_of_element_located((By.CLASS_NAME,'list'))
            )

            #获取数据列表
            li_tags = active_report_tag.find_elements(By.XPATH,"./ul/li")
            if len(li_tags) == 0:
                li_tags = active_report_tag.find_elements(By.XPATH, "./li")
            for li_tag in li_tags:
                time.sleep(1)
                try:
                    title_tag = li_tag.find_element(By.XPATH,'./p[2]/a')
                    # 文章title
                    base_title = title_tag.text
                    base_title = re.sub(r'[<>:"/\\|?*\x00-\x1f\u200B-\u200F\u2002-\u200A\u2028\u2029\uFEFF]+', '_', base_title).strip()
                    try:
                        fbrq_tag = li_tag.find_element(By.CLASS_NAME, 'cwrq')
                        # 文章发布日期
                        fbrq = fbrq_tag.text
                    except NoSuchElementException:
                            # fbrq_tag = li_tag.find_element(By.XPATH, './p[2]/span')
                        fbrq=None
                except NoSuchElementException:
                    title_tag = li_tag.find_element(By.XPATH, './a')
                    # 文章title
                    base_title = title_tag.get_attribute('title')
                    base_title = re.sub(r'[<>:"/\\|?*\x00-\x1f\u200B-\u200F\u2002-\u200A\u2028\u2029\uFEFF]+', '_', base_title).strip()
                    fbrq_tag = li_tag.find_element(By.XPATH, './a/span')
                    # 文章发布日期
                    fbrq = fbrq_tag.text
                try:
                    zcwh=li_tag.find_element(By.CLASS_NAME,'fwzh ')
                    # 政策文号
                    zcwh = zcwh.text
                except NoSuchElementException:
                    # try:
                    #     base_zcwh = li_tag.find_element(By.XPATH, './p[2]/span').text
                    #     if '公布' in base_zcwh:
                    #         # 使用正则表达式匹配“xxx第x号”，并确保匹配的内容不包含“日”字之前的部分
                    #         pattern = r"日([^日]+?第\d+号)"  # 匹配"日"后面的内容，直到"第x号"
                    #         match = re.search(pattern, base_zcwh)
                    #         if match:
                    #             zcwh = match.group(1).strip()  # 提取匹配的部分并去除前后空格
                    # except NoSuchElementException:
                        zcwh=None
                        pass
                newsData = {}
                #文章href
                href = title_tag.get_attribute('href')
                if href and href.startswith('http'):
                    cjwydz=href
                else:
                    cjwydz=base_url+href


                #查看数据是否存在
                query_param = {
                    'title': base_title,
                    'fbrq': fbrq,
                    'cjwydz':cjwydz,
                    'cjwy_lanmu': lanmu,
                }
                flag = is_exists_data(datasource,query_param)

                #存在则跳过
                if flag:
                    stop_crawling=True
                    break
                stop_crawling = False
                logging.info(f'开始采集栏目：{lanmu}，标题————{base_title}————内容,日期{fbrq}，url：{cjwydz}')
                newsData['title'] = base_title
                content_data = get_content(driver, cjwydz,base_title)

                newsData['id'] = content_data.get('id')
                newsData['last_title'] = content_data['title']
                newsData['fbrq'] = fbrq
                newsData['ly'] = content_data.get('ly')
                newsData['lx'] = content_data.get('lx')
                newsData['fj'] = content_data.get('fj')
                newsData['cjwz'] = datasource.get('cjwz')
                newsData['cjwy_lanmu'] = lanmu
                newsData['cjwydz'] = cjwydz
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
                insertGgContent(datasource,newsData)
                insertGgContent(syy_datasource, newsData)
                lanmu_count += 1
            # 遇到2016年前的数据就终止外层循环
            if stop_crawling:
                break

            if not stop_crawling:
                try:
                    time.sleep(1)
                    #点击下一页
                    next_page_button = driver.find_element(By.XPATH,"//a[text()='下一页']")
                    # 检查是否包含禁用类（layui-disabled）
                    if "layui-disabled" in next_page_button.get_attribute("class"):
                        logging.info(f"{lanmu}已到末页，检索终止")
                        break
                    else:
                        driver.execute_script("arguments[0].click();", next_page_button)
                except (NoSuchElementException, ElementNotInteractableException):
                    logging.info(f"{lanmu}已到末页，检索终止")
                    break

        total_count += lanmu_count

        return lanmu_count, fj_count  # 返回本地计数和全局附件计数
    finally:
        driver.close()
        driver.quit()


import os
import logging


shared_data = {
    'total_count': 0,
    'fj_count': 0,
    'total_content': ''
}


def process_item(item):
    global shared_data,temp_dir,folder_name,folder_name2

    lanmu = item['cjwy_lanmu']
    url = item['url']
    folder_name = os.path.join('政策文件2', '国家税务总局', lanmu)
    folder_name2 = os.path.join('政策文件','国家税务总局', lanmu)

    # 为当前线程设置临时文件夹路径
    temp_dir =os.path.join('temp', f'{lanmu}_temp')
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    try:
        # 调用采集函数
        lanmu_count, local_fj_count = domain(url, lanmu)


        shared_data['total_count'] += lanmu_count
        shared_data['fj_count'] += local_fj_count

        # 更新实时统计信息
        current_stats = f'栏目:{lanmu}, 已采集{lanmu_count}条数据，附件{local_fj_count}个'
        lanmu_content= f'{current_stats}\n'
        shared_data['total_content']=shared_data['total_content']+lanmu_content


    except Exception as e:
        error_msg = f"栏目[{lanmu}]采集失败: {str(e)}"
        logging.error(error_msg)
        # send_dingding_msg(error_msg, False, [])


def run_threaded_tasks():

    shared_data.update({
        'total_count': 0,
        'fj_count': 0,
        'total_content': ''
    })

    for item in public_url:
        process_item(item)

    # 最终统计
    final_report = (
        f"国家税务总局-政策文件部分采集完成\n"
        f"总数据量: {shared_data['total_count']}条\n"
        f"总附件数: {shared_data['fj_count']}个\n"
        f"详细统计:\n{shared_data['total_content']}"
    )

    send_dingding_msg(final_report, False, [])
    logging.info(final_report)
    return final_report


