import logging
import datetime
import re
import shutil
import time
import pymysql
import redis
import requests
from bs4 import BeautifulSoup
from requests import request
import urllib
import os
import paramiko
import base64
import hashlib
import hmac
import sqlglot
# from requests_toolbelt import MultipartEncoder
import portalocker  # 添加portalocker库用于跨平台文件锁

from config import idwork, local_save_folder,syy_server_push_file_path,get_sql_file_path,fj_save_path,sql_save_folder,backup_dir,log_filename,API_URL,redis_ip,redis_port,redis_password,redis_db

# logging.basicConfig(
#     filename=log_filename,
#     level=logging.INFO,
#     format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S',
#     encoding='utf-8'
# )

# 定义请求头
# headers = {
#     'Accept': '*/*',
#     'Accept-Language': 'zh-CN,zh;q=0.9',
#     'Cache-Control': 'no-cache',
#     'Connection': 'keep-alive',
#     'Content-Type': 'multipart/form-data; boundary=----WebKitFormBoundary0qAcf0najSBEgCfj',
#     'Origin': f'http://{IP}:{port}',
#     'Pragma': 'no-cache',
#     'Referer': f'http://{IP}:{port}/browser/czqav2',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
# }



def connect_to_mysql_with_retry(datasource, max_retries=3, retry_delay=5):
    """
    带重试机制的 MySQL 连接函数
    :param datasource: 包含数据库连接信息的字典
    :param max_retries: 最大重试次数
    :param retry_delay: 每次重试之间的延迟时间（秒）
    :return: 数据库连接对象和光标对象
    """
    retries = 0
    while retries < max_retries:
        try:
            # 创建 mysql 链接
            conn = pymysql.connect(
                host=datasource.get('host'),
                port=datasource.get('port'),
                user=datasource.get('user'),
                password=datasource.get('password'),
                db=datasource.get('db')
            )
            # 光标对象
            cur = conn.cursor()

            # 执行连接数据库
            cur.execute('USE ' + datasource.get('db'))
            return conn, cur
        except pymysql.Error as e:
            logging.info(f"连接数据库时发生错误: {e}. 重试次数: {retries + 1}/{max_retries}")
            retries += 1
            if retries < max_retries:
                time.sleep(retry_delay)
    logging.info("达到最大重试次数，无法连接到数据库。")
    return None, None

def insertGgContent(newsData,datasource,conn=None, cur=None):
    """
    插入公告内容
    :param newsData: 公告数据字典
    :param datasource: 数据源配置
    :param conn: 可选，外部传入的数据库连接对象
    :param cur: 可选，外部传入的游标对象
    :return: 返回连接和游标对象(当外部传入时)，以便外部可以继续使用同一事务
    """
    close_conn = False
    local_sql_content = ''  # 使用局部变量
    try:
        if conn is None or cur is None:
            conn, cur = connect_to_mysql_with_retry(datasource)
            close_conn = True  # 标记为需要关闭连接

        newsData.setdefault('createTime', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # insertSql = (
        #         "INSERT INTO " + datasource.get("contentTable") +
        #         " (id, title,ly, lx, fbrq, fj, cjwz, cjwy_lanmu, cjwydz, cjwy_title,cjwy_keywords, cjwy_description, wjnr, zbdw, fwjg, zcwh, syh, cjwy_mbx, sjlx,cjwy_publishdate, create_time) " +
        #         "VALUES (%(id)s, %(title)s,%(ly)s, %(lx)s, %(fbrq)s, %(fj)s, %(cjwz)s, %(cjwy_lanmu)s, %(cjwydz)s,%(cjwy_title)s, %(cjwy_keywords)s, %(cjwy_description)s, %(wjnr)s, %(zbdw)s, %(fwjg)s, %(zcwh)s, %(syh)s, %(cjwy_mbx)s, %(sjlx)s,%(cjwy_publishdate)s, %(createTime)s)"
        # )
        insertSql = (
                "INSERT INTO " + datasource.get("contentTable") +
                " (id, title,ly, lx, fbrq, fj, cjwz, cjwy_lanmu, cjwydz, cjwy_title,cjwy_keywords, cjwy_description, wjnr, zbdw, fwjg, zcwh, syh, cjwy_mbx, cjwy_publishdate, create_time) " +
                "VALUES (%(id)s, %(title)s,%(ly)s, %(lx)s, %(fbrq)s, %(fj)s, %(cjwz)s, %(cjwy_lanmu)s, %(cjwydz)s,%(cjwy_title)s, %(cjwy_keywords)s, %(cjwy_description)s, %(wjnr)s, %(zbdw)s, %(fwjg)s, %(zcwh)s, %(syh)s, %(cjwy_mbx)s, %(cjwy_publishdate)s, %(createTime)s)"
        )
        # 执行插入操作
        cur.execute(insertSql,newsData)

        full_sql = cur._executed
        logging.info(f"=====================================执行完整的sql语句{full_sql}")
        dm_sql = sqlglot.transpile(full_sql,read="mysql",write="oracle")[0]
        local_sql_content += dm_sql + ';\n'

        #提交事务并关闭文件连接
        if close_conn:
            conn.commit()
            cur.close()
            conn.close()
        return conn, cur, local_sql_content  # 返回局部变量
    except Exception as e:
        logging.error(f"插入数据时出现错误: {e}",exc_info=True)
        if close_conn and conn:
            try:
                conn.rollback()
            except:
                pass
            try:
                cur.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        raise  # 重新抛出异常以便上层处理

def insertFileInfo(fileInfo,datasource, conn=None, cur=None):
    """
    插入文件信息（支持事务）
    :param fileInfo: 文件信息字典
    :param datasource: 数据源配置
    :param conn: 可选，外部传入的数据库连接对象
    :param cur: 可选，外部传入的游标对象
    :return: 返回连接和游标对象(当外部传入时)，以便外部可以继续使用同一事务
    """
    close_conn = False
    local_sql_content = ''  # 使用局部变量
    try:
        if conn is None or cur is None:
            conn, cur = connect_to_mysql_with_retry(datasource)
            close_conn = True  # 标记为需要关闭连接

        # fileInfo.setdefault('id', idwork.get_id())
        fileInfo.setdefault('createTime', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        insertSql = ("INSERT INTO " + 't_supply_zx_acq_file_zfzcwj_copy2' +
                     " (id, table_name, table_id, file_type, file_name,chinese_file_name,chinese_file_path,is_main_file,file_path,href,create_time) " +
                     "VALUES (%(id)s, %(tableName)s, %(tableId)s,%(fileType)s,%(fileName)s,%(chinese_file_name)s,%(chinese_file_path)s,%(isMainFile)s,%(filePath)s,%(href)s,%(createTime)s)")

        # 执行插入操作
        cur.execute(insertSql, fileInfo)

        full_sql = cur._executed
        logging.info(f"=====================================执行完整的sql语句{full_sql}")
        dm_sql = sqlglot.transpile(full_sql, read="mysql", write="oracle")[0]
        local_sql_content += dm_sql + ';\n'

        if close_conn:
            conn.commit()
            cur.close()
            conn.close()
        return conn, cur, local_sql_content  # 返回局部变量
    except Exception as e:
        logging.error(f"插入文件信息时出现错误: {e}")
        if close_conn and conn:
            try:
                conn.rollback()
            except:
                pass
            try:
                cur.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        raise

def process_content_with_files(newsData, fileInfos, datasource):
    """
    先插入多个文件信息，再插入内容，使用同一事务
    :param newsData: 公告内容数据
    :param fileInfos: 文件信息列表（可能多个）
    :param datasource: 数据源配置
    """
    conn = None
    cur = None
    local_sql_content = ''  # 使用局部变量
    try:
        # 获取连接
        conn, cur = connect_to_mysql_with_retry(datasource)
        conn.begin()  # 开始事务

        # 先插入所有文件信息
        if fileInfos:
            for fileInfo in fileInfos:
                fileInfo['tableId'] = newsData['id']  # 确保文件关联到正确的内容ID
                conn, cur, file_sql = insertFileInfo(fileInfo, datasource, conn, cur)
                local_sql_content += file_sql

        # 对newsData的正文内容做处理
        # newsData['wjnr'] = html_to_text(newsData['wjnr'])
        # 标题去除特殊符号
        # newsData['title'] = deal_folder_name(newsData['title'])

        # 再插入内容
        conn, cur, content_sql = insertGgContent(newsData, datasource, conn, cur)
        local_sql_content += content_sql

        # 提交事务
        conn.commit()
        logging.info("事务提交成功")
        if local_sql_content != '':
            # 打开文件进行读取
            try:
                with open(get_sql_file_path(), 'r+', encoding='utf-8') as file:
                    # 使用portalocker进行跨平台文件锁定
                    portalocker.lock(file, portalocker.LOCK_EX)
                    try:
                        content = file.read()
                        content += local_sql_content
                        file.seek(0)
                        file.write(content)
                        file.truncate()
                    finally:
                        # 释放文件锁
                        portalocker.unlock(file)
            except Exception as e:
                logging.error(f"写入SQL文件时出错：{e}",exc_info=True)

    except Exception as e:
        logging.error(f"处理内容及文件时出错: {e}")
        if conn:
            try:
                conn.rollback()
                logging.info("已回滚事务")
            except Exception as rollback_error:
                logging.error(f"回滚事务时出错: {rollback_error}")
        raise
    finally:
        if cur:
            try:
                cur.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def html_to_text(html):
    """
    html转纯文本
    """
    # 创建 BeautifulSoup 对象
    soup = BeautifulSoup(html, 'html.parser')

    # 定义块级元素列表，这些元素通常表示段落或分隔
    block_elements = ['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'pre']

    # 遍历块级元素，在其后插入换行符
    for tag in soup.find_all(block_elements):
        tag.append('\n')

    # 对于 <br> 标签，也插入换行符
    for br in soup.find_all('br'):
        br.replace_with('\n')

    # 获取纯文本内容
    text = soup.get_text()

    # 去除多余的空白字符
    text = text.strip()

    return text


# 发送钉钉消息
def send_dingding_msg(content, is_at_all, mobiles):
    my_secret = 'SEC1ba0c81e3db411abe54838f87f0e1b6fb2059665e287cae7c749b260d7e69d50'
    my_url = 'https://oapi.dingtalk.com/robot/send?access_token=717eb26f548c7f552e7da911287c799694b8043241de627cabd9685961223ff8'
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

def upload_folder(sftp, local_folder, remote_folder):
    try:
        # 尝试创建远程文件夹
        sftp.mkdir(remote_folder)
        logging.info(f"创建远程目录: {remote_folder}")
    except FileExistsError:
        pass
    except Exception as e:
        logging.error(f"创建远程目录 {remote_folder} 失败: {e}")
        return

    # 遍历本地文件夹中的文件
    for file in os.listdir(local_folder):
        local_file_path = os.path.join(local_folder, file)
        if os.path.isfile(local_file_path):
            remote_file_path = os.path.join(remote_folder, file).replace("\\", "/")
            try:
                sftp.put(local_file_path, remote_file_path)
                logging.info(f"上传文件: {local_file_path} -> {remote_file_path}")
            except Exception as e:
                logging.error(f"上传文件 {local_file_path} 失败: {e}")

#  syy文件推送
def send_to_syy():
    sftpInfo = {
        'host': '218.19.148.220',
        'port': 17201,
        'user': 'xjkj',
        'passwd': 'Wbsj_Xjkj@205.207',
    }

    logging.info("开始推送文件")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    client.connect(sftpInfo.get('host'), sftpInfo.get('port'),
                   sftpInfo.get('user'), sftpInfo.get('passwd'),
                   banner_timeout=60, timeout=10800)
    sftp = client.open_sftp()
    time.sleep(3)
    try:
        # 上传文件夹
        upload_folder(sftp, local_save_folder, syy_server_push_file_path)
    except:
        logging.error("文件推送失败!",exc_info=True)
        sftp.close()
        client.close()
    sftp.close()
    client.close()
    pass

#  文件上传到api
# def push_data_to_api(max_retries=3, retry_delay=5):
#     r = redis.Redis(host=redis_ip, port=redis_port, db=redis_db, password=redis_password)
#
#     files_collection = get_folder_contents(fj_save_path) + get_folder_contents(sql_save_folder)
#
#     # 确保备份目录存在
#     if not os.path.exists(backup_dir):
#         os.makedirs(backup_dir)
#
#     for file_name in files_collection:
#         table_id = ''
#         if file_name in get_folder_contents(fj_save_path):
#             folder_path = fj_save_path
#             table_id = r.get(file_name)
#         else:
#             folder_path = sql_save_folder
#         file_path = os.path.join(folder_path, file_name)
#
#         # 如果sql文件大小为0则删除
#         if file_name in get_folder_contents(sql_save_folder):
#             try:
#                 file_size = os.path.getsize(file_path)
#                 if file_size == 0:
#                     os.remove(file_path)
#                     logging.info(f"已删除大小为 0 的文件: {file_name}")
#             except Exception as e:
#                 logging.error(f"删除文件 {file_name} 时出现错误: {e}")
#
#         # 检查是否为文件
#         if not os.path.isfile(file_path):
#             logging.warning(f"跳过非文件项: {file_path}")
#             continue
#
#         file_name_without_date = file_name
#         if file_name in get_folder_contents(fj_save_path) and '_' in file_name and '.' in file_name:
#             # 找到最后一个下划线的位置
#             underscore_index = file_name.rfind('_')
#             # 找到点号的位置
#             dot_index = file_name.rfind('.')
#             # 提取文件前缀部分
#             file_name_without_date = file_name[:underscore_index] + file_name[dot_index:]
#
#         retry_count = 0
#         while retry_count < max_retries:
#             try:
#                 # 使用 with 语句确保文件正确关闭
#                 # 使用 with 语句确保文件正确关闭
#                 with open(file_path, 'rb') as file:
#                     file_content = file.read()  # 读取文件内容到内存
#
#                 # 构建 multipart/form-data 请求体
#                 multipart_data = MultipartEncoder(
#                     fields={
#                         'fileName': file_name_without_date,
#                         'file': (file_name_without_date, file_content, 'application/octet-stream'),
#                         'table_id': table_id
#                     }
#                 )
#
#                 headers = {
#                     'Content-Type': multipart_data.content_type
#                 }
#
#                 # 发送 POST 请求
#                 response = requests.post(API_URL, data=multipart_data, headers=headers, timeout=30)
#
#                 # 检查响应状态码
#                 if response.status_code == 200:
#                     logging.info(f"文件 {file_name} 上传成功，响应内容：{response.text}")
#
#                     try:
#                         # 备份文件
#                         backup_file_path = os.path.join(backup_dir, file_name)
#                         shutil.copy2(file_path, backup_file_path)
#
#                         # 重命名原文件
#                         new_file_name = file_name + '.done'
#                         new_file_path = os.path.join(folder_path, new_file_name)
#
#                         # 确保文件没有被占用
#                         try:
#                             os.rename(file_path, new_file_path)
#                         except OSError as e:
#                             if e.winerror == 32:  # Windows 文件被占用错误
#                                 logging.warning(f"文件 {file_name} 暂时被占用，等待1秒后重试")
#                                 time.sleep(1)
#                                 os.rename(file_path, new_file_path)
#                             else:
#                                 raise
#
#                         logging.info(f"文件 {file_name} 处理完成：已备份并重命名")
#                         break  # 上传成功，跳出重试循环
#
#                     except (shutil.Error, OSError) as e:
#                         logging.error(f"文件 {file_name} 备份或重命名失败: {e}")
#                         # 继续重试
#
#                 else:
#                     logging.error(f"文件 {file_name} 上传失败，状态码：{response.status_code}，响应内容：{response.text}")
#                     # 继续重试
#
#             except requests.RequestException as e:
#                 logging.error(f'文件 {file_name} 上传发生错误: {e}')
#                 # 继续重试
#
#             except Exception as e:
#                 logging.error(f'文件 {file_name} 处理过程中发生未知错误: {e}')
#                 # 继续重试
#
#             retry_count += 1
#             if retry_count < max_retries:
#                 logging.info(f"将在 {retry_delay} 秒后进行第 {retry_count + 1} 次重试")
#                 time.sleep(retry_delay)
#             else:
#                 logging.error(f"文件 {file_name} 上传失败，已达到最大重试次数 {max_retries}")
#
#     return

def get_folder_contents(folder_path):
    """
    获取指定文件夹下的所有文件和文件夹，过滤掉后缀为 .done 的文件
    :param folder_path: 文件夹路径
    :return: 过滤后的文件和文件夹列表
    """
    contents = []
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path) and item.endswith('.done'):
            continue
        contents.append(item)
    return contents

def delete_done_files(folder_path):
    # 检查文件夹是否存在
    if not os.path.exists(folder_path):
        print(f"文件夹 {folder_path} 不存在。")
        return

    now = datetime.datetime.now()
    # 计算两天前的时间
    two_days_ago = now - datetime.timedelta(days=2)
    # 遍历文件夹下的所有文件和子文件夹
    try:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # 检查文件后缀是否为 .done
                if file.endswith('.done'):
                    file_path = os.path.join(root, file)
                    # 获取文件的创建时间
                    create_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
                    # 检查文件创建时间是否在两天前
                    if create_time <= two_days_ago:
                        # 删除文件
                        os.remove(file_path)
        logging.info("上传完成的SQL文件与附件已定时清理")
    except Exception as e:
        logging.error(f"清理过期文件时出错：{e}")
        pass

def deal_folder_name(title):
    """
    标题名称处理
    """
    # 定义正则表达式，匹配不允许的特殊字符
    pattern = r'[\\/*?:"<>|]'
    # 使用空字符串替换匹配到的特殊字符
    cleaned_name = re.sub(pattern, '', title)
    return cleaned_name

def remove_number_dot_prefix(file_name):
    """
    去除文件名中的数字.
    """
    pattern = r'^\d+\.\s?'
    result = re.sub(pattern, '', file_name, flags=re.MULTILINE)
    return result

def deal_with_file(file_url:str, save_folder:str, file_chinese_name:str, table_id:str):
    """
    文件添加时间戳后缀下载到本地，并存入redis
    """
    # 连接redis
    # try:
    #     r = redis.Redis(host=redis_ip, port=redis_port, db=redis_db, password=redis_password)
    # except redis.ConnectionError as e:
    #     logging.error(f'redis连接失败，请检查redis运行状态————{e}')
    #     return False
    # except Exception as e:
    #     logging.error(f'未知错误————{e}')
    #     return False
    # 发送请求获取文件内容
    try:

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(file_url, stream=True, headers = headers)
        response.raise_for_status()

        # 获取当前时间并格式化为所需的字符串
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        base_name, extension = os.path.splitext(file_chinese_name)
        # 为文件名添加时间戳后缀
        current_time_file_name = f"{base_name}_{timestamp}{extension}"
        file_path = os.path.join(save_folder, current_time_file_name)
        with open(file_path, 'wb') as file:
            # 从网络逐块读取数据并写入本地文件，避免内存不足
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        logging.info(f"文件下载成功，保存路径为: {file_path}")
        # r.set(current_time_file_name,table_id, ex=86400)
    except Exception as e:
        logging.error(f'文件处理出错————{e}')

# if __name__ == "__main__":
#     push_data_to_api()
