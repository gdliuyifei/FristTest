import logging
import time
from datetime import datetime
# import paramiko
import base64
import hashlib
import hmac

# from apscheduler.schedulers.blocking import BlockingScheduler
from requests import request
import urllib
import os

# 驱动路径
chrome_driver = r"/home/zxdm/chrome/chromedriver-linux64/chromedriver"

push_formatted_date = ''
local_save_folder = ''
# syy服务器附件推送路径
syy_server_push_file_path = ''

# 初始化今天日期
def init_date():
    global push_formatted_date,local_save_folder,syy_server_push_file_path
    today = datetime.date.today()
    formatted_date = today.strftime("%Y-%m-%d")
    push_formatted_date = today.strftime('%Y%m%d')
    folder_name = f"ldjhjqbyw_{formatted_date}"
    local_save_folder = os.path.join(os.path.dirname(__file__), folder_name)
    syy_server_push_file_path = f'/data/LawData/ldjhjqbyw/{push_formatted_date}/'
    pass

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




def insertGgContent(newsData,datasource):
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
    conn.close()
#插入文件关联表
def insertFileInfo(fileInfo,datasource):
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
    insertSql = ("INSERT INTO " + 't_supply_zx_acq_file_zfzcwj_copy2' +
                 " (id, table_name, table_id, file_type, file_name,chinese_file_name,chinese_file_path,is_main_file,file_path,href,create_time) " +
                 "VALUES (%(id)s, %(tableName)s, %(tableId)s,%(fileType)s,%(fileName)s,%(chinese_file_name)s,%(chinese_file_path)s,%(isMainFile)s,%(filePath)s,%(href)s,%(createTime)s)")

    # 执行插入操作
    cur.execute(insertSql, fileInfo)

    # 提交事务并关闭连接
    conn.commit()
    cur.close()
    conn.close()


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
        pass

    # 遍历本地文件夹中的文件
    for file in os.listdir(local_folder):
        local_file_path = os.path.join(local_folder, file)
        if os.path.isfile(local_file_path) and not file.lower().endswith('.done'):
            remote_file_path = os.path.join(remote_folder, file).replace("\\", "/")
            try:
                sftp.put(local_file_path, remote_file_path)
                logging.info(f"上传文件: {local_file_path} -> {remote_file_path}")

                # 给本地已上传的文件添加 .done 后缀
                done_file_path = local_file_path + '.done'
                os.rename(local_file_path, done_file_path)
                logging.info(f"已将本地文件 {local_file_path} 重命名为 {done_file_path}")

            except Exception as e:
                logging.error(f"上传文件 {local_file_path} 失败: {e}")

#  syy文件推送
# def send_to_syy():
#     sftpInfo = {
#         'host': '218.19.148.220',
#         'port': 17201,
#         'user': 'xjkj',
#         'passwd': 'Wbsj_Xjkj@205.207',
#     }
#
#     logging.info("开始推送文件")
#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
#     client.connect(sftpInfo.get('host'), sftpInfo.get('port'),
#                    sftpInfo.get('user'), sftpInfo.get('passwd'),
#                    banner_timeout=60, timeout=10800)
#     sftp = client.open_sftp()
#     time.sleep(3)
#     try:
#         # 上传文件夹
#         upload_folder(sftp, local_save_folder, syy_server_push_file_path)
#     except:
#         logging.error("文件推送失败!",exc_info=True)
#         sftp.close()
#         client.close()
#     sftp.close()
#     client.close()
#     pass

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




