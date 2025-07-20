import datetime
import logging
import time
import os
import threading
from pathlib import Path

"""
脚本公共配置
"""

# redis配置信息
redis_ip = 'localhost'
redis_port = '6379'
redis_db = 5
redis_password = ''

# 获取项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
# 日志文件路径
log_filename = PROJECT_ROOT / 'logs/app.log'
# 附件保存路径
fj_save_path = PROJECT_ROOT / 'fj'
# sql文件保存路径
sql_save_folder = PROJECT_ROOT / 'sql'
# 最新sql文件路径
sql_file_path = ''
# 备份文件夹
backup_dir = PROJECT_ROOT / 'backup'

push_formatted_date = ''
local_save_folder = ''
# syy服务器附件推送路径
syy_server_push_file_path = ''
folder_name = ''

# 文件上传API 地址
API_URL = f'http://localhost:9088/uploadFiles'


# 驱动路径
chrome_driver = r"D:\pycharm\chrome\chromedriver.exe"

datasource_mysql = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'dbtype': 'mysql',
    'db': 'wbsj',
    'contentTable': 't_supply_zx_zczx'
}

# 初始化今天日期
def init_date():
    global push_formatted_date,local_save_folder,syy_server_push_file_path,folder_name
    today = datetime.date.today()
    formatted_date = today.strftime("%Y-%m-%d")
    push_formatted_date = today.strftime('%Y%m%d')
    folder_name = f"zczx_{formatted_date}"
    local_save_folder = os.path.join(os.path.dirname(__file__), folder_name)
    syy_server_push_file_path = f'/data/LawData/zczx/{push_formatted_date}/'
    pass

# 初始化创建当前时间的空sql文件
def init_sql_file():
    global sql_file_path
    today = datetime.datetime.today()
    formatted_datetime = today.strftime("%Y-%m-%d_%H_%M")
    sql_filename = f'insert_sql_{formatted_datetime}.sql'
    sql_file_path = sql_save_folder / sql_filename
    if not os.path.exists(sql_file_path):
        try:
            with open(sql_file_path,'w') as file:
                pass
            logging.info(f'sql文件创建成功---------{sql_file_path}')
        except Exception as e:
            logger.error(f'sql文件创建失败----------{sql_file_path},错误信息:{e}')
    return

def get_sql_file_path():
    return sql_file_path


def get_today_folder():
    init_date()
    return folder_name

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
    用于生成IDs的线程安全实现
    """
    _lock = threading.Lock()
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(IdWorker, cls).__new__(cls)
        return cls._instance

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
        
        # 初始化线程本地存储
        if not hasattr(self, '_local'):
            self._local = threading.local()
        if not hasattr(self._local, 'sequence'):
            self._local.sequence = sequence
        if not hasattr(self._local, 'last_timestamp'):
            self._local.last_timestamp = -1

    def _gen_timestamp(self):
        """
        生成整数时间戳
        :return:int timestamp
        """
        return int(time.time() * 1000)

    def get_id(self):
        """
        获取新ID（线程安全）
        :return:
        """
        with self._lock:
            # 确保线程本地存储已初始化
            if not hasattr(self._local, 'sequence'):
                self._local.sequence = 0
            if not hasattr(self._local, 'last_timestamp'):
                self._local.last_timestamp = -1

            timestamp = self._gen_timestamp()

            # 时钟回拨
            if timestamp < self._local.last_timestamp:
                logging.error('clock is moving backwards. Rejecting requests until{}'.format(self._local.last_timestamp))
                raise Exception

            if timestamp == self._local.last_timestamp:
                self._local.sequence = (self._local.sequence + 1) & SEQUENCE_MASK
                if self._local.sequence == 0:
                    timestamp = self._til_next_millis(self._local.last_timestamp)
            else:
                self._local.sequence = 0

            self._local.last_timestamp = timestamp

            new_id = ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT) | \
                    (self.datacenter_id << DATACENTER_ID_SHIFT) | \
                    (self.worker_id << WOKER_ID_SHIFT) | \
                    self._local.sequence
            return new_id

    def _til_next_millis(self, last_timestamp):
        """
        等到下一毫秒
        """
        timestamp = self._gen_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._gen_timestamp()
        return timestamp

# 创建全局单例实例
idwork = IdWorker(1, 1, 0)




