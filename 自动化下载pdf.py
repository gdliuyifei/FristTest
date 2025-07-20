
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
from selenium import webdriver

# 驱动路径
chrome_driver = r"D:\ProgramFiles\chromedriver\chromedriver-win64(1)\chromedriver.exe"
url = "https://guangfu.bjx.com.cn/news/20240913/1400504.shtml"
def init_driver():
    chrome_options = Options()

    #启用无头模式，后台运行
    # chrome_options.add_argument("--headless")
    # 创建 Service 对象
    service = Service(chrome_driver)

    #创建 chrom 浏览器实例，同时传入chromeOptions
    return webdriver.Chrome(service=service,options=chrome_options)

driver = init_driver()
driver.set_window_size(1200, 2000)
driver.get(url)

# 调整PDF参数（A4纵向+适当边距）
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
import base64
with open('output4.pdf', 'wb') as f:
    f.write(base64.b64decode(pdf_data['data']))

