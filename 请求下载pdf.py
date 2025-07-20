import pdfkit
path_wkhtmltopdf = r'D:\ProgramFiles\wkhtmltopdf\bin\wkhtmltopdf.exe'  # 替换为你的实际路径
config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
pdfkit.from_url("https://www.nea.gov.cn/2024-05/20/c_1310775384.htm", 'out1.pdf',configuration=config)
print(f'成功下载正文pdf')