# -*- coding:utf-8 -*-
# pyhdfs库
from pyhdfs import HdfsClient
import datetime
from email.mime.text import MIMEText
import smtplib
from email.header import Header

mail_to_list = ["jason@xiaoxiaomo.com"]  # 邮件收件人，多个用逗号隔开
mail_host = ""  # 邮件服务器
mail_user = "jason@xiaoxiaomo.com"  # 邮箱账户名

client = HdfsClient("hostname")
ROOT_DIR = "/user/hive/warehouse/email.db/"
CHECK_TABLE = [
    "table",
    "t_xiaoxiaomo"
]


# 发送邮件
def send_mail(text):
    head = "【重要】ETL监控HDFS数据异常"
    message = MIMEText(text, 'html', 'utf-8')
    message['From'] = Header(mail_user, 'utf-8')
    message['To'] = Header(";".join(mail_to_list), 'utf-8')
    message['Subject'] = Header(head, 'utf-8')

    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.sendmail(mail_user, mail_to_list, message.as_string())
        server.quit()
        print '运行失败！邮件发送成功:'+head
    except Exception, _e:
        print "Error:无法发送邮件", str(_e)


def today():
    return datetime.date.today()


def yesterday():
    return today() - datetime.timedelta(days=1)


# 执行主方法
if __name__ == '__main__':
    print "监控HDFS......"
    yesterday_datetime_format = yesterday()
    for table in CHECK_TABLE:
        is_success = False
        has_data = False
        content = ""
        try:
            path = ROOT_DIR + table + "/" + str(yesterday_datetime_format)
            if client.exists(path):
                client_list = client.list_status(path)
                for file_status in client_list:
                    if (file_status.get("pathSuffix").startswith('part-')) and (int(file_status.get("length")) > 0):
                        has_data = True
                    elif file_status.get("pathSuffix").__eq__("_SUCCESS"):
                        is_success = True
            else:
                content = "异常信息：HDFS路径不存在 <br>" + \
                          str("HDFS路径：") + path
        except Exception, e:
            content = "异常信息：" + str(e) + "<br>" + \
                      str("HDFS路径：") + path

        if (content == "") and (not is_success):
            content = "异常信息：" + table + "相关job运行失败" + "<br>" + \
                      str("HDFS路径：") + path

        if (content == "") and (not has_data):
            content = "异常信息：" + table + "相关job运行无数据" + "<br>" + \
                      str("HDFS路径：") + path

        if content != "":
            send_mail(content)
        else:
            print "运行成功！date：" + str(yesterday_datetime_format) + " table:" + table


