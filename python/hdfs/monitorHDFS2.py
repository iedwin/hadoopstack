# -*- coding:utf-8 -*-
# hdfs库
from hdfs.client import Client
import datetime
from email.mime.text import MIMEText
import smtplib
from email.header import Header

mail_to_list = ["jason@xiaoxiaomo.com"]  # 邮件收件人，多个用逗号隔开
mail_host = ""  # 邮件服务器
mail_user = "jason@xiaoxiaomo.com"  # 邮箱账户名

client = Client("http://namenode:50070")
ROOT_DIR = "/user/hive/"
CHECK_TABLE = [
    "table",
    "t_xiaoxiaomo"
]


# 发送邮件
def send_mail(text):
    message = MIMEText(text, 'html', 'utf-8')
    message['From'] = Header(mail_user, 'utf-8')
    message['To'] = Header(";".join(mail_to_list), 'utf-8')
    message['Subject'] = Header("【重要】ETL监控HDFS数据异常", 'utf-8')

    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.sendmail(mail_user, mail_to_list, message.as_string())
        server.quit()
        print '发送成功'
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
            client_list = client.list(path, True)
            for i in range(0, len(client_list)):
                if (client_list[i][0].startswith('part-')) and (int(client_list[i][1].get("length")) > 0):
                    has_data = True
                elif client_list[i][0].__eq__("_SUCCESS"):
                    is_success = True

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
