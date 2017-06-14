# -*- coding:utf-8 -*-
import re
import datetime
import urllib2
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
import smtplib
from email.header import Header

mail_to_list = ["jason@xiaoxiaomo.com"]  # 邮件收件人，多个用逗号隔开
mail_host = ""  # 邮件服务器
mail_user = "jason@xiaoxiaomo.com"  # 邮箱账户名

# 线上环境spark页面
spark_job_url = "http://master:8088/cluster/apps/RUNNING"  # spark集群正在运行的任务地址
task_names = [
    "topicName2",
    "topicName"]  # 任务名称，多个任务用逗号分隔
time_delayed = 10


# 发送邮件
def send_mail(text):
    message = MIMEText(text, 'html', 'utf-8')
    message['From'] = Header(mail_user, 'utf-8')
    message['To'] = Header(";".join(mail_to_list), 'utf-8')
    message['Subject'] = Header("【重要】Spark Streaming 监控预警", 'utf-8')

    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.sendmail(mail_user, mail_to_list, message.as_string())
        server.quit()
        print '发送成功'
    except Exception, e:
        print "Error:无法发送邮件", str(e)


# 根据任务名称获取任务跳转地址URL
def get_url(script_str, task_name):
    href = ""
    try:
        for tt in script_str.string.splitlines():
            if task_name in tt.rstrip():
                href = BeautifulSoup(tt,"lxml").findAll("a", {"href": re.compile("^http:.*")})[0].attrs["href"]

    except Exception, e:
        print "解析标签出错", str(e)
    return href


# 获取任务运行地址任务运行时间状态
def get_task_status(url):
    url += "streaming/"
    HTML = ""
    time = ""
    try:
        HTML = urllib2.urlopen(url)
    except Exception, e:
        print "打开url出错", str(e), str(url)
    try:
        bs_obj = BeautifulSoup(HTML.read(),"lxml")
        time = bs_obj.findAll("table", {"id": "completed-batches-table"})[0].find("tbody").findAll("tr")[0].findAll("td")[0].findAll("a")[0].get_text()
    except Exception, e:
        print "解析标签出错", str(e)
    return time


# 计算指定的时间与当前时间差的秒数
def get_time_diff(t_str):
    # type: (long) -> long
    start_time = datetime.datetime.now()
    end_time = datetime.datetime.strptime(t_str, '%Y/%m/%d %H:%M:%S')
    timedelta = abs(start_time - end_time)
    return timedelta.days * 24 * 3600 + timedelta.seconds


# 检测是否延时过长,超过了指定的阀值
def check_delayed(time, url, task_name):

    # 延时指定时间则发送报警邮件
    if time != "":
        delayed = get_time_diff(time.strip())
        if (delayed > time_delayed):
            url += "streaming/"
            content_info = "任务延时：" + str(delayed) + str("秒") + "<br>" + \
                        str("任务名称：") + str(task_name) + "<br>" + \
                        str("任务url：") + str(url)
            send_mail(content_info)
        else:
            print "任务:" + str(task_name) + "检测正常"
    else:
        pass

# 执行主方法
if __name__ == '__main__':

    print "启动监控程序......"

    try:
        html = urllib2.urlopen(spark_job_url)
        bsObj = BeautifulSoup(html.read(),"lxml")

        # 通过BeautifulSoup解析的html 在javascript脚本中获取数据
        script_str = bsObj.findAll("script")[6]

        # 通过task_name 获取task_url
        for task_name in task_names:

            # 如果没有找到该任务，即该job已近挂掉
            task_url = get_url(script_str, task_name)
            if task_url == '':
                content = "spark任务中没有找到运行的任务：" + str("job 地址：") + spark_job_url
                send_mail(content)
            else:
                run_time = get_task_status(task_url)
                check_delayed(run_time, task_url, task_name)

    except Exception, e:
        print "打开url出错", str(e), str(spark_job_url)

