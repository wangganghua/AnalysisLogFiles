# -*- encoding:utf-8 -*-
# time: 2018-6-25
# author: xxxxx
# python3
# pip install pandas、（取消使用）
# import pandas as pd
import re
from connectionmysql.connectionmysql import ConnectionMySql
from datetime import datetime
import time


def analysis():
    sqltablename = "bc_importantlog"
    # header=None:没有每列的column name，可以自己设定
    # encoding='gb2312':其他编码中文显示错误
    # delim_whitespace=True:用空格来分隔每行的数据
    # index_col=0:设置第1列数据作为index
    # sep = ',' 间隔符
    # data = pd.read_table('box.log', header=None, encoding='gb2312', delim_whitespace=False, sep=',', index_col=0)
    # print(len(data.values))
    # print(data.values)
    fw = open("box.log", "r")
    # fw = open("box_3333-2018-6-20-9-57.log", "r")
    fr = fw.readlines()
    index = 1
    listdata = []
    for line in fr:
        cmdtime = str(re.search("(?<=).*?(?=,BOX>>>)", line).group())
        cmdtime = time.mktime(time.strptime(cmdtime, "%a %b %d %H:%M:%S %Y"))

        # 1、操作时间
        ax_operatingtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cmdtime))

        # 2、用户
        ax_users = str(re.search("(?<=user:).*?(?=,)", line).group()).strip()

        # 3、ip
        ax_ip = str(re.search("(?<=ip:).*?(?=,)", line).group()).strip()

        # 4、端口
        ax_port = str(re.search("(?<=port:).*?(?=,)", line).group()).strip()

        cmd = str(re.search("(?<=cmd:).*?(?=ret:)", line).group()).strip()
        re_rccmd = re.compile(r",$")
        cmd = str(re_rccmd.sub("", cmd)).strip()
        # 5、操作命令
        ax_operating = str(re.search("(?<=\[).*?(?=\])", cmd).group()).strip()

        # 6、截取 操作命令后的文件连接、等等
        ax_goback = str(re.search("(?<=\]).*", cmd).group()).strip()

        # 7、添加者
        ax_addauther = "root"

        # 8、当前添加时间
        ax_ret = str(re.search("(?<=ret:).*", line).group()).strip()

        # 9、类型
        ax_type = 1

        listdata.append((ax_operatingtime, ax_users, ax_ip, ax_port, ax_operating, ax_goback, ax_addauther, ax_ret, ax_type))
        index += 1
        if index >= 5000:
            wx = ConnectionMySql("insert into {0}(ax_operatingtime, ax_users, ax_ip, ax_port, ax_operating, ax_goback, ax_addauther, ax_ret, ax_type)  values (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
            sqltablename))
            wx.inserintoMySql(listdata)
            # 初始化变量
            index = 0
            listdata = []
    # 保存剩余的数据
    if len(listdata) > 0:
        wx = ConnectionMySql(
            "insert into {0}(ax_operatingtime, ax_users, ax_ip, ax_port, ax_operating, ax_goback, ax_addauther, ax_ret, ax_type)  values (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
            sqltablename))
        wx.inserintoMySql(listdata)
    # 初始化变量
    index = 0
    listdata = []
    fw.close()


if __name__ == "__main__":
    print(u"开始: %s" % datetime.now())
    analysis()
    print(u"结束: %s" % datetime.now())
    # WGH = "Wed Jun 20 09:57:04 2018"
    # wx = time.mktime(time.strptime(WGH, "%a %b %d %H:%M:%S %Y"))
    # print(wx)
    # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(wx)))
