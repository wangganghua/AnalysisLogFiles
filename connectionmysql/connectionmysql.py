# -*- encoding:utf-8 -*-
# time: 2018-6-25
# author: xxxxx
# python3
# pip install PyMySQL
import pymysql


class ConnectionMySql:
    strsql = "none"
    # 连接MySql字符串
    user = "root"  # 用户名
    password = "root"  # 密码
    host = "192.168.2.245"  # 数据库ip
    # timeout = 3306  # 超时时间设置
    database = "analysisdata"  # 数据库

    def __init__(self, strsql):
        ConnectionMySql.strsql = strsql

    # 查询MySql数据库并 return 查询结果数组
    def selectMySqlandreturn(self):
        istrue = True
        index = 1
        while istrue:
            try:
                con = pymysql.connect(user="%s" % self.user, password="%s" % self.password, host="%s" % self.host,
                                      charset="utf8", database="%s" % self.database)
            except pymysql.OperationalError:
                # wr = SaveErrorLogsFile("连接MySql数据库错误：%s" % e.message)
                # wr.saveerrorlog()
                istrue = True
                index += 1
                print("开始连接MySql第 %s 次" % index)
            else:
                istrue = False
                cur = con.cursor()
                sql = self.strsql
                try:
                    cur.execute(sql)
                except Exception:
                    # wr = SaveErrorLogsFile("查询MySql数据库错误信息：%s".decode("utf8").encode("gbk") % e.message)
                    # wr.saveerrorlog()
                    break
                returnax = []
                for ix in cur:
                    returnax.append(ix)
                cur.close()
                con.commit()
                con.close()
                return returnax

    def inserintoMySql(self, values):
        istrue = True
        wa = 1
        while istrue:
            try:
                con = pymysql.connect(user="%s" % self.user, password="%s" % self.password, host="%s" % self.host,
                                      charset="utf8", database="%s" % self.database)
            except pymysql.OperationalError:
                # wr = SaveErrorLogsFile("连接MySql数据库错误：%s".encode("gbk") % e.message)
                # wr.saveerrorlog()
                istrue = True
                wa += 1
                print ("开始连接MySql第 %s 次" % wa)
            else:
                istrue = False
                cur = con.cursor()
                sql = self.strsql
                try:
                    cur.executemany(sql, values)
                except pymysql.OperationalError:
                    istrue = False
                    print ("error ")
                    print (sql)
                    print (values)
                    # wr = SaveErrorLogsFile("插入数据错误：%s".encode("gbk") % e.message)
                    # wr.saveerrorlog()
                    con.rollback()
                    cur.close()
                    con.close()
                else:
                    cur.close()
                    con.commit()
                    con.close()

    def executeMySql(self):
        istrue = True
        wa = 1
        while istrue:
            try:
                con = pymysql.connect(user="%s" % self.user, password="%s" % self.password, host="%s" % self.host,
                                       charset="utf8", database="%s" % self.database)
            except pymysql.OperationalError:
                # wr = SaveErrorLogsFile("连接MySql数据库错误：%s" % e.message)
                # wr.saveerrorlog()
                istrue = True
                wa += 1
                print ("开始连接MySql第 %s 次" % wa)
            else:
                istrue = False
                cur = con.cursor()
                sql = self.strsql
                try:
                    cur.execute(sql)
                except pymysql.OperationalError:
                    istrue = False
                    # wr = SaveErrorLogsFile("执行execute错误信息：%s".encode("gbk") % e.message)
                    # wr.saveerrorlog()
                    con.rollback()
                    cur.close()
                    con.close()
                else:
                    cur.close()
                    con.commit()
                    con.close()