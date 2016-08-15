# -*- coding: utf-8 -*-
from __future__ import division
import tushare as ts
import pandas as pd
import numpy as np
import uuid as uuid
from pandas import Series
import datetime
import os
import cx_Oracle

class DataInt(object):
    dict_zb={}

    filePath = ""

    def __init__(self):

        self.dict_zb={'secid':'300014','stock_code':'300014','indic':{'kama': {'weight':0.2,'parameter':5},'tema':{'weight':0.32,'parameter':12}}

                      }

    def getStorPath(self):
        filePath = "/datastor/"
        return filePath

    def getDict(self):
        return self.dict_zb

    def getSecData(self, secid, startdate, enddate):
        df = ts.get_hist_data(secid, start=startdate, end=enddate)

        nowdate = self.getCurrentTime()

        df.to_csv(self.getStorPath()+secid+"_"+nowdate+".txt")

        return df
    #
    def getSecDateFromTxt(self,path):
        df=pd.read_csv(path)
        return df
    def getCurrentTime(self):
        now = datetime.datetime.now()
        nowdate = now.strftime('%Y-%m-%d')
        return nowdate
    def getTimeFromToday(self,ddays):
        now = datetime.datetime.now()
        delta = datetime.timedelta(days=ddays)
        n_days = now + delta
        return n_days.strftime('%Y-%m-%d')


    def createData(self,code):
        # print "start program"
        #
        # ma = matest()
        # d1 = ma.getDict()
        # pwd = os.getcwd()
        #
        # filename = d1.get('secid') + "_" + ma.getCurrentTime() + ".txt"
        # beginDate = "";
        # endDate = "";
        #
        # if os.path.isfile(ma.getStorPath() + filename):
        #     df = ma.getSecDateFromTxt(ma.getStorPath() + filename)
        #     print '数据已存在'
        # else:
        #     df = ma.getSecData(d1.get('secid'), ma.getTimeFromToday(-900), ma.getCurrentTime())
        #     print '通过互联网下载完毕'

        # a = sc_get()
        # for sci in range(len(a['STOCKCODE'])):
        #     #        a['STOCKCODE'][sci]='300344'
        #     print('sci', sci, a['STOCKCODE'][sci])
        data = price_get(stock_code=code)
        return data


# 数据获取函数：
def price_get(stock_code='300109'):
    db = cx_Oracle.connect('mc4', 'mc4998', '192.168.0.18:1521/racdb')

    sql = '''
                select trdate,v_open*(adj_price/v_close) op,v_high*(adj_price/v_close) hi,v_low*(adj_price/
        v_close) lo,adj_price cl,v_change ch          from stk_mkt_ex_t where stkcode='%s' order by trdate
                '''
    data = pd.read_sql(sql % (stock_code,), db)
    return data


if __name__ == "__main__":
    print "start program"

    ma=DataInt()
    d1 = ma.getDict()
    pwd=os.getcwd()

    filename=d1.get('secid')+"_"+ma.getCurrentTime()+".txt"
    beginDate="";
    endDate="";
    ma.sc_get()
    ma.createData()





