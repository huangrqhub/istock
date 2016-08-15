# -*- coding: utf-8 -*-
# modify by hrq
# 2016-8-1
from __future__ import division
import math
import numpy as np
import pandas as pd
import talib as ta
import dataint
import cx_Oracle
import random as rd
from pymongo import MongoClient
import scipy.stats.mstats as mstats
import configer
from etl5 import ETL as etl
from ex import EX as ex
import multiprocessing
import os, time
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

#读取配置文件，获得数据库参数
config = configer.Config('base.ini')
mongo_url = config.get('database', 'mongo_url')
mongo_port = config.get('database', 'mongo_port')
mongo_sid = config.get('database', 'mongo_sid')
mongo_user = config.get('database', 'mongo_user')
mongo_passwd = config.get('database', 'mongo_passwd')


def dowork1(socklist,mode,seq):
    
    client = MongoClient('mongodb://'+mongo_user+':'+mongo_passwd+'@'+mongo_url+':'+mongo_port+'/'+mongo_sid)
    mdb = client.mcdb
    mgconnect = mdb.ga
    di = dataint.DataInt()
    dp = DataProcUnit()
    i=0

    for sci in range(len(socklist)):
        #print socklist.STKCODE
        code = socklist.STKCODE.iloc[i]
        data = di.createData(code)
        i+=1
        if len(data) < 30:
            print('可交易天数少于30 days，不予处理')
            print "code is:", code
            continue

        dict_zb = mgconnect.find_one({'stock_code': code})

        dict_zb1 = {'secid': code, 'stock_code': code,
                    'opt_indic': {'kama': {'weight': 0.2, 'parameter': 15},
                                  'tema': {'weight': 0.32, 'parameter': 30}}
                    }

        if dict_zb == None:
            dict_zb = dict_zb1

        dp.processData(data, dict_zb, mode, buylist=[], salelist=[], positionlist=[], emptylist=[])

def get_r(data):
    x = [8,13,21,34,55]

    cl = np.array(data['CL'],dtype='f8')
    for i in x:
        data['m'+str(i)] = ta.EMA(cl,timeperiod=i)

    for i,r in data.iterrows():
        z = [[r['m8'],8],[r['m13'],13],[r['m21'],21],[r['m34'],34],[r['m55'],55]]
        c,p = mstats.spearmanr(x,[k[1] for k in sorted(z,reverse=True)])
        data.loc[i,'r'] = c
    return data['r']

#双均线交叉：
def MA_Cross(price_dt,short_ma=10,long_ma=20):

    s=ta.SMA(np.array(price_dt['CL'],dtype='f8'),timeperiod=short_ma)
    l=ta.SMA(np.array(price_dt['CL'],dtype='f8'),timeperiod=long_ma)
    signal=pd.DataFrame({'s':s,'l':l})
    signal.loc[(signal['s']>signal['l']),'bs']=1
    signal.loc[(signal['s']<signal['l']),'bs']=0
    return signal['bs']


class DataProcUnit(object):

    def __init__(self):
        self.opt_ks = 0
        self.opt_kl = 0  # 指标计算：
        #读取配置文件信息
        self.oraurl = config.get('database', 'oraurl')
        self.oraport=config.get('database', 'oraport')
        self.orasid=config.get('database', 'orasid')
        self.ora_user=config.get('database', 'ora_user')
        self.ora_passwd=config.get('database', 'ora_passwd')
        
        self.db = cx_Oracle.connect(self.ora_user, self.ora_passwd, self.oraurl+":"+self.oraport+"/"+self.orasid)
        self.cur = self.db.cursor()


    # 指标计算：
    def indic_cal(self,price_data, indic_dict,isfrist):

        chromo_fun = {'dema': ta.DEMA, 'ema': ta.EMA, 'kama': ta.KAMA, 'midpoint': ta.MIDPOINT, 'sma': ta.SMA,
                      'tema': ta.TEMA, 'trima': ta.TRIMA, 'wma': ta.WMA, 'macd': ta.MACD,
                      't3': ta.T3, 'kdj': ta.STOCH, 'mom': ta.MOM, 'roc': ta.ROC, 'rsi': ta.RSI, 'trix': ta.TRIX,
                      'ma_cross': MA_Cross, 'ema_ali': get_r}
        indictor0 = ['dema', 'ema', 'kama', 'sma', 'tema', 'trima']  # midpoint,wma被删除
        for k in indic_dict:
            if k in indictor0:
                price_data[k] = chromo_fun[k](real=np.array(price_data['CL'], dtype='f8'),
                                              timeperiod=indic_dict[k]['parameter'])
                price_data.loc[price_data[price_data['CL'] > price_data[k]].index, k + '_SIGNAL'] = 1
                price_data.loc[price_data[price_data['CL'] < price_data[k]].index, k + '_SIGNAL'] = 0
            if k == 'ma_cross':
                price_data['ma_cross_SIGNAL'] = chromo_fun['ma_cross'](price_dt=price_data,
                                                                       short_ma=indic_dict['ma_cross']['parameter'][
                                                                           'ma_cross_sma'],
                                                                       long_ma=indic_dict['ma_cross']['parameter'][
                                                                           'ma_cross_lma'])
            if k == 'rsi':
                price_data['rsi'] = chromo_fun['rsi'](real=np.array(price_data['CL'], dtype='f8'),
                                                      timeperiod=indic_dict['rsi']['parameter'])
                price_data.loc[price_data[price_data['CL'] > price_data['rsi']].index, 'rsi_SIGNAL'] = 1
                price_data.loc[price_data[price_data['CL'] < price_data['rsi']].index, 'rsi_SIGNAL'] = 0
            if k == 'ema_ali':
                price_data['ema_ali'] = chromo_fun['ema_ali'](price_data)
                price_data.loc[price_data[0 < price_data['ema_ali']].index, 'ema_ali_SIGNAL'] = 1
                price_data.loc[price_data[0 > price_data['ema_ali']].index, 'ema_ali_SIGNAL'] = 0
            if k == 'roc':
                price_data['roc'] = chromo_fun['roc'](real=np.array(price_data['CL'], dtype='f8'),
                                                      timeperiod=indic_dict['roc']['parameter'])
                price_data.loc[price_data[price_data['CL'] > price_data['roc']].index, 'roc_SIGNAL'] = 1
                price_data.loc[price_data[price_data['CL'] < price_data['roc']].index, 'roc_SIGNAL'] = 0
            if k == 'mom':
                price_data['mom'] = chromo_fun['mom'](real=np.array(price_data['CL'], dtype='f8'),
                                                      timeperiod=indic_dict['mom']['parameter'])
                price_data.loc[price_data[price_data['CL'] > price_data['mom']].index, 'mom_SIGNAL'] = 1
                price_data.loc[price_data[price_data['CL'] < price_data['mom']].index, 'mom_SIGNAL'] = 0
            if k == 'macd':
                price_data['macd'], price_data['macdsignal'], price_data['macdhist'] = chromo_fun['macd'](
                    np.array(price_data['CL'], dtype='f8'))
                price_data.loc[price_data[0 < price_data['macd']].index, 'DIFF'] = 1
                price_data.loc[price_data[0 < price_data['macdsignal']].index, 'DEA'] = 1
                price_data.loc[price_data[price_data['macd'] > price_data['macdsignal']].index, 'D'] = 1
                price_data.loc[price_data[0 > price_data['macd']].index, 'DIFF'] = 0
                price_data.loc[price_data[0 > price_data['macdsignal']].index, 'DEA'] = 0
                price_data.loc[price_data[price_data['macd'] < price_data['macdsignal']].index, 'D'] = 0
                price_data['macd_SIGNAL'] = price_data['DIFF'] * price_data['DEA'] * price_data['D']
            if k == 't3':
                price_data['t3'] = chromo_fun['t3'](real=np.array(price_data['CL'], dtype='f8'))
                price_data.loc[price_data[price_data['CL'] > price_data['t3']].index, 't3_SIGNAL'] = 1
                price_data.loc[price_data[price_data['CL'] < price_data['t3']].index, 't3_SIGNAL'] = 0

            if k == 'kdj':
                price_data['kdj_slowk'], price_data['kdj_slowd'] = chromo_fun['kdj'](
                    np.array(price_data['HI'], dtype='f8'), np.array(price_data['LO'], dtype='f8'),
                    np.array(price_data['CL'], dtype='f8'))
                price_data.loc[
                    price_data[price_data['kdj_slowk'] > price_data['kdj_slowd']].index, 'kdj' + '_SIGNAL'] = 1
                price_data.loc[
                    price_data[price_data['kdj_slowk'] < price_data['kdj_slowd']].index, 'kdj' + '_SIGNAL'] = 0


        l = len(price_data)

        signal = 0
        signal_pre=0
        #buyflag=1  buy
        #buyflag=0  sale
        if isfrist:
            price_data['buy_SIGNAL']=0
            for k in indic_dict:
                    price_data[k+'_WSIGNAL']=price_data[k + '_SIGNAL'] * indic_dict[k]['weight']
                    price_data['buy_SIGNAL']+=price_data[k+'_WSIGNAL']

            price_data.loc[price_data['buy_SIGNAL']>=0.5,'buy_SIGNAL']=1
            price_data.loc[price_data['buy_SIGNAL']<0.5,'buy_SIGNAL']=-1
            #去除为nan的数据

            return price_data[['TRDATE','buy_SIGNAL']].dropna()

        else:

            rsdict={}
            for k in indic_dict:

                signal += price_data[k + '_SIGNAL'].iloc[l - 1] * indic_dict[k]['weight']
                signal_pre += price_data[k + '_SIGNAL'].iloc[l - 2] * indic_dict[k]['weight']

            if signal >=0.5:
                rsdict.setdefault('TRDATE',price_data['TRDATE'].iloc[l - 1])
                rsdict.setdefault('buy_SIGNAL',1)
            else:
                rsdict.setdefault('TRDATE', price_data['TRDATE'].iloc[l - 1])
                rsdict.setdefault('buy_SIGNAL', -1)

            if signal_pre >=0.5:
                rsdict.setdefault('TRDATE_PRE', price_data['TRDATE'].iloc[l - 2])
                rsdict.setdefault('buy_SIGNAL_PRE', 1)
            else:
                rsdict.setdefault('TRDATE_PRE', price_data['TRDATE'].iloc[l - 2])
                rsdict.setdefault('buy_SIGNAL_PRE', -1)
            return rsdict


# 运算处理
# buylist , salelist,
    def processData(self,data,dictzb,opt,buylist,salelist,positionlist,emptylist):

        if int(opt) == 9:
            #clear bs_t1 data
            procResult = self.indic_cal(data,dictzb['opt_indic'],True)

        else:
            procResult = self.indic_cal(data, dictzb['opt_indic'], False)
        #判断处理结果返回的类型

        if isinstance(procResult,pd.DataFrame):
            #procResult = procResult.sort(ascending=False)
            length = len(procResult)
            #循环计算买卖点 比较规则为 第二天的指标相对与前一天的指标发生改变的时候，
            #进行标记 相减为1 发出买 为 -1 发出 卖

            if length>1:
                if(procResult['buy_SIGNAL'].iloc[0]) == 1:
                    buylist.append(procResult.iloc[0])
                else:
                    salelist.append(procResult.iloc[0])


            for index in xrange(0,length-1):

                if procResult['buy_SIGNAL'].iloc[index+1] - procResult['buy_SIGNAL'].iloc[index] == 2:
                    buylist.append(procResult.iloc[index+1])
                if procResult['buy_SIGNAL'].iloc[index+1] - procResult['buy_SIGNAL'].iloc[index] == -2:
                    salelist.append(procResult.iloc[index+1])
                if procResult['buy_SIGNAL'].iloc[index+1] - procResult['buy_SIGNAL'].iloc[index] == 0:
                    if procResult['buy_SIGNAL'].iloc[index+1]==1:
                        positionlist.append(procResult.iloc[index+1])
                    else:
                        emptylist.append(procResult.iloc[index+1])

            sql = '''insert into bs_t1 values ('%s',%s,to_date('%s','yyyymmdd'))'''
            #将买卖结果指标写入数据库
            bslist = buylist+salelist

            for i in xrange(0,len(bslist)):
                obj = bslist[i]
                #write into database
                self.cur.execute(sql % (dictzb['stock_code'], obj['buy_SIGNAL'], obj['TRDATE'].strftime('%Y%m%d')))

            self.db.commit()

        else:

            if procResult['buy_SIGNAL'] - procResult['buy_SIGNAL_PRE'] == 2:
                buylist.append(procResult)
            if procResult['buy_SIGNAL'] - procResult['buy_SIGNAL_PRE'] == -2:
                salelist.append(procResult)
            if procResult['buy_SIGNAL'] - procResult['buy_SIGNAL_PRE'] == 0:
                if procResult['buy_SIGNAL'] == 1:
                    positionlist.append("2")
                else:
                    emptylist.append("-2")

            # write into database

            sql = '''insert into bs_t1 values ('%s',%s,to_date('%s','yyyymmdd'))'''
            # 将买卖结果指标写入数据库
            bsolist = buylist + salelist

            for i in xrange(0, len(bsolist)):
                obj = bsolist[i]
                # write into database
                self.cur.execute(sql % (dictzb['stock_code'], obj['buy_SIGNAL'], obj['TRDATE'].strftime('%Y%m%d')))

            self.db.commit()

        return "Finish One Session"

    #update stock status
    def upstkstatus(self):
        sql1 = '''select t.stkcode
                  ,CASE WHEN t1.trdate=to_date('%s','yyyy-mm-dd') THEN status
            			      ELSE DECODE(status,1,2,-1,-2) END status
            from bs_t1 t , (SELECT stkcode,MAX(trdate) trdate FROM bs_t1 GROUP BY stkcode) t1
            WHERE t.stkcode=t1.stkcode AND t.trdate=t1.trdate'''
        di = dataint.DataInt()
        nowtime = di.getCurrentTime()
        d = pd.read_sql(sql1 % (nowtime), self.db)

        for i in xrange(len(d)):
            sql1 = '''update stk_dig_t set status=%s, stdate=sysdate where stkcode='%s' '''
            self.cur.execute(sql1 % (d['STATUS'].iloc[i], d['STKCODE'].iloc[i]))
            self.db.commit()

    #strip list,return a dictory

    def listsel(self,stocklist):
        leng = len(stocklist)
        stip = 100
        times = int(leng / stip) + 1
        rsdic={}
        for i in xrange(0, times):
            rsdic[i]= stocklist[i * stip:(i + 1) * stip]
        return rsdic

    #启动函数
    def go(self):

        t = time.time()
        mode = config.get('ga', 'compmode')
        process = config.get('concurrent','processor')
        # exceute fuquan
        #复权
        e1 = ex()
        e1.EX_ALL()
        print "------------买卖点运算启动-----------"
        #如模式为9，就是重新标注买卖点
        if int(mode) == 9:
            sql = '''delete from bs_t1'''
            self.cur.execute(sql)
            self.db.commit()
        #get the stocklist

        y = [0, '20000101']
        y = self.cur.callproc('sync_stk_td_p', y)
        # if y[0] < 1 : return
        #获取股票交易代码
        sql = '''select distinct stkcode from mv_stk_td where trdate=to_date('%s','yyyymmdd')'''
        socklist = pd.read_sql(sql % (y[1],), self.db)
        # get the stocklist
        #将股票代码列表重新分段，传给多进程处理
        rslist = self.listsel(socklist)

        pool = multiprocessing.Pool(processes=int(process))
        
        for i in xrange(len(rslist)):
            pool.apply_async(dowork1, (rslist.get(i),mode,i))
        pool.close()
        pool.join()

        print "Sub-process(es) done."
        #update stock status
        #更新股票持有或者卖出标记
        self.upstkstatus()
        # sycn mongodb
        e = etl()
        e.setGGSJ(0)
        e.setDIG()

        print('time cost', time.time() - t)


if __name__ == "__main__":

    dp = DataProcUnit()
    dp.go()

