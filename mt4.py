# -*- coding: utf-8 -*- 
from __future__ import division
import pandas as pd
import cx_Oracle
import talib as ta
import numpy as np
import os
# from etl4 import ETL as etl
# from ex import EX as ex
# from multiprocessing import Pool

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

db = cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
cur = db.cursor()
# MAs = {'dema':ta.DEMA, 'ema':ta.EMA, 'kama':ta.KAMA, 'sma':ta.SMA, 'tema':ta.TEMA, 'trima':ta.TRIMA, 'wma':ta.WMA}
MAs = {'sma':ta.SMA}

# def tuning_ALL():
#    # 优化日线所有股票    
#    stks = cur.execute('select stkcode from vw_stk_code where rownum<10').fetchall()
#    print len(stks)
#    pool = Pool(processes=3)    # set the processes max number 3
#    for s in stks:
#        result = pool.apply_async(get_tuning, s)
#    pool.close()
#    pool.join()
#    if result.successful():
#        print 'successful'


def get_stat(data):
    data.loc[data['b']>data['b'].shift(),'deal']=1
    data.loc[data['b']<data['b'].shift(),'deal']=0
    data['deal'] = data['deal'].shift()
    # data['deal'].iloc[(data['deal'].count()-1)] = 0
    data['deal'].values[-1] = 0
    cnt=len(data)
    dt = data.dropna().copy()
    if len(dt)==0: return [0,0,0,0]
    dt['e'] = dt.OP/dt.OP.shift()*0.997
    trcnt = dt['e'].count()

    es = dt.loc[dt['deal']==0]['e'].cumprod()
    e=es.iloc[len(es)-1]
    return [e*(1-trcnt/cnt), e, trcnt, cnt]

def get_tuning(stk):
    # 计算最优均线周期及计算方式
    # sql = '''select trdate,exb cl,op*(exb/cl) op,ch from mv_stk_td where stkcode='%s' order by 1'''
    sql = '''
        select trdate,v_open*(adj_price/v_close) op,v_high*(adj_price/v_close) hi,v_low*(adj_price/v_close) lo,adj_price cl,v_change ch
          from stk_mkt_ex_t where stkcode='%s' order by trdate
        '''
    data = pd.read_sql(sql % (stk,),db)
    print stk,len(data)
    if len(data)<40: return 1

    x = [-100,0,0,0] + ['x',0,0,0]
    for k,v in MAs.items():
        # single MA
        #for i in range(10,30):
        #    data['ma1'] = v(np.array(data['CL']),timeperiod=i)
        #    data.loc[data[data['CL']>data['ma1']].index,'b']=1 # buy 
        #    data.loc[data[data['CL']<=data['ma1']].index,'b']=0 # sale
        #    e = get_stat(data)
        #    if e[0] > x[0]:
        #        x=e + [k,i,0,1]
        # two MAs
        for i in range(14,40):
            for j in range(7,14):
                if j>=(i-1): continue
                data['ma1']=v(np.array(data['CL']),timeperiod=j)
                data['ma2']=v(np.array(data['CL']),timeperiod=i)
                data.loc[data[data['ma1']>data['ma2']].index,'b']=1 # buy
                data.loc[data[data['ma1']<=data['ma2']].index,'b']=0 # sale   
                e = get_stat(data)
                if e[0] > x[0]:
                    x=e + [k,j,i,2]
    x += [stk]
    print x
    cur.execute(''' insert into ma_tu_t2 values(%s,%s,%s,%s,'%s',%s,%s,%s,'%s')''' % tuple(x))
    db.commit()
    return 0

def get_stks():
    stks = cur.execute('select stkcode from vw_stk_code').fetchall()
    for s in stks:
        get_tuning(s[0])

# tuning_ALL()
# tuning('300013')
get_stks()
