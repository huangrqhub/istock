# -*- coding: utf-8 -*- 
from __future__ import division
import pandas as pd
import cx_Oracle
import talib as ta
import numpy as np
import tushare as ts
import sys,os
from pymongo import MongoClient
from bson.dbref import DBRef

reload(sys)
sys.setdefaultencoding( "utf8" )
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

db=cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
cur = db.cursor()

client = MongoClient('mongodb://mcdb:mc969@192.168.0.32:27017/mcdb')
mdb = client.mcdb

def get_Index():
    # 获取指数清单
    df = ts.get_index()
    cur.execute('truncate table idx_t')
    for v in df.values:
        if v[0][0:2]=='00' and v[0]<>'000300':
            t = (v[0],v[1],'1A'+v[0][2:6])
        else:
            t = (v[0],v[1],v[0])
        cur.execute('''insert into idx_t values('%s','%s','%s',0,0,sysdate,0)''' % t)
    
    cur.execute('''update idx_t set isused=1 where idxcode in ('000001','000300','399001','399005','399006')''')
    db.commit()

def get_h_Data():
    # 初始化数据
    idx = cur.execute('select idxcode,stkcode from idx_t where isused=1').fetchall()
    sql = '''insert into idx_td_t values('%s',to_date('%s','yyyy-mm-dd'),%s,%s,%s,%s,%s,%s,%s)'''
    for i in idx:
        df = ts.get_h_data(i[0],index=True)
        df['p']=df['close'].shift(-1)
        df['ch']=df['close']/df['p']-1
        df.reset_index(inplace=True)
        df = df.dropna()
        for v in df.values:
            cur.execute(sql % (i[1],v[0].strftime('%Y-%m-%d'),v[1],v[2],v[3],v[4],v[5],v[6],v[8]))
        db.commit()

def get_Data():
    # 每日更新数据
    df = ts.get_index()
    sql = '''insert into idx_td_t values('%s',trunc(sysdate),%s,%s,%s,%s,%s,%s,%s)'''
    for v in df.values:
        if v[0][0:2]=='00' and v[0]<>'000300':
            idxcode = idxcode='1A' + v[0][2:6]
        else:
            idxcode = v[0]
        cur.execute(sql % (idxcode,v[3],v[6],v[5],v[7],v[8],v[9],v[2]/100))
    db.commit()

def stats(data):
    cnt = data.count()['ma1'] # 交易日期数量
    data.loc[data[(data['b']==1) & (data['b'].shift(1)==0)].index,'bs'] = 1 # buy
    data.loc[data[(data['b']==0) & (data['b'].shift(1)==1)].index,'bs'] = -1 # sale    
    dt = data.dropna().copy()
    dt['ma1'] = dt['CL'].shift(-1)
    dt = dt.loc[dt[dt['bs']==1].index].dropna()
    if len(dt)==0: return [0,0,0,0]
    trcnt = dt.count()['bs'] # 交易次数
    scnt = dt.loc[dt[dt['CL']<dt['ma1']].index].count()['bs'] # 成功次数
    e = (dt['ma1']/dt['CL']*0.998).cumprod().values[-1] # 收益
    return [e*(1-2*trcnt/cnt)*(scnt/trcnt),cnt,trcnt,scnt,e]

def tuning(stk):
    sql = '''
        select trdate,v_close cl,trunc(trdate,'d')+7 ww 
          from idx_td_t where stkcode='%s' and trdate>(sysdate-365*1.5) order by 1
    '''
    dd = pd.read_sql(sql % stk,db,index_col='TRDATE')
    dw = dd.resample('W',how='last').dropna()
    x = [0,0,0,0,0]
    for i in range(20,30):
        dd['ma1']=ta.KAMA(np.array(dd['CL'],dtype='f8'),timeperiod=i)
        dd['b']= 0
        dd.loc[dd[dd['CL']>dd['ma1']].index,'b']=1
        y = stats(dd)
        if y[0]>x[0]: x = y + [i]
    z = [0,0,0,0,0]
    for i in range(15,30):
        dw['ma1']=ta.KAMA(np.array(dw['CL'],dtype='f8'),timeperiod=i)
        dw['b']= 0
        dw.loc[dw[dw['CL']>dw['ma1']].index,'b']=1
        y = stats(dw)
        if y[0]>z[0]: z = y + [i]
    return x + z

def tuning_IDX():
    ds, = cur.execute('''select sysdate-nvl(max(tdate),to_date('20150101','yyyymmdd')) ds from MV_TU_IDX t''').fetchone()
    if ds<31: return
    cur.execute('truncate table ma_tu_idx_t')
    stks = cur.execute('select stkcode from idx_t where isused=1').fetchall()
    sql = '''insert into ma_tu_idx_t values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',sysdate)'''
    for s in stks:
        z = tuning(s)
        cur.execute(sql % tuple(z + [s[0]]))
    db.commit()
    cur.callproc('dbms_mview.refresh',['mv_tu_idx','c'])

def bs(stk,opt):
    sql = '''
          select trdate,v_close cl,trunc(trdate,'d')+7 ww from idx_td_t where stkcode='%s' order by 1
        '''
    dd = pd.read_sql(sql % stk,db,index_col='TRDATE')
    dw = dd.resample('W',how='last').dropna()    
    ma = cur.execute('''select d,w from mv_tu_idx where stkcode='%s' ''' % stk).fetchone()
    dd['ma1']=ta.KAMA(np.array(dd['CL'],dtype='f8'),timeperiod=ma[0])
    dw['ma3']=ta.KAMA(np.array(dw['CL'],dtype='f8'),timeperiod=ma[1])
    data = pd.merge(dd,dw,on='WW')
    data.columns = ['CL','WW','ma1','CLW','ma3']    
    data.loc[data[data['CL']>=data['ma1']].index,'b'] = 1
    data.loc[data[data['CL']<data['ma1']].index,'b'] = 0
    data.loc[data[data['CLW']>=data['ma3']].index,'bw'] = 1
    data.loc[data[data['CLW']<data['ma3']].index,'bw'] = 0  
    # 标记买卖点
    data['b1'] = data['b'].shift(1)
    data.loc[data[(data['b']==1) & (data['b1']==0)].index,'bs'] = 1 # buy
    data.loc[data[(data['b']==1) & (data['b1']==1)].index,'bs'] = 2 # 持股
    data.loc[data[(data['b']==0) & (data['b1']==1)].index,'bs'] = -1 # sale
    data.loc[data[(data['b']==0) & (data['b1']==0)].index,'bs'] = -2 # 持币
    data['bw1'] = data['bw'].shift(1)
    data.loc[data[(data['bw']==1) & (data['bw1']==0)].index,'bsw'] = 1 # buy
    data.loc[data[(data['bw']==1) & (data['bw1']==1)].index,'bsw'] = 2 # 持股
    data.loc[data[(data['bw']==0) & (data['bw1']==1)].index,'bsw'] = -1 # sale
    data.loc[data[(data['bw']==0) & (data['bw1']==0)].index,'bsw'] = -2 # 持币
    data = data.fillna(0)    
    dd.reset_index(inplace=True)
    data['TRDATE']=dd['TRDATE']
    sql = '''insert into bs_idx_t values ('%s',%s,to_date('%s','yyyymmdd'))'''
    if opt==9:                
        for v in data.values:
            if v[8] in (1,-1): cur.execute(sql % (stk[0],v[8],v[11].strftime('%Y%m%d')))
    else:
        v = data.values[-1]
        if v[8] in (1,-1): cur.execute(sql % (stk[0],v[8],v[11].strftime('%Y%m%d')))    
    cur.execute('''update idx_t set status=%s, stdate=sysdate, sw=%s where stkcode='%s' ''' % (v[8],v[10],stk[0]))
    db.commit()

def bs_IDX(opt):
    stks = cur.execute('select stkcode from idx_t where isused=1').fetchall()
    for s in stks:
        bs(s,opt)

def set_Info(): 
    sql = '''select stkcode,idxname,status,sw,to_char(stdate,'mm/dd') sdate,to_char(stdate,'mm/dd hh24:mi') ldate from idx_t where isused=1'''   
    stks = cur.execute(sql).fetchall()
    info = {1 : '出现变盘机会', 2 : '处于多头行情', -1 : '出现变盘风险', -2 : '处于空头行情', 0 : '行情未知'}    
    for r in stks:
        c = mdb.stkdig # 摘要
        l = {}
        l['_id'] = r[0]
        l['code'] = r[0]
        l['name'] = r[1]
        l['content'] ='#指数状态# ' + r[5] + '\n长线' + info[r[3]] + ', 短线' + info[r[2]]
        l['cdate'] = r[5]  # mm/dd hh24:mi
        l['status'] = r[2]
        l['stdate'] = r[5]
        l['spel'] = r[0]
        c.save(l)        

        c = mdb.ml # 列表
        l = {}
        l['stk'] = DBRef('stkdig',r[0])
        l['sudate'] = r[4]
        l['mxid'] = 'mxdpzs'
        l['profit'] = 0
        l['_id'] = r[0]
        c.save(l)

def isOpen():
    df = ts.get_hist_data('sh')
    td, = cur.execute('''select to_char(sysdate,'yyyy-mm-dd') from dual''').fetchone()
    if df.index[0] <> td: return

    get_Data()
    bs_IDX(0)
    set_Info()
    set_Data()
    set_MX()

def set_Data():
    c = mdb.ggsj # 日线
    sql1 = '''
      select to_char(trdate,'yyyymmdd') trdate,v_open,v_close,v_high,v_low,v_volume,ch
        from idx_td_t
        where stkcode='%s' and trdate>sysdate-300
        order by 1 desc
        '''
    sql2 = '''select status,to_char(trdate,'yyyymmdd') trdate from bs_idx_t where stkcode='%s' and trdate>sysdate-300 '''
    rows = cur.execute('select stkcode,idxname from idx_t where isused=1').fetchall()
    for rc in rows:
        bs = {}
        rows_bs = cur.execute(sql2 % (rc[0],)).fetchall()
        rows_d = cur.execute(sql1 % (rc[0],)).fetchall()
        datas = []
        for rd in rows_d:
            l = {}
            l['open']=round(rd[1],2)
            l['close'] = round(rd[2],2)
            l['high'] = round(rd[3],2)
            l['low'] = round(rd[4],2)
            l['vol'] = round(rd[5],2)
            l['wrange'] = round(rd[6]*100,2)
            l['date']=rd[0]
            l['b'] = 0
            l['s'] = 0
            if (1,rd[0]) in rows_bs: l['b'] = 1
            if (-1,rd[0]) in rows_bs: l['s'] = 1
            datas.append(l)
        c.save({'_id':rc[0],'code':rc[0], 'name': rc[1], 'datas':datas})

def profit(stk):
    sql = '''
      select t.trdate,t.v_close cl,b.status
        from idx_td_t t,bs_idx_t b
        where t.stkcode=b.stkcode and t.trdate=b.trdate and t.stkcode='%s' and t.trdate>sysdate-90
        union
        select trdate,v_close cl,-1
        from idx_td_t
        where stkcode='%s' and trdate=(select max(trdate) from idx_td_t where stkcode='%s')
        order by 1
        '''
    data = pd.read_sql(sql % (stk,stk,stk),db)
    data['p']=data['CL'].shift(-1)
    data = data.loc[data[data['STATUS']==1].index].dropna()
    if len(data)==0: return [0,0]
    trcnt = data.count()['STATUS'] # 交易次数
    scnt = data.loc[data[data['CL']<data['p']].index].count()['STATUS'] # 成功次数
    e = (data['p']/data['CL']*0.998).cumprod().values[-1] -1 # 收益
    return [e,scnt/trcnt]

def profit_IDX():
    stks = cur.execute('select stkcode from idx_t where isused=1').fetchall()
    y = [0,0,0]
    for s in stks:
        x = profit(s[0])
        y[0] += x[0]
        y[1] += x[1]
        y[2] += 1
    return (round(y[0]/y[2]*100,2),round(y[1]/y[2]*100,2))

def set_MX():
    e = profit_IDX()
    cur.execute('''update mx_t set last_time=sysdate,profit=%s,succ=%s where code='mxdpzs' ''' % e)
    db.commit()
    from etl4 import ETL
    e = ETL()
    e.setMX()

if __name__ == '__main__':
    p = sys.argv[1]
    if p=='0':
        isOpen()
    elif p=='1':
        tuning_IDX()
    elif p=='9':
        get_Index()
        get_h_Data()
        tuning_IDX()
        bs_IDX(9)
        set_Info()
        set_Data()
    else:
        pass

