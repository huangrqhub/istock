# -*- coding: utf-8 -*- 
from __future__ import division
import pandas as pd
import cx_Oracle
import numpy as np
import sys,os

reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

db=cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')

def load_from_db(stk,stktype=0):
    if stktype==0: # 个股
        sql = '''
            select trdate,exb cl from mv_stk_td where stkcode='%s' and trdate>to_date('20150101','yyyymmdd') order by 1
            '''
    else: # 指数
        sql = '''
            select trdate,v_close cl from idx_td_t where stkcode='%s' order by 1
            '''
    data = pd.read_sql(sql % (stk,), db, index_col='TRDATE')
    return data

def get_stock_list():
    sql = '''
        select stkcode from mv_stk_td where trdate=trunc(sysdate)
        minus
        select stkcode from stk_cb_t
        '''
    cur = db.cursor()
    return cur.execute(sql).fetchall()

def load_from_web(stk,db,stktype=0):
    pass

def set_signal(data,opt=0):
    if 'bs' in data.columns: del data['bs']
    data.loc[data[(data.ls>data.ll) & (data.ls.shift()<data.ll.shift())].index,'bs'] = 1 # buy
    data.loc[data[(data.ls<data.ll) & (data.ls.shift()>data.ll.shift())].index,'bs'] = -1 # sale    
    if opt==1:
        data.loc[data[(data.ls>data.ll) & (data.ls.shift()>data.ll.shift())].index,'bs'] = 2 # cover
        data.loc[data[(data.ls<data.ll) & (data.ls.shift()<data.ll.shift())].index,'bs'] = -2 # short
    return data

def get_stat(data,libor=0):
    # 数据统计
    data['bs'] = data['bs'].shift()
    data['bs'][-1] = -1
    cnt = data.count()['ll']
    dt = data.dropna().copy()
    if len(dt)==0: return [0,0,0,0,0]

    dt.reset_index(inplace=True)
    dt['e'] = dt.CL/dt.CL.shift()
    dt['d'] = dt.TRDATE-dt.TRDATE.shift()    
    dt.set_index('TRDATE',inplace=True)
    dt = dt.loc[dt[dt['bs']==-1].index].dropna()    
    if len(dt)==0: return [0,0,0,0,0]

    e = dt['e'].cumprod()[-1] 
    trcnt = dt['e'].count()
    scnt = dt.loc[dt[dt['e']>1].index,'e'].count()
    md = int(str(dt.median()['d']).split(' ')[0])
    sharp = ((dt['e']-1).mean() - libor)/dt['e'].std()
    return [round(e-1,4),round(trcnt/cnt,4),round(scnt/trcnt,4),md,round(sharp,4)]

def get_grade(res):
    return res[0]*res[-1] # 收益与sharp乘积

