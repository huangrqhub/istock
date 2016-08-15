# -*- coding: utf-8 -*- 
from __future__ import division
import pandas as pd
import cx_Oracle
import talib as ta
import numpy as np
import os
from etl4 import ETL as etl
from ex import EX as ex

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class MATuning:
    def __init__(self):
        self.db = cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
        self.cur = self.db.cursor()
        self.MAs = {'dema':ta.DEMA, 'ema':ta.EMA, 'kama':ta.KAMA, 'sma':ta.SMA, 'tema':ta.TEMA, 'trima':ta.TRIMA, 'wma':ta.WMA}

    def profit_MX(self,sqlstks,sqldata,lt):
        # 收益计算
        stks = self.cur.execute(sqlstks).fetchall()

        y = [0, 0, 0, 0]
        for s in stks:    
            data = pd.read_sql(sqldata % (s[0], lt, lt, s[0], lt) ,self.db)
            data['e']=data['EXB'].shift(-1)/data['EXB']
            cnt_succ = data.loc[data[(data['STATUS']==1) & (data['e']>1)].index,'e'].count()
            cnt = data.loc[data[data['STATUS']==1].index]['e'].dropna().count()
            if cnt>0: 
                e = data.loc[data['STATUS']==1,'e'].cumprod().dropna().values[-1]-1
                y[0] += cnt
                y[1] += cnt_succ
                y[2] += e
                y[3] += 1
        if y[0]==0 or y[3]==0:
            return [0,0]
        else:
            return (round(y[1]/y[0]*100,2), round(y[2]/y[3]*100,2))

    def profit_DAY(self,sqlstks,lasttime):
        # 今日机会收益及胜率计算      
        sql = '''
          select * from (
            select t.stkcode,t.exb,b.status,t.trdate
              from mv_stk_td t, bs_t1 b
              where t.stkcode=b.stkcode and t.trdate=b.trdate and t.stkcode='%s' 
                and t.trdate>=(to_date('%s','yyyymmdd')-31) and t.trdate<=to_date('%s','yyyymmdd')
            union all
            select t.stkcode,t.exb,-1,t.trdate
              from mv_stk_td t 
              where t.stkcode='%s'
                and trdate=to_date('%s','yyyymmdd')) 
          order by trdate
            '''
 
        x = self.profit_MX(sqlstks,sql,lasttime)
        return x

    def profit_GOOD(self,lasttime):
        # 今日优选收益及胜率计算      
        sql = '''
          select * from (
            select t.stkcode,t.exb,b.status,t.trdate
              from mv_stk_td t, bs_t1 b
              where t.stkcode=b.stkcode and t.trdate=b.trdate and t.stkcode='%s' 
                and t.trdate>=(to_date('%s','yyyymmdd')-31) and t.trdate<=to_date('%s','yyyymmdd')
            union all
            select t.stkcode,t.exb,-1,t.trdate
              from mv_stk_td t 
              where t.stkcode='%s'
                and trdate=to_date('%s','yyyymmdd')) 
          order by trdate
            '''
 
        x = self.profit_MX('select stkcode from stk_good_t',sql,lasttime)
        sql = '''
          update mx_t set succ=%s,profit=%s,last_time=sysdate where code='mxjryx'
            '''
        self.cur.execute(sql % x)
        self.db.commit()

    def ins_STKS(self,opt):
        sql = '''select to_char(max(trdate),'yyyymmdd'),to_char(sysdate,'yyyymmdd') from stk_mkt_ex_t'''
        y = self.cur.execute(sql).fetchone()
        if y[0]<>y[1]: return

        sql = '''
          select stkcode,to_char(trdate,'mm/dd') trdate,0 pro,'1d'||seq_ml.nextval id
            from bs_t1 
            where trdate=to_date('%s','yyyymmdd') and status=1 and stkcode in (select stkcode from stk_good_t)
            '''
        rows = self.cur.execute(sql % (y[1],)).fetchall()
        # rows = self.cur.execute(sql % ('20160711',)).fetchall()

        e = etl()
        e.setML('mxjryx',rows,9) # 更新股票列表
        self.profit_GOOD(y[1])         
        e.setMX()
    
    def comp(self):
        sql = '''select to_char(sysdate-%s,'yyyymmdd'),to_char(sysdate-%s,'D') from dual'''
        for i in range(0,365):
            now,d = self.cur.execute(sql % (i,i)).fetchone()
            if d in ('1','7'): continue
            d = self.profit_DAY('select stkcode from vw_stk_code',now)
            g = self.profit_DAY('select stkcode from stk_list_t',now)
            n = self.profit_DAY('select stkcode from stk_good_t',now)
            print now,g,d,n

m = MATuning()
m.ins_STKS(0)
