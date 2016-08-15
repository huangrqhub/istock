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

    def bs_1HOUR(self,opt):
        # 标记日线买卖点
        y = [0,'200001010101']
        y = self.cur.callproc('sync_stk_td_1h_p',y)
        if y[0]<1 and opt<>9: return

        if opt==9: 
            self.cur.execute('truncate table bs_1h_t')
            sql = 'select stkcode from vw_stk_code'
            stks = self.cur.execute(sql).fetchall()
            y[1], = self.cur.execute('''select to_char(max(trdate),'yyyymmddhh24mi') trdate from mv_stk_td_1h''').fetchone()
        else:
            if y[0] == 0: return            
            sql = '''select distinct stkcode from mv_stk_td_1h where trdate=to_date('%s','yyyymmddhh24mi')'''
            stks = self.cur.execute(sql % (y[1],)).fetchall()

        if not stks: return

        for s in stks:
            x = self.cur.execute('''select ma,d1,d2,t from mv_ma_tu_1h where stkcode='%s' ''' % s).fetchone()
            if not x: continue
            if x[3]==0:
                x = ['kama',10,0,1]

            sql = '''
              select trdate,cl,ch from mv_stk_td_1h where stkcode='%s' order by 1
                '''
            d = pd.read_sql(sql % s, self.db)
            if len(d)<x[1]: continue
            d = self.bs(d,x)            

            sql = '''insert into bs_1h_t values ('%s',%s,to_date('%s','yyyymmddhh24mi'))'''
            if opt==9:                
                for v in d.values:
                    if v[-1] in (1,-1): self.cur.execute(sql % (s[0],v[-1],v[0].strftime('%Y%m%d%H%M')))
            else:
                v = d.values[-1]
                if v[-1] in (1,-1): self.cur.execute(sql % (s[0],v[-1],v[0].strftime('%Y%m%d%H%M')))

            sql = '''
              update stk_dig_t 
                set content='#小时线BS点# '||'%s'||
                      case %s when 2 then ' 持股' when 1 then ' 买入' when -1 then ' 卖出' when -2 then ' 持币' else '状态不明' end
                  , cdate=sysdate 
                where stkcode='%s'
                '''
            self.cur.execute(sql % (v[0].strftime('%m/%d %H:%M'), v[-1], s[0]))
            self.db.commit()
        
        # 更新信息
        e = etl()
        e.setGGSJ(1) # 更新日线数据
        e.setDIG() #更新摘要信息
        sql = '''
          select stkcode,to_char(trdate,'mm/dd hh24:mi') trdate,0 pro,'1h'||seq_ml.nextval id
            from bs_1h_t 
            where trdate=to_date('%s','yyyymmddhh24mi') and status=1
            '''
        rows = self.cur.execute(sql % (y[1],)).fetchall()
        e.setML('mxjrjh1h',rows,9) # 更新股票列表
        self.profit_1HOUR(y[1])
        e.setMX() # 更新模型统计数据
        e.setConf('data1h',0)
        e.setConf('bs1h',0)

    def bs_DAY(self,opt):
        # 标记日线买卖点
        e1 = ex()
        e1.EX_ALL()

        y = [0,'20000101']
        y = self.cur.callproc('sync_stk_td_p',y)
        if y[0]<1 and opt<>9: return

        if opt==9: 
            self.cur.execute('truncate table bs_t')
            sql = 'select stkcode from vw_stk_code'
            stks = self.cur.execute(sql).fetchall()
            y[1], = self.cur.execute('''select to_char(max(trdate),'yyyymmdd') trdate from mv_stk_td''').fetchone()
        else:
            if y[0] == 0: return            
            sql = '''select distinct stkcode from mv_stk_td where trdate=to_date('%s','yyyymmdd')'''
            stks = self.cur.execute(sql % (y[1],)).fetchall()

        if not stks: return

        for s in stks:
            x = self.cur.execute('''select ma,d1,d2,t from mv_ma_tu where stkcode='%s' ''' % s).fetchone()
            if not x: continue
            if x[3]==0:
                x = ['kama',10,0,1]

            sql = '''
              select trdate,exb cl,ch from mv_stk_td where stkcode='%s' order by 1
                '''
            d = pd.read_sql(sql % s, self.db)
            if len(d)<x[1]: continue
            d = self.bs(d,x)            

            sql = '''insert into bs_t values ('%s',%s,to_date('%s','yyyymmdd'))'''
            if opt==9:                
                for v in d.values:
                    if v[-1] in (1,-1): self.cur.execute(sql % (s[0],v[-1],v[0].strftime('%Y%m%d')))
            else:
                v = d.values[-1]
                if v[-1] in (1,-1): self.cur.execute(sql % (s[0],v[-1],v[0].strftime('%Y%m%d')))
            self.cur.execute('''update stk_dig_t set status=%s, stdate=sysdate where stkcode='%s' ''' % (v[-1], s[0]))
            self.db.commit()
        
        # 更新信息
        e = etl()
        e.setGGSJ(0) # 更新日线数据
        e.setDIG() #更新摘要信息
        sql = '''
          select stkcode,to_char(trdate,'mm/dd') trdate,0 pro,'1d'||seq_ml.nextval id
            from bs_t 
            where trdate=to_date('%s','yyyymmdd') and status=1
            '''
        rows = self.cur.execute(sql % (y[1],)).fetchall()
        e.setML('mxjrjh',rows,9) # 更新股票列表
        self.profit_DAY(y[1])
        e.setMX() # 更新模型统计数据
        e.setConf('bs1d',0)
        
    def tuning_1HOUR(self):
        # 优化1小时线所有股票
        e = etl()
        if e.getConf('ma1h',1)<>1: return

        # self.cur.execute('dbms_mview.refresh',['mv_stk_td_1h','c'])
        self.cur.execute('truncate table ma_tu_1h_t') # 清除上次优化结果

        sql = '''select trdate,exb cl,ch from mv_stk_td_1h where stkcode='%s' order by 1'''
        stks = self.cur.execute('select stkcode from vw_stk_code').fetchall()
        for s in stks:
             d = pd.read_sql(sql % s,self.db)
             if len(d)<40: continue
             rst = self.tuning(d) # 计算最优均线及周期
             if rst:
                 rst.append(s[0])
                 self.cur.execute(''' insert into ma_tu_1h_t values('%s',%s,%s,%s,%s,'%s')''' % tuple(rst))
                 self.db.commit()

        self.cur.callproc('dbms_mview.refresh',['mv_ma_tu_1h','c'])        
        e.setConf('ma1h',30)

    def tuning_DAY(self):
        # 优化日线所有股票
        e = etl()
        if e.getConf('ma1d',1)<>1: return

        # self.cur.execute('dbms_mview.refresh',['mv_stk_td','c'])
        self.cur.execute('truncate table ma_tu_t') # 清除上次优化结果

        sql = '''select trdate,exb cl,ch from mv_stk_td where stkcode='%s' order by 1'''
        stks = self.cur.execute('select stkcode from vw_stk_code').fetchall()
        for s in stks:
             d = pd.read_sql(sql % s,self.db)
             if len(d)<40: continue
             rst = self.tuning(d) # 计算最优均线及周期
             if rst:
                 rst.append(s[0])
                 self.cur.execute(''' insert into ma_tu_t values('%s',%s,%s,%s,%s,'%s')''' % tuple(rst))
                 self.db.commit()

        self.cur.callproc('dbms_mview.refresh',['mv_ma_tu','c'])
        e.setConf('ma1d',90)
        self.cur.callproc('mm_p')

    def profit(self,data):
        # 计算收益
        data['b'] = data['b'].shift(1) 
        data.loc[data[(data['b']<>data['b'].shift(1))].index,'bs'] = 1
        # if data['bs'].count()>data['CL'].count()*0.25:
        #     return 0
        data['e'] = (data['CH']*data['b']/100 + 1).cumprod()
        return round(data.iloc[len(data)-1]['e'],2)

    def tuning(self,data):
        # 计算最优均线周期及计算方式
        x = ['x',0,0,0,0]
        for k,v in self.MAs.items():
            # single MA
            for i in range(10,30):
                data['ma1'] = v(np.array(data['CL']),timeperiod=i)
                data.loc[data[data['CL']>data['ma1']].index,'b']=1 # buy 
                data.loc[data[data['CL']<=data['ma1']].index,'b']=0 # sale
                e = self.profit(data)
                if e>x[1]:
                    x=[k,e,i,0,1]
            # two MAs
            for i in range(15,30):
                for j in range(10,25):
                    if j>=(i-1): continue
                    data['ma1']=v(np.array(data['CL']),timeperiod=j)
                    data['ma2']=v(np.array(data['CL']),timeperiod=i)
                    data.loc[data[data['ma1']>data['ma2']].index,'b']=1 # buy
                    data.loc[data[data['ma1']<=data['ma2']].index,'b']=0 # sale   
                    e = self.profit(data)
                    if e>x[1]:
                        x=[k,e,j,i,2]
        return x

    def bs(self,data,ma):
        # 买卖点计算
        if ma[3] == 1:
            data['ma1'] = self.MAs[ma[0]](np.array(data['CL'],dtype='f8'), timeperiod=ma[1])
            data.loc[data[data['CL']>data['ma1']].index,'b']=1 # buy 
            data.loc[data[data['CL']<data['ma1']].index,'b']=0 # sale
        else:
            data['ma1'] = self.MAs[ma[0]](np.array(data['CL'],dtype='f8'), timeperiod=ma[1])
            data['ma2'] = self.MAs[ma[0]](np.array(data['CL'],dtype='f8'), timeperiod=ma[2])
            data.loc[data[data['ma1']>data['ma2']].index,'b']=1 # buy 
            data.loc[data[data['ma1']<data['ma2']].index,'b']=0 # sale
        
        # 标记买卖点
        data['b1'] = data['b'].shift(1)
        data.loc[data[(data['b']==1) & (data['b1']==0)].index,'bs'] = 1 # buy
        data.loc[data[(data['b']==1) & (data['b1']==1)].index,'bs'] = 2 # 持股
        data.loc[data[(data['b']==0) & (data['b1']==1)].index,'bs'] = -1 # sale
        data.loc[data[(data['b']==0) & (data['b1']==0)].index,'bs'] = -2 # 持币
        data = data.fillna(0)

        return data

    def profit_MX(self,sql,lt):
        # 收益计算
        stks = self.cur.execute('select stkcode from vw_stk_code').fetchall()

        y = [0, 0, 0, 0]
        for s in stks:    
            data = pd.read_sql(sql % (s[0], lt, s[0], lt) ,self.db)
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

    def profit_DAY(self,lasttime):
        # 今日机会收益及胜率计算      
        sql = '''
          select * from (
            select t.stkcode,t.exb,b.status,t.trdate
              from mv_stk_td t, bs_t b
              where t.stkcode=b.stkcode and t.trdate=b.trdate and t.stkcode='%s' 
                and t.trdate>(to_date('%s','yyyymmdd')-31)
            union all
            select t.stkcode,t.exb,-1,t.trdate
              from mv_stk_td t 
              where t.stkcode='%s'
                and trdate=to_date('%s','yyyymmdd')) 
          order by trdate
            '''
 
        x = self.profit_MX(sql,lasttime)
        sql = '''
          update mx_t set succ=%s,profit=%s,last_time=sysdate where code='mxjrjh'
            '''
        self.cur.execute(sql % x)
        self.db.commit()

    def profit_1HOUR(self,lasttime):
        # 短线机会收益及胜率计算
        sql = '''
          select * from (
            select t.stkcode,t.exb,b.status,t.trdate
              from mv_stk_td_1h t, bs_1h_t b
              where t.stkcode=b.stkcode and t.trdate=b.trdate and t.stkcode='%s' 
                and t.trdate>(to_date('%s','yyyymmddhh24mi')-7)
            union all
            select t.stkcode,t.exb,-1,t.trdate
              from mv_stk_td_1h t 
              where t.stkcode='%s'
                and trdate=to_date('%s','yyyymmddhh24mi')) 
          order by trdate
            '''
 
        x = self.profit_MX(sql,lasttime)

        sql = '''
          update mx_t set succ=%s,profit=%s,last_time=sysdate where code='mxjrjh1h'
            '''
        self.cur.execute(sql % x)
        self.db.commit()
