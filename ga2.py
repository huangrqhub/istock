# -*- coding: utf-8 -*-
from __future__ import division
import math
import numpy as np
import pandas as pd
import random as rand
import talib as ta
import cx_Oracle
import tushare as ts
import copy
# import time
from datetime import datetime
import scipy.stats.mstats as mstats
from pymongo import MongoClient

def get_r(data):
    x = [8,13,21,34,55]
    cl = np.array(data['CL'],dtype='f8')
    for i in x:
        data['m'+str(i)] = ta.EMA(cl,timeperiod=i)
    for i,r in data.iterrows():
        if math.isnan(r['m55']): continue
        z = [[r['m8'],8],[r['m13'],13],[r['m21'],21],[r['m34'],34],[r['m55'],55]]
        c,p = mstats.spearmanr(x,[k[1] for k in sorted(z,reverse=True)])
        data.loc[i,'r'] = c
    return data['r']

#全局变量：
PSIZE=30#种群数量
TERMINAL=20#迭代代数
PC=0.95 #交叉概率
PM=0.2 #变异概率


#惩罚函数：
def get_stat(data):
    data['deal'] = data['deal'].shift()
    # data['deal'].iloc[(data['deal'].count()-1)] = 0
    data['deal'].values[-1] = 0
    cnt=len(data)
    dt = data.dropna().copy()
    if len(dt)==0: return [0,0,0,0]
    dt['e'] = dt.OP/dt.OP.shift()*0.997
    trcnt = dt['e'].count()
    if trcnt==0: return [0,0,0,0]
  
    es = dt.loc[dt['deal']==0]['e'].cumprod()
    e=es.iloc[len(es)-1]
    if math.isnan(e):
       # print es
       print 'length',len(es),cnt,trcnt
 
    return [e*(1-trcnt/cnt), e, trcnt, cnt]

#双均线交叉：
def MA_Cross(price_dt,short_ma=10,long_ma=20):

    s=ta.SMA(np.array(price_dt['CL'],dtype='f8'),timeperiod=short_ma)
    l=ta.SMA(np.array(price_dt['CL'],dtype='f8'),timeperiod=long_ma)
    signal=pd.DataFrame({'s':s,'l':l})
    signal.loc[(signal['s']>signal['l']),'bs']=1
    signal.loc[(signal['s']<signal['l']),'bs']=0
    return signal['bs']

chromo_fun={'dema':ta.DEMA,'ema':ta.EMA,'kama':ta.KAMA,'midpoint':ta.MIDPOINT,'sma':ta.SMA,'tema':ta.TEMA,'trima':ta.TRIMA,'wma':ta.WMA,'macd':ta.MACD,'t3':ta.T3,'kdj':ta.STOCH,'mom':ta.MOM,'roc':ta.ROC,'rsi':ta.RSI,'trix':ta.TRIX,'ma_cross':MA_Cross,'ema_ali':get_r}

#指标计算：
def indic_cal(price_data,indic_dict):
    chromo_fun={'dema':ta.DEMA,'ema':ta.EMA,'kama':ta.KAMA,'midpoint':ta.MIDPOINT,'sma':ta.SMA,'tema':ta.TEMA,'trima':ta.TRIMA,'wma':ta.WMA,'macd':ta.MACD,'t3':ta.T3,'kdj':ta.STOCH,'mom':ta.MOM,'roc':ta.ROC,'rsi':ta.RSI,'trix':ta.TRIX,'ma_cross':MA_Cross,'ema_ali':get_r}
    indictor0=['dema','ema','kama','sma','tema','trima']#midpoint,wma被删除
    for k in indic_dict:
        if k in indictor0:
            price_data[k]=chromo_fun[k](real=np.array(price_data['CL'],dtype='f8'),timeperiod=indic_dict[k]['parameter'])
            price_data.loc[price_data[price_data['CL']>price_data[k]].index,k+'_SIGNAL']=1
            price_data.loc[price_data[price_data['CL']<price_data[k]].index,k+'_SIGNAL']=0
        if k=='ma_cross':
            price_data['ma_cross_SIGNAL']=chromo_fun['ma_cross'](price_dt=price_data,short_ma=indic_dict['ma_cross']['parameter']['ma_cross_sma'],long_ma=indic_dict['ma_cross']['parameter']['ma_cross_lma'])
        if k=='rsi':
            price_data['rsi']=chromo_fun['rsi'](real=np.array(price_data['CL'],dtype='f8'),timeperiod=indic_dict['rsi']['parameter'])
            price_data.loc[price_data[price_data['CL']>price_data['rsi']].index,'rsi_SIGNAL']=1
            price_data.loc[price_data[price_data['CL']<price_data['rsi']].index,'rsi_SIGNAL']=0
        if k=='ema_ali':
            price_data['ema_ali']=chromo_fun['ema_ali'](price_data)
            price_data.loc[price_data[0<price_data['ema_ali']].index,'ema_ali_SIGNAL']=1
            price_data.loc[price_data[0>price_data['ema_ali']].index,'ema_ali_SIGNAL']=0
        if k=='roc':
            price_data['roc']=chromo_fun['roc'](real=np.array(price_data['CL'],dtype='f8'),timeperiod=indic_dict['roc']['parameter'])
            price_data.loc[price_data[price_data['CL']>price_data['roc']].index,'roc_SIGNAL']=1
            price_data.loc[price_data[price_data['CL']<price_data['roc']].index,'roc_SIGNAL']=0
        if k=='mom':
            price_data['mom']=chromo_fun['mom'](real=np.array(price_data['CL'],dtype='f8'),timeperiod=indic_dict['mom']['parameter'])
            price_data.loc[price_data[price_data['CL']>price_data['mom']].index,'mom_SIGNAL']=1
            price_data.loc[price_data[price_data['CL']<price_data['mom']].index,'mom_SIGNAL']=0
        if k=='macd':
            price_data['macd'],price_data['macdsignal'],price_data['macdhist']=chromo_fun['macd'](np.array(price_data['CL'],dtype='f8'))
            price_data.loc[price_data[0<price_data['macd']].index,'DIFF']=1
            price_data.loc[price_data[0<price_data['macdsignal']].index,'DEA']=1
            price_data.loc[price_data[price_data['macd']>price_data['macdsignal']].index,'D']=1
            price_data.loc[price_data[0>price_data['macd']].index,'DIFF']=0
            price_data.loc[price_data[0>price_data['macdsignal']].index,'DEA']=0
            price_data.loc[price_data[price_data['macd']<price_data['macdsignal']].index,'D']=0
            price_data['macd_SIGNAL']=price_data['DIFF']*price_data['DEA']*price_data['D']
        if k=='t3':
            price_data['t3']=chromo_fun['t3'](real=np.array(price_data['CL'],dtype='f8'))
            price_data.loc[price_data[price_data['CL']>price_data['t3']].index,'t3_SIGNAL']=1
            price_data.loc[price_data[price_data['CL']<price_data['t3']].index,'t3_SIGNAL']=0

        if k=='kdj':
            price_data['kdj_slowk'],price_data['kdj_slowd']=chromo_fun['kdj'](np.array(price_data['HI'],dtype='f8'),np.array(price_data['LO'],dtype='f8'),np.array(price_data['CL'],dtype='f8'))
            price_data.loc[price_data[price_data['kdj_slowk']>price_data['kdj_slowd']].index,'kdj'+'_SIGNAL']=1
            price_data.loc[price_data[price_data['kdj_slowk']<price_data['kdj_slowd']].index,'kdj'+'_SIGNAL']=0
    l=len(price_data)
    signal=0
    for k in indic_dict:
        signal+=price_data[k+'_SIGNAL'].iloc[l-1]*indic_dict[k]['weight']
    if signal>0.5:
        return 1
    else:
        return 0

#股票代码获取函数：
def sc_get():
    db = cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
    cur = db.cursor()    
    return cur.execute('SELECT stkcode FROM vw_stk_code').fetchall()

#数据获取函数：
def price_get(stock_code='300109'):
    db = cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
    # sql = '''
    #     select trdate,v_open*(adj_price/v_close) op,v_high*(adj_price/v_close) hi,v_low*(adj_price/v_close) lo,adj_price cl,v_change ch 
    #       from stk_mkt where stkcode='%s' order by trdate'''
    sql = '''
        select trdate,v_open*(adj_price/v_close) op,v_high*(adj_price/v_close) hi,v_low*(adj_price/v_close) lo,adj_price cl,v_change ch
          from stk_mkt_ex_t where stkcode='%s' order by trdate
        '''
    data = pd.read_sql(sql % (stock_code,),db)
    return data

#归一化函数：
def popu_norm(popu_mem):
    for i in range(len(popu_mem)):
        ep=popu_mem[i]
        sumrate=sum(ep.chromosome.values())
        ep.chromosome={k:ep.chromosome[k]/sumrate for k in chromo_name}
    return popu_mem

#指标集：
class indic_w(object):
    def __init__(self,thred=0.02):
        self.chromo_n0=['dema','ema','kama','sma','tema','trima']#midpoint,wma被删除
        self.chromo_n1=['mom','roc','rsi','ema_ali']
        self.chromo_n2=['macd','t3','kdj','ma_cross']
        self.chromo_n=self.chromo_n0+self.chromo_n1+self.chromo_n2
        len_chname=len(self.chromo_n)
        self.chromo0={i:1/len_chname for i in self.chromo_n}
        self.thred=thred
        self.dump=[]
        self.fitness=0
        self.opt_list=[]
        self.idx=0
        self.opt_para={}
        self.opt_prof=[]
        self.cn0=['dema','ema','kama','sma','tema','trima']
        self.cn1=['mom','roc','rsi','ema_ali']
        self.cn2=['macd','t3','kdj','ma_cross']
        self.wei_dict={}
        self.data_signal=[]
        self.opt_gene={}

    #淘汰函数
    def eliminate(self,wei_dict,fitness):
        if fitness<self.fitness:
            if self.idx>=len(self.opt_list):
                print('it is the best')
                self.opt_gene={k:{'weight':self.wei_dict[k],'parameter':self.opt_para[k]} for k in self.wei_dict}
                print('optimization',self.opt_gene)
                return 0
            if self.dump[-1] in self.cn0:
                self.chromo_n0.append(self.dump[-1])
            elif self.dump[-1] in self.cn1:
                self.chromo_n1.append(self.dump[-1])
            else:
                self.chromo_n2.append(self.dump[-1])

            self.opt_list.insert(self.idx,self.dump[-1])
            self.dump.remove(self.dump[-1])
            self.dump.append(self.opt_list[self.idx+1])
            self.opt_list.remove(self.opt_list[self.idx+1])
            self.idx+=1
            if self.dump[-1] in self.chromo_n0:
                self.chromo_n0.remove(self.dump[-1])
            elif self.dump[-1] in self.chromo_n1:
                self.chromo_n1.remove(self.dump[-1])
            else:
                self.chromo_n2.remove(self.dump[-1])
            self.chromo_n=self.chromo_n0+self.chromo_n1+self.chromo_n2
            len_chname=len(self.chromo_n)
            self.chromo0={i:1/len_chname for i in self.chromo_n}

        else:
            self.fitness=fitness
            self.wei_dict=wei_dict
            wei_list=[[wei_dict[k],k] for k in wei_dict]
            e_indic=sorted(wei_list)
            self.opt_list=[j[1] for j in e_indic]
            if e_indic[0][0]<self.thred:
                if e_indic[0][1] in self.chromo_n0:
                    self.chromo_n0.remove(e_indic[0][1])
                elif e_indic[0][1] in self.chromo_n1:
                    self.chromo_n1.remove(e_indic[0][1])
                else:
                    if e_indic[0][1] not in self.chromo_n2:
                        print('indictor removed',e_indic[0][1])
                        print('chromo_n2',self.chromo_n2)
                    self.chromo_n2.remove(e_indic[0][1])
                self.chromo_n=self.chromo_n0+self.chromo_n1+self.chromo_n2
                len_chname=len(self.chromo_n)
                self.chromo0={i:1/len_chname for i in self.chromo_n}
                self.dump.append(e_indic[0][1])
                self.idx=0
            else:
                print('all weight are bigger than threshold')
                self.opt_gene={k:{'weight':self.wei_dict[k],'parameter':self.opt_para[k]} for k in self.wei_dict}
                return 0
        return 1

    #优化参数
    def para_opt(self,stock_code,period=[range(10,16)+range(45,61)]):
        data=price_get(stock_code)
        self.data_signal=copy.deepcopy(data)
        bot=-10
        for k in self.cn0:
            opt_k=0
            opt_p=bot
            for p in period:
                data[k]=chromo_fun[k](real=np.array(data['CL'],dtype='f8'),timeperiod=p)
                data.loc[data[data['CL']>data[k]].index,k+'_SIGNAL']=1
                data.loc[data[data['CL']<data[k]].index,k+'_SIGNAL']=0
                if 'deal' in data.columns : del data['deal']
                data.loc[data[data[k+'_SIGNAL']>data[k+'_SIGNAL'].shift()].index,'deal']=1
                data.loc[data[data[k+'_SIGNAL']<data[k+'_SIGNAL'].shift()].index,'deal']=0
                ep=get_stat(data)[0]
                if ep>opt_p:
                    opt_k,opt_p=p,ep
            print(k,opt_k,opt_p)
            if opt_p<bot:
                self.chromo_n0.remove(k)
                print(k,'is out')
                continue
            self.opt_para[k]=opt_k
            self.opt_prof.append([opt_p,k])
            self.data_signal[k]=chromo_fun[k](real=np.array(self.data_signal['CL'],dtype='f8'),timeperiod=opt_k)
            self.data_signal.loc[self.data_signal[self.data_signal['CL']>self.data_signal[k]].index,k+'_SIGNAL']=1
            self.data_signal.loc[self.data_signal[self.data_signal['CL']<self.data_signal[k]].index,k+'_SIGNAL']=0

        #均线交叉
        opt_ks=0
        opt_kl=0
        opt_p=bot
        for ps in range(7,120,7):
            pl=ps+7
            while(pl<140):
                data['ma_cross_SIGNAL']=MA_Cross(price_dt=data,short_ma=ps,long_ma=pl)
                if 'deal' in data.columns : del data['deal']
                data.loc[data[data[k+'_SIGNAL']>data[k+'_SIGNAL'].shift()].index,'deal']=1
                data.loc[data[data[k+'_SIGNAL']<data[k+'_SIGNAL'].shift()].index,'deal']=0
                ep=get_stat(data)[0]
                if ep>opt_p:
                    opt_ks,opt_kl,opt_p=ps,pl,ep
                pl+=7
#            self.opt_para['ma_cross']={'ma_cross_sma':opt_ks,'ma_cross_lma':opt_kl}
#            self.opt_prof.append([opt_p,'ma_cross'])
        print('ma_cross',opt_ks,opt_kl,opt_p)
        if opt_p<bot:
            self.chromo_n2.remove('ma_cross')
            print('ma_cross is out')
        else:
            self.opt_para['ma_cross']={'ma_cross_sma':opt_ks,'ma_cross_lma':opt_kl}
            self.opt_prof.append([opt_p,'ma_cross'])
            self.data_signal['ma_cross_SIGNAL']=chromo_fun['ma_cross'](price_dt=data,short_ma=opt_ks,long_ma=opt_kl)

        #rsi指标
        opt_k=0
        opt_p=bot
        for p in period:
            data['rsi']=chromo_fun['rsi'](np.array(data['CL'],dtype='f8'),timeperiod=p)
            data.loc[data[50<data['rsi']].index,'rsi'+'_SIGNAL']=1
            data.loc[data[50>data['rsi']].index,'rsi'+'_SIGNAL']=0
            if 'deal' in data.columns : del data['deal']
            data.loc[data[data['rsi_SIGNAL']>data['rsi_SIGNAL'].shift()].index,'deal']=1
            data.loc[data[data['rsi_SIGNAL']<data['rsi_SIGNAL'].shift()].index,'deal']=0
            ep=get_stat(data)[0]
            if ep>opt_p:
                opt_k,opt_p=p,ep
        print('rsi',opt_k,opt_p)
        if opt_p<bot:
            self.chromo_n1.remove('rsi')
            print('rsi is out')
        else:
            self.opt_para['rsi']=opt_k
            self.opt_prof.append([opt_p,'rsi'])
            self.data_signal['rsi']=chromo_fun['rsi'](real=np.array(self.data_signal['CL'],dtype='f8'),timeperiod=opt_k)
            self.data_signal.loc[self.data_signal[self.data_signal['CL']>self.data_signal['rsi']].index,'rsi'+'_SIGNAL']=1
            self.data_signal.loc[self.data_signal[self.data_signal['CL']<self.data_signal['rsi']].index,'rsi'+'_SIGNAL']=0

        #多头排列指标
        opt_p=bot
        self.data_signal['ema_ali']=chromo_fun['ema_ali'](copy.deepcopy(data))
        self.data_signal.loc[self.data_signal[0<self.data_signal['ema_ali']].index,'ema_ali'+'_SIGNAL']=1
        self.data_signal.loc[self.data_signal[0>self.data_signal['ema_ali']].index,'ema_ali'+'_SIGNAL']=0
        data['ema_ali_SIGNAL']=self.data_signal['ema_ali_SIGNAL']
        if 'deal' in data.columns : del data['deal']
        data.loc[data[data['ema_ali_SIGNAL']>data['ema_ali_SIGNAL'].shift()].index,'deal']=1
        data.loc[data[data['ema_ali_SIGNAL']<data['ema_ali_SIGNAL'].shift()].index,'deal']=0
        opt_p=get_stat(data)[0]
        print('ema_ali',opt_p)
        if opt_p>bot:
            self.opt_para['ema_ali']={'parameter':'default'}
            self.opt_prof.append([opt_p,'ema_ali'])
        else:
            self.chromo_n1.remove('ema_ali')
            print('ema_ali is out')

        #roc指标
        opt_k=0
        opt_p=bot
        for p in period:
            data['roc']=chromo_fun['roc'](np.array(data['CL'],dtype='f8'),timeperiod=p)
            data.loc[data[0<data['roc']].index,'roc'+'_SIGNAL']=1
            data.loc[data[0>data['roc']].index,'roc'+'_SIGNAL']=0
            if 'deal' in data.columns : del data['deal']
            data.loc[data[data['roc_SIGNAL']>data['roc_SIGNAL'].shift()].index,'deal']=1
            data.loc[data[data['roc_SIGNAL']<data['roc_SIGNAL'].shift()].index,'deal']=0
            ep=get_stat(data)[0]
            if ep>opt_p:
                opt_k,opt_p=p,ep
        print('roc',opt_k,opt_p)
        if opt_p<bot:
            self.chromo_n1.remove('roc')
            print('roc is out')
        else:
            self.opt_para['roc']=opt_k
            self.opt_prof.append([opt_p,'roc'])
            self.data_signal['roc']=chromo_fun['roc'](real=np.array(self.data_signal['CL'],dtype='f8'),timeperiod=opt_k)
            self.data_signal.loc[self.data_signal[self.data_signal['CL']>self.data_signal['roc']].index,'roc'+'_SIGNAL']=1
            self.data_signal.loc[self.data_signal[self.data_signal['CL']<self.data_signal['roc']].index,'roc'+'_SIGNAL']=0

        #mom指标
        opt_k=0
        opt_p=bot
        for p in period:
            data['mom']=chromo_fun['mom'](np.array(data['CL'],dtype='f8'),timeperiod=p)
            data.loc[data[0<data['mom']].index,'mom'+'_SIGNAL']=1
            data.loc[data[0>data['mom']].index,'mom'+'_SIGNAL']=0
            if 'deal' in data.columns : del data['deal']
            data.loc[data[data['mom_SIGNAL']>data['mom_SIGNAL'].shift()].index,'deal']=1
            data.loc[data[data['mom_SIGNAL']<data['mom_SIGNAL'].shift()].index,'deal']=0
            ep=get_stat(data)[0]
            if ep>opt_p:
                opt_k,opt_p=p,ep
        print('mom',opt_k,opt_p)
        if opt_p<bot:
            self.chromo_n1.remove('mom')
            print('mom is out')
        else:
            self.opt_para['mom']=opt_k
            self.opt_prof.append([opt_p,'mom'])
            self.data_signal['mom']=chromo_fun['mom'](real=np.array(self.data_signal['CL'],dtype='f8'),timeperiod=opt_k)
            self.data_signal.loc[self.data_signal[self.data_signal['CL']>self.data_signal['mom']].index,'mom'+'_SIGNAL']=1
            self.data_signal.loc[self.data_signal[self.data_signal['CL']<self.data_signal['mom']].index,'mom'+'_SIGNAL']=0

        #macd指标
        opt_p=bot
        self.data_signal['macd'],self.data_signal['macdsignal'],self.data_signal['macdhist']=chromo_fun['macd'](np.array(self.data_signal['CL'],dtype='f8'))
        self.data_signal.loc[self.data_signal[0<self.data_signal['macd']].index,'DIFF']=1
        self.data_signal.loc[self.data_signal[0<self.data_signal['macdsignal']].index,'DEA']=1
        self.data_signal.loc[self.data_signal[self.data_signal['macd']>self.data_signal['macdsignal']].index,'D']=1
        self.data_signal.loc[self.data_signal[0>self.data_signal['macd']].index,'DIFF']=0
        self.data_signal.loc[self.data_signal[0>self.data_signal['macdsignal']].index,'DEA']=0
        self.data_signal.loc[self.data_signal[self.data_signal['macd']<self.data_signal['macdsignal']].index,'D']=0
        self.data_signal['macd_SIGNAL']=self.data_signal['DIFF']*self.data_signal['DEA']*self.data_signal['D']
        data['macd_SIGNAL']=self.data_signal['macd_SIGNAL']
        if 'deal' in data.columns : del data['deal']
        data.loc[data[data['macd_SIGNAL']>data['macd_SIGNAL'].shift()].index,'deal']=1
        data.loc[data[data['macd_SIGNAL']<data['macd_SIGNAL'].shift()].index,'deal']=0

        opt_p=get_stat(data)[0]
        print('macd',{'fastperiod':12, 'slowperiod':26, 'signalperiod':9},opt_p)
        if opt_p>bot:
            self.opt_para['macd']={'fastperiod':12, 'slowperiod':26, 'signalperiod':9}
            self.opt_prof.append([opt_p,'macd'])
        else:
            self.chromo_n2.remove('macd')
            print('macd is out')

        #T3指标
        opt_p=bot
        self.data_signal['t3']=chromo_fun['t3'](np.array(self.data_signal['CL'],dtype='f8'))
        self.data_signal.loc[self.data_signal[self.data_signal['CL']>self.data_signal['t3']].index,'t3'+'_SIGNAL']=1
        self.data_signal.loc[self.data_signal[self.data_signal['CL']<self.data_signal['t3']].index,'t3'+'_SIGNAL']=0
        data['t3_SIGNAL']=self.data_signal['t3_SIGNAL']
        if 'deal' in data.columns : del data['deal']
        data.loc[data[data['t3_SIGNAL']>data['t3_SIGNAL'].shift()].index,'deal']=1
        data.loc[data[data['t3_SIGNAL']<data['t3_SIGNAL'].shift()].index,'deal']=0
        opt_p=get_stat(data)[0]
        print('t3',{'timeperiod':5, 'vfactor':0.7},opt_p)
        if opt_p>bot:
            self.opt_para['t3']={'timeperiod':5, 'vfactor':0.7}
            self.opt_prof.append([opt_p,'t3'])
        else:
            self.chromo_n2.remove('t3')
            print('t3 is out')

        #kdj指标
        opt_p=bot
        self.data_signal['kdj_slowk'],self.data_signal['kdj_slowd']=ta.STOCH(np.array(self.data_signal['HI'],dtype='f8'),np.array(self.data_signal['LO'],dtype='f8'),np.array(self.data_signal['CL'],dtype='f8'))
        self.data_signal.loc[self.data_signal[self.data_signal['kdj_slowk']>self.data_signal['kdj_slowd']].index,'kdj'+'_SIGNAL']=1
        self.data_signal.loc[self.data_signal[self.data_signal['kdj_slowk']<self.data_signal['kdj_slowd']].index,'kdj'+'_SIGNAL']=0
        data['kdj_SIGNAL']=self.data_signal['kdj_SIGNAL']
        if 'deal' in data.columns : del data['deal']
        data.loc[data[data['kdj_SIGNAL']>data['kdj_SIGNAL'].shift()].index,'deal']=1
        data.loc[data[data['kdj_SIGNAL']<data['kdj_SIGNAL'].shift()].index,'deal']=0
        opt_p=get_stat(data)[0]
        print('kdj',{'fastk_period':5, 'slowk_period':3, 'slowk_matype':0, 'slowd_period':3, 'slowd_matype':0},opt_p)
        if opt_p>bot:
            self.opt_para['kdj']={'fastk_period':5, 'slowk_period':3, 'slowk_matype':0, 'slowd_period':3, 'slowd_matype':0}
            self.opt_prof.append([opt_p,'kdj'])
        else:
            self.chromo_n2.remove('kdj')
            print('kdj is out')

        self.chromo_n=self.chromo_n0+self.chromo_n1+self.chromo_n2

#    def indic_bs(self):


#评价体系类：
class eval_sys(object):
    def __init__(self,prof=0,win_pro=0,match_rate=0,sharp=0,retrace_max=0,deal_times=0):
        self.prof=prof
        self.win_pro=win_pro
        self.match_rate=match_rate
        self.sharp=sharp
        self.retrace_max=retrace_max
        self.deal_times=deal_times
        self.fitscore=0

    def score_cal(self):pass

#种群类：
class popu(object):
    def __init__(self,chromosome,amount=0.0,sharp_rate=0.0,win_rate=0.0,fitness=0.0):
        self.chromosome=chromosome#指标权重参数字典
        self.amount=amount#资金总量
        self.sharp_rate=sharp_rate
        self.win_rate=win_rate
        self.fitness=fitness#适应度

#初始化种群：
def popu_init(chromo_name_i,chromo_i,style=0,chn=[]):
    if style==0:
        popu_mem=[popu(chromosome=chromo_i) for i in range(PSIZE-len(chromo_i))]
    elif style==1:
        popu_mem=[popu(chromosome=chromo_i[j]) for j in range(len(chromo_i))]
    else:
        popu_mem=[popu(chromosome={j:rand.uniform(0,1) for j in chromo_name_i}) for i in range(PSIZE)]
        for i in range(PSIZE):
            s=sum(popu_mem[i].chromosome.values())
            for j in range(len(chromo_name_i)):
                popu_mem[i].chromosome[chromo_name_i[j]]/=s
    return popu_mem

#解码函数：
def decode(popu_mem,chnd0,chnd1,chnd2,chnd,data_sig,para_d):
    if popu_mem[0].fitness==0:
        sta=0
    else:
        sta=PSIZE
    for i in range(sta,len(popu_mem)):
        popu_mem[i].fitness=fitness_cal(indic_wei0=popu_mem[i].chromosome,chn0=chnd0,chn1=chnd1,chn2=chnd2,chn=chnd,para=para_d,data=copy.deepcopy(data_sig))
    return popu_mem

#交叉函数：
def crossover(popu_mem,chromo_name_c):
    len_p=len(popu_mem)
    len_chn=len(chromo_name_c)
    for i in range(0,len_p,2):
        if rand.uniform(0,1)<PC:
            temp_chromo1={}
            temp_chromo2={}
            cr_pos=rand.randint(0,len_chn-2)
            x=i
            y=i+1
            if rand.randint(0,1)==1:
                x=i+1
                y=i
            for j in range(len_chn):
                if cr_pos<j:
                    temp_chromo1[chromo_name_c[j]]=popu_mem[x].chromosome[chromo_name_c[j]]
                    temp_chromo2[chromo_name_c[j]]=popu_mem[y].chromosome[chromo_name_c[j]]
                else:
                    temp_chromo1[chromo_name_c[j]]=popu_mem[y].chromosome[chromo_name_c[j]]
                    temp_chromo2[chromo_name_c[j]]=popu_mem[x].chromosome[chromo_name_c[j]]

            popu_mem.append(popu(chromosome=temp_chromo1))
            popu_mem.append(popu(chromosome=temp_chromo2))
    return popu_mem

#变异函数：
def mutation(popu_mem,chromo_name_m):
    for i in range(PSIZE):
        len_chname=len(chromo_name_m)
        if rand.uniform(0,1)<PC:
            temp_chromo=copy.copy(popu_mem[i].chromosome)
            mt_pos=rand.randint(0,len_chname-1)
            temp_chromo[chromo_name_m[mt_pos]]=rand.uniform(0,1)
            popu_mem.append(popu(chromosome=temp_chromo))
    return popu_mem

#适应度函数：
def fitness_cal(indic_wei0,chn0,chn1,chn2,chn,para,data=price_get(),dealtimes_need=0,thred_b=0.5,thred_s=0.5):
    #归一化
    sf=sum(indic_wei0.values())
    if sf==0:
        return 0
    temp_chf={k:indic_wei0[k]/sf for k in indic_wei0}
    indic_wei=temp_chf
    for each_name in chn:
        data[each_name+'_SIGNAL']=data[each_name+'_SIGNAL']*indic_wei[each_name]
    data['wei_indic']=0
    for k in chn:

        data['wei_indic']=data['wei_indic']+data[k+'_SIGNAL']
    data.loc[data[data['wei_indic']>thred_b].index,'bs']=1
    data.loc[data[data['wei_indic']<thred_s].index,'bs']=0
    data.loc[data[data['bs']>data['bs'].shift()].index,'deal']=1
    data.loc[data[data['bs']<data['bs'].shift()].index,'deal']=0
    data['deal']=data['deal'].shift()
    cnt=len(data)
    dt=data.dropna().copy()
    if len(dt)==0:return 0
    trcnt=len(dt)
    dt['net_prof']=dt.OP/dt.OP.shift()*0.997
    es=dt.loc[dt['bs']==0,'net_prof'].cumprod()
    if len(es)==0:return 0
    e=es.iloc[len(es)-1]
    if dealtimes_need>0:
        print('deal_days',len(data['TRDATE']))
        print('deal_times',len(data.loc[data[(data['bs']<>data['bs'].shift(1))].index]))
        print('prof in fact',e)
        print('fitness',e*(1-trcnt/cnt))
        return e
#    if math.isnan(e): print('fit_cal_nan',e)
    return e*(1-trcnt/cnt)

#累积适应度函数：
def sumfit(fitness_list):
    sumf=fitness_list[:1]
#    print('sumf_all',sumf)
    for i in fitness_list:
        if math.isnan(i):
#           print('fitness',i,fitness_list.index(i))
           break
    for i in range(len(fitness_list))[1:]:
        sumf.append(sumf[-1]+fitness_list[i])
        if math.isnan(sumf[i]):
           print('sumf',i,sumf[i],sumf[0])
           break
    return list(np.array(sumf)/sumf[-1])

#种群适应度排序函数：
def popu_sort(popu_mem):
    lp=len(popu_mem)
    for i in range(lp):
        if math.isnan(popu_mem[i].fitness):
            popu_mem[i].fitness=0
    fit_idx=[[popu_mem[i].fitness,i] for i in range(lp)]
    fit_sort=sorted(fit_idx)
    popu_new=[popu_mem[fit_sort[i][1]] for i in range(lp)]
    for i in range(lp):
        if math.isnan(popu_new[i].fitness):
            print('popu_sort_nan',i,popu_new[i].fitness)
    return popu_new

#选择函数：
def select(popu_mem):
    if len(popu_mem)<PSIZE:
        print('popu size',len(popu_mem))
    fl=sumfit([i.fitness for i in popu_mem])#适应度累积分布
    if len(fl)<len(popu_mem):
        print('fl len',len(fl),'popu len',len(popu_mem))
    fl_idx=[[fl[i],i] for i in range(len(popu_mem))]
    temp_popu=[]
    ps=sorted([[rand.uniform(0,1)] for i in range(PSIZE)])#随机选择数列表
    idx=0
    j=0
    while(idx<PSIZE):
        if j>=len(popu_mem):
            print('j',j,'fl[j-1]',fl[j-1],'i',idx,'ps(i)',ps[idx])

        if fl[j]>=ps[idx]:
            temp_popu.append(popu_mem[j])
            idx+=1
        else:
            j+=1
    #精英机制：确保留下原种群中适应度最大的
    if temp_popu[-1].fitness<popu_mem[-1].fitness:
        temp_popu[-1]=popu_mem[-1]
    return temp_popu

#主函数：
def GA_main(init0=0,sc_ga='300109'):
    spt=1
    # 初始化种群：
    flag=1
    indic_w0=indic_w(thred=0.10)
    #优化参数：
    indic_w0.para_opt(stock_code=sc_ga,period=range(20,60))
    # t=time.time()
    while(flag>0):
        chromo_name0=indic_w0.chromo_n0
        chromo_name1=indic_w0.chromo_n1
        chromo_name2=indic_w0.chromo_n2
        chromo_name=indic_w0.chromo_n
        len_chname=len(chromo_name)

        #等权重基因初始化：
        chromo0={j:1/len_chname for j in chromo_name}#每个指标（染色体）的初始权重   
        #按最优化参数所得收益进行归一化，即指标的初始权重等于该指标最优化收益率占所有指标最优化收益率总和的百分率
        for k in indic_w0.opt_prof:
            if k[0]<0:
               k[0]=0.005
        opt_prof_dict={k[1]:k[0] for k in indic_w0.opt_prof}

        prof_opt=[opt_prof_dict[k] for k in indic_w0.chromo_n]
        sumopt=sum(prof_opt)
        chromo_init_fit={k:opt_prof_dict[k]/sumopt for k in indic_w0.chromo_n}
        #按照仅使用单个指标进行初始化
        single_chromo_popu=[]
        single_chromo={k:0 for k in indic_w0.chromo_n}
        single_i=0
        for k in single_chromo:
            temp_single=copy.deepcopy(single_chromo)
            temp_single[k]=1
            single_chromo_popu.append(temp_single)
            single_i+=1
        popu_mem01=popu_init(chromo_name_i=chromo_name,style=init0,chromo_i=chromo_init_fit)

        popu_mem02=popu_init(chromo_name_i=chromo_name,style=1,chromo_i=single_chromo_popu)

        popu_mem0=popu_mem01+popu_mem02
        i=0
        while(i<=TERMINAL):
            s=sum(popu_mem0[-1].chromosome.values())
            temp_ch={k:popu_mem0[-1].chromosome[k]/s for k in popu_mem0[-1].chromosome}
        #交叉
            popu_mem1=crossover(popu_mem0,chromo_name_c=chromo_name)
        #变异
            popu_mem2=mutation(popu_mem1,chromo_name_m=chromo_name)
        #解码
            popu_mem3=decode(popu_mem=popu_mem2,chnd0=chromo_name0,chnd1=chromo_name1,chnd2=chromo_name2,chnd=chromo_name,data_sig=indic_w0.data_signal,para_d=indic_w0.opt_para)
        #排序            
            popu_mem4=popu_sort(popu_mem3)
        #选择
            popu_mem0=select(popu_mem4)
            i+=1
        sf=sum(popu_mem0[-1].chromosome.values())
        temp_chf={k:popu_mem0[-1].chromosome[k]/sf for k in popu_mem0[-1].chromosome}

        #淘汰 
        flag=indic_w0.eliminate(wei_dict=temp_chf,fitness=popu_mem0[-1].fitness)
        if flag==0:
            fitness_cal(indic_wei0=indic_w0.wei_dict,chn0=chromo_name0,chn1=chromo_name1,chn2=chromo_name2,chn=indic_w0.wei_dict.keys(),para=indic_w0.opt_para,data=copy.deepcopy(indic_w0.data_signal),dealtimes_need=1)
    return {'profit':indic_w0.fitness,'opt_indic':copy.copy(indic_w0.opt_gene)}

#client = MongoClient('mongodb://mcdb:mcdb@192.168.1.142:27017/mcdb')
#mdb=client.mcdb
#c=mdb.mx

def get_result(sci):
#    a=sc_get()
#    for sci in range(len(a['STOCKCODE'])):
#        a['STOCKCODE'][sci]='300344'
    print('stock',sci,'starting')

    data=price_get(stock_code=sci)

    if len(data)<100:
        print('可交易天数少于60，不予处理')
        return
    res=GA_main(init0=0,sc_ga=sci)
    res.update({'stock_code':sci,'lasttime':datetime.now()})
    print('result',res)
    return res
#        c.save(res)


def md_read(code0):
    client = MongoClient('mongodb://mcdb:mc969@192.168.0.32:27017/mcdb')
    mdb = client.mcdb
    c = mdb.ga
    print(type(c))
    print(c.collection_names)
    for code in code0:
        dict_zb2 = c.find_one({'stock_code':code})
        print(dict_zb2)




if __name__ == "__main__":      
    print 'starting at:',datetime.now()
    import sys
    h=get_result(sys.argv[1])
    print(h)
    print(type(h))
    print 'end at:',datetime.now()
    indic_cal(price_data=price_get(stock_code='000004'),indic_dict=h['opt_indic'])

    


