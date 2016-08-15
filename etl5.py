# -*- coding: utf-8 -*- 
from __future__ import division
from pymongo import MongoClient
import cx_Oracle
import os
from bson.dbref import DBRef
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class ETL:
    def __init__(self):
        client = MongoClient('mongodb://mcdb:mc969@192.168.0.32:27017/mcdb')
        self.mdb = client.mcdb # Mongodb连接

        self.odb=cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
        self.ocur = self.odb.cursor() # Oracle连接

    # 初始化用户自选股
    def initFOLLOW(self):
        self.ocur.execute('''update conf_t set last_time=to_date('20160101','yyyymmdd') where code='news_v1' ''')
        self.odb.commit()

        self.ocur.callproc('init_follow_p')
        self.mdb.ggxx.drop()
        self.mdb.xx.drop()
        self.setGGXX()
        self.setXX()

    # 同步VIP选股
    def setVIP(self):
        self.ocur.callproc('dbms_mview.refresh',['mv_mxvip','c'])
        sql = '''select stkcode,sudate,profit,'vip'||id id from mv_mxvip'''
        rows = self.ocur.execute(sql).fetchall()
        if not rows: return

        self.setML('mxvip',rows,0)
        self.ocur.execute('''update mx_t set last_time=sysdate where code='mxvip' ''')
        self.odb.commit()
        self.setMX()
        lastid, = self.ocur.execute('select max(id) from mv_mxvip').fetchone()
        self.setConf('mxvip',lastid)

    # 策略, 定时更新, 每分钟检查一次
    def setCL(self):
        self.ocur.callproc('dbms_mview.refresh',['mv_cl','c'])
        rows = self.ocur.execute('select * from mv_cl').fetchall()
        if not rows: return

        c = self.mdb.cl
        for r in rows:
            l = {}
            l['_id'] = r[0]
            l['cdate'] = r[1]
            l['content'] = r[2]
            l['name'] = r[3]
            l['image'] = r[5]
            l['isvip'] = r[6]
            c.save(l)

        # lastid, = self.ocur.execute('select max(id) from mv_cl').fetchone()
        self.setConf('cl',0)

    # 个人消息
    def setXX(self):        
       self.ocur.callproc('dbms_mview.refresh',['mv_user_news','c'])
       rows = self.ocur.execute('select * from mv_user_news').fetchall()
       if not rows: return

       c = self.mdb.xx
       for r in rows:
           l = {}
           l['_id'] = r[2]
           l['userid'] = r[0]
           l['newsid'] = DBRef('ggxx',r[1])
           c.insert(l)

       lastid, = self.ocur.execute('select nvl(max(id),0) from mv_user_news').fetchone()
       self.setConf('xx',lastid)

    # 个股新闻, 爬取后更新
    def setGGXX(self):        
        self.ocur.callproc('sync_news_p')
        rows = self.ocur.execute('''select id,stkcode,content,to_char(cdate,'mm/dd hh24:mi') cdate,title,stkname,type from mv_news''').fetchall()
        if not rows: return

        c = self.mdb.ggxx
        for r in rows:
            l = {}
            l['_id'] = r[0]
            l['code'] = r[1]
            l['content'] = r[2]
            l['cdate'] = r[3]
            l['name'] = r[4]
            l['sname'] = r[5]
            l['type'] = r[6]
            c.save(l)

        lastid, = self.ocur.execute('select max(id) from mv_news').fetchone()
        self.setConf('ggxx',lastid)
        self.setDIG()

    ## 个股交易数据, 每日数据更新完毕后更新
    def setGGSJ(self,opt):  
        if opt==0:
            c = self.mdb.ggsj # 日线
            sql1 = '''
              select to_char(trdate,'yyyymmdd') trdate,op,cl,hi,lo,vo,ch
                from mv_stk_td
                where stkcode='%s' 
                order by 1 desc
                '''
            sql2 = '''select status,to_char(trdate,'yyyymmdd') trdate from bs_t1 where stkcode='%s' '''
        else:
            c = self.mdb.ggsj1h # 1小时线
            sql1 = '''
              select to_char(trdate,'yyyymmddhh24mi') trdate,op,cl,hi,lo,vo,ch
                from mv_stk_td_1h
                where stkcode='%s' 
                order by 1 desc
                '''
            sql2 = '''select status,to_char(trdate,'yyyymmddhh24mi') trdate from bs_1h_t where stkcode='%s' '''

        rows = self.ocur.execute('select stkcode,stocksname from vw_stk_code').fetchall()
        for rc in rows:
            bs = {}
            rows_bs = self.ocur.execute(sql2 % (rc[0],)).fetchall()
            rows_d = self.ocur.execute(sql1 % (rc[0],)).fetchall()
            datas = []
            for rd in rows_d:
                l = {}
                l['open']=round(rd[1],2)
                l['close'] = round(rd[2],2)
                l['high'] = round(rd[3],2)
                l['low'] = round(rd[4],2)
                l['vol'] = round(rd[5],2)
                l['wrange'] = round(rd[6],2)
                l['date']=rd[0]
                l['b'] = 0
                l['s'] = 0
                if (1,rd[0]) in rows_bs: l['b'] = 1
                if (-1,rd[0]) in rows_bs: l['s'] = 1
                datas.append(l)
            c.save({'_id':rc[0],'code':rc[0], 'name': rc[1], 'datas':datas})

    ## 模型对应的股票列表, 不同选股模型更新各自股票列表
    def setML(self,code,data,opt):        
        c = self.mdb.ml
        if opt==9: c.remove({'mxid':code}) # 9 完全刷新, 其它 追加

        for r in data:
            l = {}
            l['stk'] = DBRef('stkdig',r[0])
            l['sudate'] = r[1]
            l['mxid'] = code 
            l['profit'] = r[2]
            l['_id'] = r[3]
            c.save(l)

    ## 股票信息摘要及最新状态, 模型更新状态后或爬虫爬取完毕后更新
    def setDIG(self):
        self.ocur.callproc('sync_dig_p')
        sql = '''
          select stkcode,stkname,content,to_char(cdate,'mm/dd hh24:mi') cdate
                ,status,to_char(stdate,'mm/dd') stdate,spel
            from mv_stk_dig
            '''
        c = self.mdb.stkdig
        rows = self.ocur.execute(sql).fetchall()
        for r in rows:
            l = {}
            l['_id'] = r[0]
            l['code'] = r[0]
            l['name'] = r[1]
            l['content'] = r[2]
            l['cdate'] = r[3]
            l['status'] = r[4]
            l['stdate'] = r[5]
            l['spel'] = r[6]
            c.save(l)

    ## 选股模型, 选股模型定期计算收益及胜率后更新
    def setMX(self):
        sql = '''
          select code,name,content,isvip,to_char(last_time,'mm/dd hh24:mi') ltime,image
                ,to_char(succ,'990.99') succ,to_char(profit,'990.99') profit,ktype 
            from mx_t where code not in ('mxjrjh1h','mxjrjh')
            '''
        rows = self.ocur.execute(sql).fetchall()
        c = self.mdb.mx
        c.drop()
        for r in rows:
            l = {}
            l['_id']=r[0]
            l['code'] = r[0]
            l['name'] = r[1]
            l['content'] = r[2]
            l['isvip'] = r[3]
            l['lasttime'] = r[4]
            l['image'] = r[5]
            l['succ'] = r[6]
            l['profit'] = r[7]
            l['ktype'] = r[8]
            c.save(l)

    ## 获取配置的最新值
    def getConf(self,code,opt):        
        x = self.ocur.callproc('conf_p',[code,opt,0])
        return x[2]

    ## 更新配置值
    def setConf(self,code,value):
        self.ocur.callproc('conf_p',[code,9,value])
