# -*- coding: utf-8 -*-
from __future__ import division
import pandas as pd
import urllib2 
import tushare as ts
import cx_Oracle
import sys,os
from etl4 import ETL as etl

reload(sys)
sys.setdefaultencoding( "utf8" )
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

db = cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
cur = db.cursor()

def get_Stocks():
    cur.execute('truncate table stk_code_t')
    ds = ts.get_stock_basics()
    sql = '''
      insert into stk_code_t values ('%s','%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,to_date(%s,'yyyymmdd'))
        '''
    ds.reset_index(inplace=True)
    for r in ds.values:
        t = tuple(r)
        if t[15]>0: cur.execute(sql % t)
    db.commit()
    cur.execute('insert into stk_info_ch_t select t.*,trunc(sysdate) from stk_code_t t')
    db.commit()

def cr_cb():
    now = ['2016年1季报','2015-12-31','2016年1季报','2016-03-31']
    now = cur.callproc('get_cbtype_p',now)
    yd, = cur.execute('''select to_char(sysdate-2,'yyyy-mm-dd') from dual''').fetchone()
    url = 'http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=YJBB&fd=%s&st=13&sr=-1&p=%s&ps=50'

    for i in range(1,100):
        response = urllib2.urlopen(url % (now[1],i)).read()
        cnt = 0
        if 'false' in response: return
        for r in eval(response):
            t = r.decode('utf8').split(',')
            if t[16]<yd: return
            cur.callproc('cr_cb_p',[t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],t[11],t[12],t[13],t[14],t[16],t[17]])
            cnt += 1
        if cnt<50: return 

def guzhi():
    sql = '''
      select '#估值#'||to_char(sysdate,'mm/dd hh24:mi')||' PEG: '||round(p.pe/cb.jl_tb,2)||', '|| case when p.pe/cb.jl_tb>2 then '超过合理估值' 
                  when p.pe/cb.jl_tb<1 then '低于合理估值'
                  when p.pe/cb.jl_tb between 1 and 2 then '合理估值区'
             end
            ,p.stkcode
        from
        (select c.stkcode,c.jl_tb
        from cb_t c,(select stkcode,max(rpt_type) rpt_type from cb_t group by stkcode) m
        where c.stkcode=m.stkcode and c.rpt_type=m.rpt_type) cb,
        (select stkcode,pe from STK_CODE_T t) p
        where cb.stkcode=p.stkcode and p.stkcode in (select stkcode from stk_cb_t) and p.pe/cb.jl_tb>0
    '''
    rows = cur.execute(sql).fetchall()
    for r in rows:
        cur.execute('''update stk_dig_t set content='%s',cdate=sysdate where stkcode='%s' ''' % r)
    db.commit()

    e = etl()
    e.setDIG()

cr_cb()
get_Stocks()
guzhi()
