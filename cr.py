# -*- coding: utf-8 -*- 
import urllib2 
import cx_Oracle
import sys,os
reload(sys)
sys.setdefaultencoding( "utf8" )
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

db=cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
cur = db.cursor()

now = ['2016年1季报','2015-12-31','2016年1季报','2016-03-31']
now = cur.callproc('get_cbtype_p',now)

def cr_yjkb(crdate,crtype):
    yd, = cur.execute('''select to_char(sysdate-2,'yyyy-mm-dd') from dual''').fetchone()
    url = 'http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=YJKB&fd=%s&st=13&sr=-1&p=%s&ps=50'

    for i in range(1,100):
        response = urllib2.urlopen(url % (crdate,i)).read()
        if 'stats:false' in response: return
        for r in eval(response):
            t = r.decode('utf8').split(',')
            if t[13]<yd: return
            # print t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],t[11],t[12],t[13],now[1],t[14]
            cur.callproc('cr_yjkb_p',[t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],t[11],t[12],t[13],crtype,t[14]])

cr_yjkb(now[1],now[0])
cr_yjkb(now[3],now[2])

def cr_yjyg(crdate,crtype):
    yd, = cur.execute('''select to_char(sysdate-2,'yyyy-mm-dd') from dual''').fetchone()
    url = 'http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=YJYG&fd=%s&st=4&sr=-1&p=%s&ps=50'
    
    for i in range(1,100):
        response = urllib2.urlopen(url% (crdate,i)).read()
        if 'stats:false' in response: return
        for r in eval(response):
            t = r.decode('utf8').split(',')
            if t[7]<yd: return
            cur.callproc('cr_yjyg_p',[t[0],t[1],t[2].replace('&sbquo',','),t[3],t[4],t[5],t[7],crtype,t[8]])

cr_yjyg(now[1],now[0])
#cr_yjyg('2016-06-30','2016半年报')
cr_yjyg(now[3],now[2])

def cr_ggcg():
    yd, = cur.execute('''select to_char(sysdate-2,'yyyy-mm-dd') from dual''').fetchone()
    url = 'http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=GG&sty=GGMX&p=%s&ps=100'
    
    for i in range(1,100):
        response = eval(urllib2.urlopen(url % i).read())
        for r in response:
            t = r.decode('utf8').split(',')    
            if t[5]<yd: return
            cur.callproc('cr_ggcg_p',[t[5],t[2],t[9],t[3],t[6],t[8],t[13],t[12],t[1],t[14],t[10],t[0]])

cr_ggcg()

