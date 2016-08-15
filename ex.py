# -*- coding: utf-8 -*- 
from __future__ import division
import cx_Oracle
from datetime import datetime
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class EX:
    def __init__(self):
        self.db = cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
        self.cur = self.db.cursor()

    def EX(self,code,dt):
        rows_ex = self.cur.execute('''select exdate,cum from vw_stk_ex where stkcode='%s' order by 1''' % (code,)).fetchall()
        rows_ex = [(datetime.strptime('19000101','%Y%m%d'),1)]+rows_ex+[(datetime.strptime('20990101','%Y%m%d'),1)]

        # 获取复权因子
        for i in range(0,len(rows_ex)-1):
            if dt>rows_ex[i][0] and dt<=rows_ex[i+1][0] : break
        
        del rows_ex[0:i]
        ex = rows_ex[0]
        del rows_ex[0]

        sql = '''
          select trdate,topen,thigh,tlow,tclose,chng_pct,tvolume
            from vw_stk_mkt where stkcode='%s' and trdate>to_date('%s','yyyymmdd') order by 1
            '''
        rows_data = self.cur.execute(sql % (code,dt.strftime('%Y%m%d'))).fetchall()
        sql = '''
            insert into stk_mkt_ex_t values('%s',to_date('%s','yyyymmdd'),%s,%s,%s,%s,%s,%s,%s)
            '''
        # 后复权数据
        for rd in rows_data:
            if rd[0]>=rows_ex[0][0]:
                ex = rows_ex[0]
                del rows_ex[0]
            self.cur.execute(sql % (code,rd[0].strftime('%Y%m%d'),rd[1],rd[2],rd[3],rd[4],rd[5],rd[6],rd[4]*ex[1]))
        self.db.commit()

    def EX_ALL(self):
        # 复权所有股票
        dp, = self.cur.execute('select max(trdate) from stk_mkt_ex_t').fetchone()
        da, = self.cur.execute('select max(trdate) from vw_stk_mkt').fetchone()
        if dp>=da: return

        stks = self.cur.execute('''select stkcode from vw_stk_code''').fetchall()
        for s in stks:
            sql = '''select nvl(max(trdate),to_date('20140101','yyyymmdd')) from stk_mkt_ex_t where stkcode='%s' '''
            d, = self.cur.execute(sql % s).fetchone()
            self.EX(s[0],d)
