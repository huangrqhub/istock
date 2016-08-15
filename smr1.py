#-*-coding:utf-8-*-
import scipy.stats.mstats as mstats
from bt3 import *
import talib as ta
from etl4 import ETL as etl
import os,sys
import chardet

reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

import cx_Oracle
db1=cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
x = [8,13,21,34,55,89,144,233]

def get_r(stk):
    data = load_from_db(stk)
    if len(data)<144: return []
    
    cl = np.array(data['CL'],dtype='f8')
    for i in x:
        data['m'+str(i)] = ta.EMA(cl,timeperiod=i)
    
    for i,r in data.iterrows():
        z = [[r['m8'],8],[r['m13'],13],[r['m21'],21],[r['m34'],34],[r['m55'],55],[r['m89'],89],[r['m144'],144],[r['m233'],233]]
        c,p = mstats.spearmanr(x,[k[1] for k in sorted(z,reverse=True)])
        data.loc[i,'r'] = c
    data['mr'] = ta.SMA(np.array(data['r'],dtype='f8'),10)

    return data.values[-1][-2:]

def get_grade_info(value):
    info = ('强势','较强势','盘整','较弱势','弱势','转强','转弱','持续')
    g = (0.6,0.2,-0.2,-0.6)
    ri = []    

    if value[0] > g[0]:
        ri.append(info[0])
    elif value[0] <= g[0] and value[0] > g[1]:
        ri.append(info[1])
    elif value[0] <= g[1] and value[0] > g[2]:
        ri.append(info[2])
    elif value[0] <= g[2] and value[0] > g[3]:
        ri.append(info[3])
    else:
        ri.append(info[4])

    if value[0] > value[1]:
        ri.append(info[5])
    elif value[0] < value[1]:
        ri.append(info[6])
    else:
        ri.append(info[7])
    return ri

stks = get_stock_list()

cur = db1.cursor()
sql = '''update stk_dig_t set content='#个股状态# '||to_char(sysdate,'mm/dd ')||'%s',cdate=sysdate where stkcode='%s' '''
for s in stks:
    r = get_r(s[0])
    if len(r)>0:
        m = get_grade_info(r)
        t = '处于'+m[0]+'行情, '+'走势'+m[1]
        sql1 = sql % (t,s[0])
        cur.execute(sql1)
        db1.commit()
        #print chardet.detect(sql1),sql1

e = etl()
e.setDIG()

