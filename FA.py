#coding:utf-8
from __future__ import division
import cx_Oracle
import numpy as np
import pandas as pd
from sklearn.decomposition import FactorAnalysis

db=cx_Oracle.connect('mc3','mc3','192.168.1.119:1521/orcl')
sql='select * from VW_DEP_OPR'
data=pd.read_sql(sql,db)
print(data.head())
cur=db.cursor()
cur.execute('truncate table tmp')
for eyear in range(2010,2016):
    for eq in range(1,5):
        yq=str(eyear)+'-'+str(eq)
        data0=data[data['RTYPE']==yq].copy()
        sn=['CUR','QR','CAR','IR','SR','AR','ARTO','ARTD','CAT','CAD']
        dc=data0[sn].corr().copy()
        a,b=np.linalg.eig(dc)
        sa=[[a[i]/sum(a),dc.columns[i]] for i in range(len(sn))]
        sa.sort(reverse=True)
        sas=0
        sa_num=0
        sa0=[]
        for i in range(len(sa)):
            sas+=sa[i][0]
            sa0.append(sa[i])
            if sas>0.85:
                break
        print('sa0',sa0)
        fr=0
        for ej in sa0:
            fr+=data0[ej[1]]*ej[0]
        rank_s=fr.rank(ascending=False)
        data0['fr']=fr
        d2=data0[data0['RTYPE']=='2013-1']
        rank_s=d2['fr'].rank(ascending=False)
        d2['sr']=rank_s
        nam=d2[['SECCODE','fr','sr']].copy()
        ts=nam['SECCODE'].value_counts()
        klk=[(nam['sr'][nam['SECCODE']==i]).mean() for i in ts.index]
        pp=pd.Series(klk,index=ts.index)
        frank=pp.sort_values(ascending=False).copy()
        for v in zip(data0['SECCODE'].values,data0['fr'].values,data0['RTYPE'].values):
            cur.execute('''insert into tmp values ('%s',%s,'%s')''' % v)
        db.commit()
