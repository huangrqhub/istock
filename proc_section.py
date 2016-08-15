# -*- coding: utf-8 -*-
# write by hrq

from __future__ import division
import math
import numpy as np
import pandas as pd
import cx_Oracle
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class datawork(object):

    def __init__(self):

        self.db = cx_Oracle.connect('mc3', 'mc3', '192.168.1.119:1521/orcl')
        self.ocur = self.db.cursor()


    def insert_sec_data(self):
        #获得所有的报表时间
        sql = '''select distinct rtype from f_pro_t order by rtype asc'''
        #获得查询结果
        rows = self.ocur.execute(sql).fetchall()

        sql1 = '''select m.secname,m.seccode,p.rtype,median(p.roe) as m_roe,avg(p.roe) as a_roe,median(p.npr) as m_npr,avg(npr) as a_npr,
        median(p.gpr) as m_gpr,avg(p.gpr) as a_gpr,median(p.np) as m_np,avg(p.np) as a_np,median(p.eps) as m_eps,avg(p.eps) as a_eps,
        median(p.bi) as m_bi,avg(p.bi) as a_bi,median(p.bips) as m_bips,avg(p.bips) as a_bips from f_pro_t p,mv_sw3_5 m where p.stkcode=m.stkcode
        and p.rtype='%s' group by m.secname,m.seccode,p.rtype '''

        sql2 = '''select m.secname,m.seccode,c.rtype, median(c.cf_s) as m_cfs,avg(c.cf_s) as a_cfs,median(c.rr) as m_rr,avg(c.rr) as a_rr,
median(c.cf_nm) as m_cf_nm,avg(c.cf_nm) as a_cf_nm,median(c.cf_l) as m_cf_l,avg(c.cf_l) as a_cf_l,median(c.cr) as m_cr,avg(c.cr) as a_cr
from mv_sw3_5 m,f_cash_t c
where c.stkcode=m.stkcode and c.rtype='%s'
group by m.secname,m.seccode,c.rtype '''

        sql3='''select m.secname,m.seccode,d.rtype,median(d.cur) as m_cur,avg(d.cur) as a_cur,median(d.qr) as m_qr,avg(d.qr) as a_qr,median(d.car) as m_car,
avg(d.car) as a_car,median(d.ir) as m_ir,avg(d.ir) as a_ir,median(d.sr) as m_sr,avg(d.sr) as a_sr,median(d.ar) as m_ar,
avg(d.ar) as a_ar
from mv_sw3_5 m,f_deb_t d
where d.stkcode=m.stkcode and d.rtype='%s'
group by m.secname,m.seccode,d.rtype '''

        sql4 = '''select m.secname,m.seccode,f.rtype,median(f.mbrg) as m_mbrg,avg(f.mbrg) as a_mbrg,median(f.nprg) as m_nprg,avg(f.nprg) as a_nprg,median(f.nav) as m_nav,
avg(f.nav) as a_nav,median(f.targ) as m_targ,avg(f.targ) as a_targ,median(f.epsg) as m_epsg,avg(f.epsg) as a_epsg,median(f.seg) as m_seg,avg(f.seg) as
a_seg
from mv_sw3_5 m,f_gro_t f
where f.stkcode=m.stkcode and f.rtype='%s'
group by m.secname,m.seccode,f.rtype '''

        sql5 = '''select m.secname,m.seccode,a.rtype,median(a.eps_yoy) as m_eps_yoy,avg(a.eps_yoy) as a_eps_yoy,median(a.bvps) as m_byps,avg(a.bvps) as a_bvps,
median(a.epcf) as m_epcf,avg(a.epcf) as a_epcf,median(a.pro_yoy) as m_pro_yoy,avg(a.pro_yoy) as a_pro_yoy

from mv_sw3_5 m,f_rpt_t a
where m.stkcode=a.stkcode and a.rtype='%s'

group by m.secname,m.seccode,a.rtype '''

        sql = '''insert into F_SEC_FINANCE_LEV3(secname,seccode,rtype,m_roe,a_roe,m_npr,a_npr,m_gpr,a_gpr,m_np,a_np,m_eps,a_eps,m_bi,a_bi,m_bips,
a_bips,m_cf_s,a_cf_s,m_rr,a_rr,m_cf_nm,a_cf_nm,m_cf_l,a_cf_l,m_cr,a_cr,
m_cur,a_cur,m_qr,a_qr,m_car,a_car,m_ir,a_ir,m_sr,a_sr,m_ar,a_ar,m_mbrg,a_mbrg,
m_nprg,a_nprg,m_navg,a_navg,m_targ,a_targ,m_epsg,a_epsg,m_seg,a_seg,m_eps_yoy,a_eps_yoy,m_bvps,a_bvps,m_epcf,a_epcf,m_pro_yoy,a_pro_yoy)
 values ('%s','%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f'
 ,'%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f')'''
        #插入数据前先清空原来的数据
        self.ocur.execute("delete from F_SEC_FINANCE_LEV3")
        self.db.commit()
        #对查询的5个结果进行合并成为一个大表
        for i in rows:

            data1 = pd.read_sql(sql1 % (i[0],), self.db)
            data2 = pd.read_sql(sql2 %(i[0],),self.db)
            data3 = pd.read_sql(sql3 %(i[0],),self.db)
            data4 = pd.read_sql(sql4 % (i[0],), self.db)
            data5 = pd.read_sql(sql5 % (i[0],), self.db)
            dfa = pd.merge(data1,data2,on=['SECNAME','SECCODE','RTYPE'])
            dfb = pd.merge(dfa,data3,on=['SECNAME','SECCODE','RTYPE'])
            dfc = pd.merge(dfb, data4, on=['SECNAME', 'SECCODE', 'RTYPE'])
            dfd = pd.merge(dfc, data5, on=['SECNAME', 'SECCODE', 'RTYPE'])
            dfd = dfd.fillna(0)
            print dfd

            for x in xrange(0,len(dfd)):
                obj = dfd.iloc[x]

                # write into database
                self.ocur.execute(sql % (obj['SECNAME'], obj['SECCODE'], obj['RTYPE'], obj['M_ROE'], obj['A_ROE'], obj['M_NPR'],
       obj['A_NPR'], obj['M_GPR'], obj['A_GPR'], obj['M_NP'], obj['A_NP'], obj['M_EPS'], obj['A_EPS'],
       obj['M_BI'], obj['A_BI'], obj['M_BIPS'], obj['A_BIPS'], obj['M_CFS'], obj['A_CFS'], obj['M_RR'],
       obj['A_RR'], obj['M_CF_NM'], obj['A_CF_NM'], obj['M_CF_L'], obj['A_CF_L'], obj['M_CR'], obj['A_CR'],
       obj['M_CUR'], obj['A_CUR'], obj['M_QR'], obj['A_QR'], obj['M_CAR'], obj['A_CAR'], obj['M_IR'],
       obj['A_IR'], obj['M_SR'], obj['A_SR'], obj['M_AR'], obj['A_AR'], obj['M_MBRG'], obj['A_MBRG'],
       obj['M_NPRG'], obj['A_NPRG'], obj['M_NAV'], obj['A_NAV'], obj['M_TARG'], obj['A_TARG'],
       obj['M_EPSG'], obj['A_EPSG'], obj['M_SEG'], obj['A_SEG'], obj['M_EPS_YOY'], obj['A_EPS_YOY'],
       obj['M_BYPS'], obj['A_BVPS'], obj['M_EPCF'], obj['A_EPCF'], obj['M_PRO_YOY'], obj['A_PRO_YOY']))

            self.db.commit()

        print "inset data done!"

    #根据财报时间 和 类型 ，获得排序数据
    def getSecOrder(self,rtype,category):

        #按照 单个标准 来统计
        sql ='''select * from F_SEC_FINANCE_LEV3 t where t.rtype='%s' order by %s desc '''
        data1 = pd.read_sql(sql % (rtype,category,), self.db)
        data1['seq']=data1.index + 1
        data1['seqby'] = category
        return data1


    def insertSecSeq(self,secname,seccode,rtype,seq,seqby):

        sql = '''insert into F_SEC_SEQ_LV3 values('%s','%s','%s','%f','%s')'''
        self.ocur.execute(sql % (secname,seccode,rtype,seq,seqby,))
    #进行排序启动操作
    def doSecOrderThread(self):

        fieldlist = ['M_ROE','A_ROE','M_NPR','A_NPR','M_GPR','A_GPR','M_NP','A_NP','M_EPS','A_EPS','M_BI','A_BI','M_BIPS',
'A_BIPS','M_CF_S','A_CF_S','M_RR','A_RR','M_CF_NM','A_CF_NM','M_CF_L','A_CF_L','M_CR','A_CR',
'M_CUR','A_CUR','M_QR','A_QR','M_CAR','A_CAR','M_IR','A_IR','M_SR','A_SR','M_AR','A_AR','M_MBRG','A_MBRG',
'M_NPRG','A_NPRG','M_NAVG','A_NAVG','M_TARG','A_TARG','M_EPSG','A_EPSG','M_SEG','A_SEG','M_EPS_YOY','A_EPS_YOY','M_BVPS','A_BVPS','M_EPCF','A_EPCF','M_PRO_YOY','A_PRO_YOY']

        # 获得所有的报表时间
        sql = '''select distinct rtype from F_SEC_FINANCE_LEV3 order by rtype asc'''
        # 获得查询结果
        rows = self.ocur.execute(sql).fetchall()

        for row in rows:

            for field in fieldlist:

                df = self.getSecOrder(row[0],field)

                for x in xrange(0,len(df)):
                    obj = df.iloc[x]
                    self.insertSecSeq(obj['SECNAME'],obj['SECCODE'],obj['RTYPE'],obj['seq'],obj['seqby'])

            self.db.commit()
            
    #处理pro表的nan 数值
    def deal_pro_na(self):

        params=['ROE','NPR','GPR','NP','EPS','BI','BIPS']

        sql = '''select distinct rtype from f_pro_t order by rtype asc'''
        # 获得查询结果
        rtypes = self.ocur.execute(sql).fetchall()

        #query all nan values
        sql1 = '''select t.stkcode,t.name,t.roe,t.npr,t.gpr,t.np,t.eps,t.bi,t.bips,t.rtype,t2.secname,t2.seccode from

F_PRO_T t,mv_sw3_5 t2 where t.stkcode = t2.stkcode and t2.seccode = '%s' and t.rtype= '%s'

order by t.rtype desc '''
        #查询行业代码
        sql = '''select distinct seccode from mv_sw3_5 '''
        # 获得行业查询结果
        rows = self.ocur.execute(sql).fetchall()
        self.ocur.execute("delete from F_PRO_T_copy")
        self.db.commit()
        for type in rtypes:
            for i in rows:
                data1 = pd.read_sql(sql1 % (i[0],type[0]), self.db)

                if len(data1) < 8:
                    print 'secname less 8-----'
                    print data1[['SECNAME','SECCODE']]
                    continue

        #对空值填充
                df1 = data1.fillna(data1.median())
                df = df1.fillna(0)

                #去除极值
                for param in params:
                    md =df[param].median()
                    made = (abs(df[param] - df[param].median())).median() * 3 ** 1.438
                    df.loc[df[param] > (md + made), param] = md + made
                    df.loc[df[param] < (md - made), param] = md - made

                sql2 = '''insert into f_pro_t_copy(stkcode,name,roe,npr,gpr,np,eps,bi,bips,rtype) values ('%s','%s',
                '%f','%f','%f','%f','%f','%f','%f','%s') '''
                for x in xrange(0, len(df)):
                    obj = df.iloc[x]
                    self.ocur.execute(sql2 %(obj['STKCODE'],obj['NAME'],obj['ROE'],obj['NPR'],obj['GPR']
                    ,obj['NP'],obj['EPS'],obj['BI'],obj['BIPS'],obj['RTYPE']))
                self.db.commit()

    def avg_pro(self):
        params = ['ROE', 'NPR', 'GPR', 'NP', 'EPS', 'BI', 'BIPS']



        # query all nan values
        sql = '''select t.stkcode,t.name,t.roe,t.npr,t.gpr,t.np,t.eps,t.bi,t.bips,t.rtype from

        f_pro_t_copy t order by t.rtype asc '''
        df = pd.read_sql(sql , self.db)
        #取量纲
        for param in params:
            df.loc[:, param] = df[param] / df[param].mean()


        self.ocur.execute("delete from f_pro_t_copy")
        self.db.commit()

        sql2 = '''insert into f_pro_t_copy(stkcode,name,roe,npr,gpr,np,eps,bi,bips,rtype) values ('%s','%s',
            '%f','%f','%f','%f','%f','%f','%f','%s') '''
        for x in xrange(0, len(df)):
            obj = df.iloc[x]
            self.ocur.execute(sql2 % (obj['STKCODE'], obj['NAME'], obj['ROE'], obj['NPR'], obj['GPR']
                                      , obj['NP'], obj['EPS'], obj['BI'], obj['BIPS'], obj['RTYPE']))
            if x%2000 == 0:
                self.db.commit()
        self.db.commit()



#处理pro表的nan 数值
    def deal_gro_na(self):

        params=['MBRG','NPRG','NAV','TARG','EPSG','SEG']

        sql = '''select distinct rtype from F_GRO_T order by rtype asc'''
        # 获得查询结果
        rtypes = self.ocur.execute(sql).fetchall()

        #query all nan values
        sql1 = '''select t.stkcode,t.name,t.MBRG,t.NPRG,t.NAV,t.TARG,t.EPSG,t.SEG,t.rtype,t2.secname,t2.seccode from

F_GRO_T t,mv_sw3_5 t2 where t.stkcode = t2.stkcode and t2.seccode = '%s' and t.rtype= '%s'

order by t.rtype desc '''
        #查询行业代码
        sql = '''select distinct seccode from mv_sw3_5 '''
        # 获得行业查询结果
        rows = self.ocur.execute(sql).fetchall()
        self.ocur.execute("delete from F_GRO_T_copy")
        self.db.commit()
        for type in rtypes:
            for i in rows:
                data1 = pd.read_sql(sql1 % (i[0],type[0]), self.db)

                if len(data1) < 8:
                    print 'secname less 8-----'
                    print  data1[['SECNAME','SECCODE']]
                    continue
            #对空值填充

                df1 = data1.fillna(data1.median())
                df = df1.fillna(0)

                #去除极值
                for param in params:
                    md =df[param].median()
                    made = (abs(df[param] - df[param].median())).median() * 3 ** 1.438
                    df.loc[df[param] > (md + made), param] = md + made
                    df.loc[df[param] < (md - made), param] = md - made

                sql2 = '''insert into f_gro_t_copy(STKCODE,NAME,MBRG,NPRG,NAV,TARG,EPSG,SEG,RTYPE) values ('%s','%s',
                '%f','%f','%f','%f','%f','%f','%s') '''
                for x in xrange(0, len(df)):
                    obj = df.iloc[x]

                    self.ocur.execute(sql2 %(obj['STKCODE'],obj['NAME'],obj['MBRG'],obj['NPRG'],obj['NAV']
                    ,obj['TARG'],obj['EPSG'],obj['SEG'],obj['RTYPE']))
                self.db.commit()

    def avg_gro(self):
        params = ['MBRG', 'NPRG', 'NAV', 'TARG', 'EPSG', 'SEG']
        # query all nan values
        sql = '''select t.stkcode,t.name,t.MBRG,t.NPRG,t.NAV,t.TARG,t.EPSG,t.SEG,t.rtype from
        F_GRO_T_copy t order by t.rtype asc '''
        df = pd.read_sql(sql, self.db)

        # 取给每个字段取量纲
        for param in params:
            df.loc[:, param] = df[param] / df[param].mean()

        self.ocur.execute("delete from F_GRO_T_copy")
        self.db.commit()

        sql2 = '''insert into f_gro_t_copy(STKCODE,NAME,MBRG,NPRG,NAV,TARG,EPSG,SEG,RTYPE) values ('%s','%s',
            '%f','%f','%f','%f','%f','%f','%s') '''
        for x in xrange(0, len(df)):
            obj = df.iloc[x]
            self.ocur.execute(sql2 % (obj['STKCODE'], obj['NAME'], obj['MBRG'], obj['NPRG'], obj['NAV']
                                      , obj['TARG'], obj['EPSG'], obj['SEG'], obj['RTYPE']))
            if x % 2000 == 0:
                self.db.commit()
        self.db.commit()


        # 处理pro表的nan 数值
    def deal_deb_na(self):

            params = ['CUR','QR','CAR','IR','SR','AR']

            sql = '''select distinct rtype from f_deb_t order by rtype asc'''
            # 获得查询结果
            rtypes = self.ocur.execute(sql).fetchall()

            # query all nan values
            sql1 = '''select t.stkcode,t.name,t.cur,t.qr,t.car,t.ir,t.sr,t.ar,t.rtype,t2.secname,t2.seccode from

F_DEB_T t,mv_sw3_5 t2 where t.stkcode = t2.stkcode and t2.seccode = '%s' and t.rtype= '%s'

order by t.rtype desc '''
            # 查询行业代码
            sql = '''select distinct seccode from mv_sw3_5 '''
            # 获得行业查询结果
            rows = self.ocur.execute(sql).fetchall()
            self.ocur.execute("delete from F_DEB_T_copy")
            self.db.commit()
            for type in rtypes:
                for i in rows:
                    data1 = pd.read_sql(sql1 % (i[0], type[0]), self.db)

                    if len(data1) < 8:
                        print 'secname less 8-----'
                        print  data1[['SECNAME', 'SECCODE']]
                        continue
                        # 对空值填充

                    df1 = data1.fillna(data1.median())
                    df = df1.fillna(0)

                    # 去除极值
                    for param in params:
                        md = df[param].median()
                        made = (abs(df[param] - df[param].median())).median() * 3 ** 1.438
                        df.loc[df[param] > (md + made), param] = md + made
                        df.loc[df[param] < (md - made), param] = md - made

                    sql2 = '''insert into F_DEB_T_copy(STKCODE,NAME,CUR,QR,CAR,IR,SR,AR,RTYPE) values ('%s','%s',
                    '%f','%f','%f','%f','%f','%f','%s') '''
                    for x in xrange(0, len(df)):
                        obj = df.iloc[x]

                        self.ocur.execute(sql2 % (obj['STKCODE'], obj['NAME'], obj['CUR'], obj['QR'], obj['CAR']
                                                  , obj['IR'], obj['SR'], obj['AR'], obj['RTYPE']))
                    self.db.commit()


# 处理pro表的nan 数值
    def deal_opr_na(self):

            params = ['ARTO','ARTD','CAT','CAD']

            sql = '''select distinct rtype from f_opr_t order by rtype asc'''
            # 获得查询结果
            rtypes = self.ocur.execute(sql).fetchall()

            # query all nan values
            sql1 = '''select t.stkcode,t.name,t.ARTO,t.ARTD,t.CAT,t.CAD,t.rtype,t2.secname,t2.seccode from

f_opr_t t,mv_sw3_5 t2 where t.stkcode = t2.stkcode and t2.seccode = '%s' and t.rtype= '%s'

order by t.rtype desc '''
            # 查询行业代码
            sql = '''select distinct seccode from mv_sw3_5 '''
            # 获得行业查询结果
            rows = self.ocur.execute(sql).fetchall()
            self.ocur.execute("delete from F_OPR_T_copy")
            self.db.commit()
            for type in rtypes:
                for i in rows:
                    data1 = pd.read_sql(sql1 % (i[0], type[0]), self.db)

                    if len(data1) < 8:
                        print 'secname less 8-----'
                        print  data1[['SECNAME', 'SECCODE']]
                        continue
                        # 对空值填充

                    df1 = data1.fillna(data1.median())
                    df = df1.fillna(0)

                    # 去除极值
                    for param in params:
                        md = df[param].median()
                        made = (abs(df[param] - df[param].median())).median() * 3 ** 1.438
                        df.loc[df[param] > (md + made), param] = md + made
                        df.loc[df[param] < (md - made), param] = md - made

                    sql2 = '''insert into F_OPR_T_copy(STKCODE,NAME,ARTO,ARTD,CAT,CAD,RTYPE) values ('%s','%s',
                    '%f','%f','%f','%f','%s') '''
                    for x in xrange(0, len(df)):
                        obj = df.iloc[x]

                        self.ocur.execute(sql2 % (obj['STKCODE'], obj['NAME'], obj['ARTO'], obj['ARTD'], obj['CAT']
                                                  , obj['CAD'], obj['RTYPE']))
                    self.db.commit()


    def avg_deb(self):

        params = ['ARTO', 'ARTD', 'CAT', 'CAD']

        # query all nan values

        sql = '''select * from F_OPR_T_copy t order by t.rtype asc '''

        df = pd.read_sql(sql, self.db)

        # 取给每个字段取量纲
        for param in params:
            df.loc[:, param] = df[param] / df[param].mean()

        self.ocur.execute("truncate table F_OPR_T_copy")

        sql2 = '''insert into F_OPR_T_copy(STKCODE,NAME,ARTO,ARTD,CAT,CAD,RTYPE) values ('%s','%s',
            '%f','%f','%f','%f','%s') '''

        for x in xrange(0, len(df)):
            obj = df.iloc[x]

            self.ocur.execute(sql2 % (obj['STKCODE'], obj['NAME'], obj['ARTO'], obj['ARTD'], obj['CAT']
                                      , obj['CAD'], obj['RTYPE']))
            if x % 2000 == 0:
                self.db.commit()

        self.db.commit()



    def avg_opr(self):

            params = ['CUR', 'QR', 'CAR', 'IR', 'SR', 'AR']

            # query all nan values

            sql = '''select * from F_deb_T_copy t order by t.rtype asc '''

            df = pd.read_sql(sql, self.db)

            # 取给每个字段取量纲
            for param in params:
                df.loc[:, param] = df[param] / df[param].mean()

            self.ocur.execute("truncate table F_deb_T_copy")

            sql2 = '''insert into F_DEB_T_copy(STKCODE,NAME,CUR,QR,CAR,IR,SR,AR,RTYPE) values ('%s','%s',
                    '%f','%f','%f','%f','%f','%f','%s') '''
            for x in xrange(0, len(df)):
                obj = df.iloc[x]

                self.ocur.execute(sql2 % (obj['STKCODE'], obj['NAME'], obj['CUR'], obj['QR'], obj['CAR']
                                          , obj['IR'], obj['SR'], obj['AR'], obj['RTYPE']))
                if x % 2000 == 0:
                    self.db.commit()
            self.db.commit()



if __name__ == "__main__":

    dw = datawork()
    #dw.insert_sec_data()
    # dw.deal_pro_na()
    # dw.avg_pro()
    # dw.deal_gro_na()
    # dw.avg_gro()
    #dw.deal_deb_na()
    #dw.deal_opr_na()
    dw.avg_deb()
    dw.avg_pro()
    #

    #dw.getSecOrder('2015-1','M_ROE')
    #排名统计


