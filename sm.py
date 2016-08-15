# -*- coding: utf-8 -*-
import xlwt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import cx_Oracle

def getExcel(data):
    workbook = xlwt.Workbook(encoding='utf-8')
    booksheet = workbook.add_sheet('Sheet 1', cell_overwrite_ok=True)
    
    for i, row in enumerate(data):
        for j, col in enumerate(row):
            booksheet.write(i, j+1, col)
    booksheet.col(0).width=50
    workbook.save('stks.xls')


def sendMail(data):
    #创建一个带附件的实例
    getExcel(data)

    msg = MIMEMultipart()    
    #构造附件1
    att1 = MIMEText(open('./stks.xls', 'rb').read(), 'base64', 'utf8')
    att1["Content-Type"] = 'application/octet-stream'
    att1["Content-Disposition"] = 'attachment; filename="股票.xls"'#这里的filename可以任意写，写什么名字，邮件中显示什么名字
    msg.attach(att1)  
    
    #加邮件头
    usrs = ['lwang@biz-wiz.com.cn','hszhang@biz-wiz.com.cn','jyliu@biz-wiz.com.cn','xrliu@biz-wiz.com.cn']
    # msg['to'] = 'xrliu@biz-wiz.com.cn;lwang@biz-wiz.com.cn;hszhang@biz-wiz.com.cn;jyliu@biz-wiz.com.cn'
    msg['from'] = 'market@biz-wiz.com.cn'
    msg['subject'] = '股票清单'
    #发送邮件
    try:
        server = smtplib.SMTP()
        server.connect('smtp.biz-wiz.com.cn')
        server.login('market@biz-wiz.com.cn','mm&123qwe')#XXX为用户名，XXXXX为密码
        for u in usrs:
            server.sendmail(msg['from'], u ,msg.as_string())
        server.quit()
        print '发送成功'
    except Exception, e:  
        print str(e)

def sm():
    db=cx_Oracle.connect('mc4','mc4998','192.168.0.18:1521/racdb')
    cur = db.cursor()    

    sql = '''select to_char(max(trdate),'yyyymmdd'),to_char(sysdate,'yyyymmdd') from stk_mkt_ex_t'''
    y = cur.execute(sql).fetchone()
    if y[0]<>y[1]: exit()

    sql = '''
      select stkcode,to_char(trdate,'mm/dd') trdate
        from bs_t 
        where trdate=to_date('%s','yyyymmdd') and status=1 and stkcode in (select stkcode from stk_good_t)
        '''
    rows = cur.execute(sql % (y[0],)).fetchall()
    sendMail(rows)
