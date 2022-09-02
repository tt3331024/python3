# -*- coding: utf-8 -*-
"""Send mail module.

Created on 2021/03/26 by Jerry.Ko
"""
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.header import Header


def connect_smtp(sender, receivers, msg):
    """連接 gmail smtp
    """
    try:
        # Gmail Sign In
        gmail_sender = sender
        gmail_passwd = 'MY_TOKEN'  # 要至官網申請應用程式密碼(https://www.learncodewithmike.com/2020/02/python-email.html)
        # gmail_passwd = 'MY_PASSWORD+' # 需至官網打開低安全性存取權限(https://medium.com/views-from-bi-pm/%E5%88%86%E4%BA%AB-%E5%A6%82%E4%BD%95%E7%94%A8-python-%E5%AF%84%E4%BF%A1-python-x-email-%E5%B7%A5%E5%85%B7-smtplib-email-6ff4936ce55e)

        smtp = smtplib.SMTP('smtp.gmail.com:587')
        smtp.ehlo()
        smtp.starttls()
        smtp.login(gmail_sender, gmail_passwd)
        smtp.sendmail(sender, receivers, msg.as_string())
        smtp.quit()
        print('send successfully')
    except:
        print('unable to send email')

def connect_smtp_outlook(sender, receivers, msg):
    """連接 outlook smtp
    """
    try:
        # outlook Sign In
        outlook_sender = sender
        outlook_passwd = 'MY_PASSWORD'  # sender用戶的信箱密碼

        smtp = smtplib.SMTP('smtp.office365.com:587')
        smtp.ehlo()
        smtp.starttls()
        smtp.login(outlook_sender, outlook_passwd)
        smtp.sendmail(sender, receivers, msg.as_string())
        smtp.quit()
        print('send successfully')
    except:
        print('unable to send email')
        
def attach_files(filepaths, msg):
    """夾帶檔案的function
    """
    for file in filepaths:
        with open(file, 'rb') as fp:
            print ('can read faile')
            part = MIMEApplication(fp.read())
        part.add_header('Content-Disposition', 'attachment', filename=file.split("/")[-1])
        msg.attach(part)

    return msg


def send_mail(receivers_list, subject, body, files=None, sender):
    """寄送 Email 的 function.
    Args:
      receivers_list: list of str. receivers of mail。
      subject: str. subject of mail。
      body: str. contain of mail。
      files: list of str. attachment files.
      sender: str. Sender of mail, default is hotaiconnected.data@gmail.com. 
    Return:
      None
    """
    msg = MIMEMultipart()
    msg['From'] = Header("寄件人名稱", 'utf-8') # sender
    msg['To'] = COMMASPACE.join(receivers_list)
    msg['Subject'] = subject
    # attach the body with the msg instance
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    if files is not None:
        msg = attach_files(files, msg)
    
    # log in smtp and send out email
    if 'gmail.com' in sender.split('@')[1]:
        connect_smtp(sender, receivers_list, msg)
