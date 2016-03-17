#!/usr/bin/env python


import os, sys, email, smtplib, hashlib
                        
SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587
#set by yourself
SMTP_USER = 'licheng5625@gmail.com'
TO_ADDR   = 'licheng5625@gmail.com'
SMTP_PASS='3753042l'

def sendmail(Subject,Strings,attachment):
    msg = email.MIMEMultipart.MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = TO_ADDR
    msg['Date'] = email.Utils.formatdate()
    msg['Subject'] = Subject
    msg.attach(email.MIMEText.MIMEText(Strings))
    if attachment!='':
        data = open(attachment, 'rb').read()
        sha1 = hashlib.sha1(data).hexdigest()
        base = os.path.basename(attachment)
        print ('send %s as attschment (%s)' % (base, sha1))
        part = email.MIMEBase.MIMEBase('application', 'octet-stream')
        part.set_payload(data)
        email.Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % base)
        msg.attach(part)

    smtp = smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT)
    if SMTP_USER and SMTP_PASS:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
    smtp.sendmail(SMTP_USER, TO_ADDR, msg.as_string())
    smtp.close()
