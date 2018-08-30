import smtplib
import email
import smtplib, email
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os
from email.utils import COMMASPACE, formatdate
import pandas as pd

def sendemail(titlestr, msgstr, receverlst=None, cclst=None, bcclst=None, attachmentlst=None, pic_attachmentlst=None):
    """

    :param titlestr:
    :param msgstr:
    :param receverlst:
    :param attachmentlst: [(DataFrame,name),(DataFrame,name),(DataFrame,name)]
    :param pic_attachmentlst: [(binary, name), (binary, name)]
    :return:
    """
    receverlst = [] if receverlst is None else receverlst
    cclst = [] if cclst is None else cclst
    bcclst = [] if bcclst is None else bcclst
    attachmentlst = [] if attachmentlst is None else attachmentlst
    pic_attachmentlst = [] if pic_attachmentlst is None else pic_attachmentlst

    msg = MIMEMultipart()
    msg['From'] = "DPJOBSCHEDULER@gmail.com"
    msg['Date'] = formatdate(localtime=True)
    body = MIMEText(msgstr, 'html')
    msg.attach(body)
    for eachattachment in attachmentlst:
        attachment = MIMEText(eachattachment[0])
        attachment.add_header("Content-Disposition", "attachment", filename=eachattachment[1])
        msg.attach(attachment)

    for eachpic in pic_attachmentlst:
        with open(eachpic[0], 'rb') as fp:
            img = MIMEImage(fp.read())
        img.add_header("Content-Disposition", "inline", filename=eachpic[1])
        msg.attach(img)

    # msg = email.message_from_string(msgstr)
    # msg['From'] = "DPJOBSCHEDULER@gmail.com"
    totallst = receverlst.copy()
    totallst.extend(cclst)
    totallst.extend(bcclst)
    if len(receverlst) != 0:
        msg['To'] = ",".join(receverlst)
    msg['cc'] = ', '.join(cclst)
    msg['Subject'] = titlestr
    s = smtplib.SMTP("smtp.googlemail.com", 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login('DPJOBSCHEDULER@gmail.com', 'sears123$')
    s.sendmail(msg["From"], totallst, msg.as_string())
    s.quit()


if __name__ == '__main__':
    test = pd.DataFrame([1,2])
    sendemail('Test', "maring rate",
              bcclst=['yiming.li2@searshc.com'],
              attachmentlst=[(test.to_csv(index=False), 'test1.csv'),]
              )
