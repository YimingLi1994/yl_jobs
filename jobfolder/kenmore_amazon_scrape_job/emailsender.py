import smtplib, email
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from email.utils import COMMASPACE, formatdate


def sendemail(titlestr, msgstr, receverlst=[], cclst=[], bcclst=[], attachmentlst=[]):
    """
    
    :param titlestr: 
    :param msgstr: 
    :param receverlst: 
    :param attachmentlst: [(DataFrame,name),(DataFrame,name),(DataFrame,name)] 
    :return: 
    """

    msg = MIMEMultipart()
    msg['From'] = "ylscheduleremail@gmail.com"
    msg['Date'] = formatdate(localtime=True)
    body = MIMEText(msgstr, 'html')
    msg.attach(body)
    for eachattachment in attachmentlst:
        attachment = MIMEText(eachattachment[0])
        attachment.add_header("Content-Disposition", "attachment", filename=eachattachment[1])
        msg.attach(attachment)

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
    s.login('ylscheduleremail@gmail.com', 'scheduler')
    s.sendmail(msg["From"], totallst, msg.as_string())
    s.quit()

if __name__=='__main__':
    sendemail("title", 'body',['yiming.li2@searshc.com'])