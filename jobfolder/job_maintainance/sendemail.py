import smtplib
import email


def sendemail(titlestr, msgstr, receverlst):
    msg = email.message_from_string(msgstr)
    msg['From'] = "dpscheduler0@dpscheduler.com"
    msg['To'] = ",".join(receverlst)
    msg['Subject'] = titlestr
    s = smtplib.SMTP("smtp.googlemail.com", 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login('jianwei.xiao0@gmail.com', 'iiooppujxiao0')
    s.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    s.quit()
