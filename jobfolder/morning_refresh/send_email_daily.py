from send_email import sendemail
import datetime

def send_daily_email():

    title = 'Greet in the morning!'
    msg = """\
    <html>
      <head></head>
      <body>
        <p>Hi!<br><br>
           How are you?<br>
           Are you ready today?<br>
           Are you one step closer to your dream?<br>
           Here is the <a href="http://www.python.org">link</a> you wanted.<br><br>

           Thanks,<br>
           Yiming
        </p>
      </body>
    </html>
    """

    sendemail(title, msg,
              bcclst=['yiming@fcreekcapital.com',
                      ]
              )


if __name__ == '__main__':
    send_daily_email()

