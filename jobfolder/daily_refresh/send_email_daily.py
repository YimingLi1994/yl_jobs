import pandas as pd
import datetime as dt, pytz
import numpy as np
from bigquery_run_with_timeout_retry.run_bigquery import read_gbq, to_gbq
from bigquery_run_with_timeout_retry.run_bigquery import runbq
from send_email import sendemail
import datetime

def send_daily_email():

    title = 'next_week_margin_rate'
    msg = 'Hi Rancy,\n\n' \
          'Here are the margin rates for next week, both sears and kmart.\n\n' \
          'Thanks,\n' \
          'Yiming'

    sendemail(title, msg,
              bcclst=['yl3573@columbia.edu',
                      ]
              )


if __name__ == '__main__':
    send_daily_email()

