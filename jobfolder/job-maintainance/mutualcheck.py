import MySQLdb
import sys
import datetime as dt
import pytz
import sendemail as se


def timecheck( lst_ping_time):
    timenow = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
    if timenow - lst_ping_time > dt.timedelta(minutes=30):
        return False
    else:
        return True


def bkpcheckmain():
    connectioninfo = ["35.224.121.101", 3306, "yiming", "yiming", "schedulerDB"] # main mysql
    sqlstr = "SELECT debugmode, job_last_ping_time FROM schedulerDB.realtime_status WHERE name='main';"
    db = MySQLdb.connect(host=connectioninfo[0],
                         port=connectioninfo[1],
                         user=connectioninfo[2],
                         passwd=connectioninfo[3],
                         db=connectioninfo[4])
    cursor = db.cursor()
    cursor.execute(sqlstr)
    retlst = cursor.fetchall()[0]
    if retlst[0] == 'debug': # if its debug model we dont send email, which stop sending soooo many emails
        return
    if timecheck(retlst[1]) is False:
        se.sendemail("error!","Hi man, Main scheduler is out of order!",
                     ['yiming.li2@searshc.com',
                      ])
        ####Turn to debug
        cursor.execute(
            "UPDATE schedulerDB.realtime_status SET debugmode='debug' WHERE name='main';"
        )
        db.commit()
    cursor.close()
    db.close()


def maincheckbkp():
    connectioninfo = ["104.197.118.95", 3306, "yiming", "yiming", "schedulerDB"]
    sqlstr = "SELECT debugmode,  job_last_ping_time FROM schedulerDB.realtime_status WHERE name='bkp';"
    db = MySQLdb.connect(host=connectioninfo[0],
                         port=connectioninfo[1],
                         user=connectioninfo[2],
                         passwd=connectioninfo[3],
                         db=connectioninfo[4])
    cursor = db.cursor()
    cursor.execute(sqlstr)
    retlst = cursor.fetchall()[0]
    if retlst[0] == 'debug':
        return
    if timecheck(retlst[1]) is False:
        se.sendemail("error!","Hi man, Bkp scheduler is out of order!",
                     ['yiming.li2@searshc.com',
                      ])
        ####Turn to debug
        cursor.execute(
            "UPDATE schedulerDB.realtime_status SET debugmode='debug' WHERE name='bkp';")
        db.commit()
    cursor.close()
    db.close()


if __name__ == '__main__':
    pathlst = sys.argv[0].split('/')
    if len(pathlst) == 1:
        pathbase = '.'
    else:
        pathbase = '/'.join(sys.argv[0].split('/')[:-1])
    bkp_flag = 0
    if len(sys.argv) == 2:
        if sys.argv[1].lower() == 'bkp':
            bkp_flag = 1
        else:
            raise ValueError('Unknow parameter {}'.format(sys.argv[1]))
    if bkp_flag == 1:
        bkpcheckmain()
    else:
        maincheckbkp()
