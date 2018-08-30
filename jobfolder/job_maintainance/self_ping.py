import MySQLdb
import sys


def ping_mysql(connectioninfo, bkpstr):
    db = MySQLdb.connect(host=connectioninfo[0],
                         port=connectioninfo[1],
                         user=connectioninfo[2],
                         passwd=connectioninfo[3],
                         db=connectioninfo[4])
    cursor = db.cursor()
    query = "UPDATE schedulerDB.realtime_status SET job_last_ping_time=NOW() WHERE name='{}';".format(bkpstr)
    cursor.execute(query)
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

    if bkp_flag == 1: #bkp ping bkp
        connectioninfo = ["104.197.118.95", 3306, "yiming", "yiming", "schedulerDB"]
        bkpstr='bkp'
    else:# main ping main
        connectioninfo = ["35.224.121.101", 3306, "yiming", "yiming", "schedulerDB"]
        bkpstr='main'
    ping_mysql(connectioninfo,bkpstr)