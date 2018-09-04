import sys
import MySQLdb
import numpy as np
import pandas as pd
import datetime as dt
import pytz


def del_log_data_from_deleted_jc(readconnectioninfo, writeconnectioninfo):
    readdb = MySQLdb.connect(host=readconnectioninfo[0],
                             port=readconnectioninfo[1],
                             user=readconnectioninfo[2],
                             passwd=readconnectioninfo[3],
                             db=readconnectioninfo[4])
    writedb = MySQLdb.connect(host=writeconnectioninfo[0],
                              port=writeconnectioninfo[1],
                              user=writeconnectioninfo[2],
                              passwd=writeconnectioninfo[3],
                              db=writeconnectioninfo[4])
    readcursor = readdb.cursor()

    selejobidquery = "SELECT id FROM schedulerDB.jobchain_table;"
    readcursor.execute(selejobidquery)
    restemp = readcursor.fetchall()
    if len(restemp) == 0:
        return
    instr = ', '.join([str(tt) for tt in np.array(restemp).flatten()])
    readcursor.close()
    readdb.close()

    writecursor = writedb.cursor()
    query = "DELETE FROM schedulerDB.jobchain_lst_run WHERE jobchain_id NOT IN ({})".format(instr)
    writecursor.execute(query)
    writedb.commit()
    writecursor.close()
    writedb.close


def del_outdate_log(readconnectioninfo, writeconnectioninfo):
    readdb = MySQLdb.connect(host=readconnectioninfo[0],
                             port=readconnectioninfo[1],
                             user=readconnectioninfo[2],
                             passwd=readconnectioninfo[3],
                             db=readconnectioninfo[4])
    writedb = MySQLdb.connect(host=writeconnectioninfo[0],
                              port=writeconnectioninfo[1],
                              user=writeconnectioninfo[2],
                              passwd=writeconnectioninfo[3],
                              db=writeconnectioninfo[4])
    readcursor = readdb.cursor()
    timenow = dt.datetime \
        .now(pytz.timezone('America/Chicago')) \
        .replace(tzinfo=None, microsecond=0)
    # five_min_ago_str = (timenow - dt.timedelta(seconds=10 * 60)).strftime('%Y-%m-%d %H:%M:%S')
    six_hours_ago_str = (timenow - dt.timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S')
    selejobidquery = "SELECT id FROM schedulerDB.jobchain_table WHERE tag='scheduler';"
    readcursor.execute(selejobidquery)
    tempans = readcursor.fetchall()
    if len(tempans) == 0:
        return
    idlst = np.array(tempans).flatten()
    readcursor.close()
    readdb.close()
    idlststr = ', '.join([str(tt) for tt in idlst])
    writecursor = writedb.cursor()
    query = "DELETE FROM schedulerDB.jobchain_lst_run WHERE jobchain_id in ({}) " \
            "AND last_run_start < \'{}\'".format(idlststr, six_hours_ago_str)
    writecursor.execute(query)
    writedb.commit()
    writecursor.close()
    writedb.close()


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

    if bkp_flag == 1:  # bkp ping bkp
        readconnectioninfo = ["35.224.121.101", 3306, "yiming", "yiming", "schedulerDB"]
        writeconnectioninfo = ["104.197.118.95", 3306, "yiming", "yiming", "schedulerDB"]
        bkpstr = 'bkp'
    else:  # main ping main
        readconnectioninfo = ["35.224.121.101", 3306, "yiming", "yiming", "schedulerDB"]
        writeconnectioninfo = ["35.224.121.101", 3306, "yiming", "yiming", "schedulerDB"]
        bkpstr = 'main'
    del_outdate_log(readconnectioninfo, writeconnectioninfo)
    del_log_data_from_deleted_jc(readconnectioninfo, writeconnectioninfo)
