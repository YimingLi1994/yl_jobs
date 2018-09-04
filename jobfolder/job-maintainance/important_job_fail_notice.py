import MySQLdb
import sys
import numpy as np
import datetime as dt
import pytz
import sendemail


def job_check(readconnectioninfo, writeconnectioninfo, bkpstr):
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
    minutenow = dt.datetime \
        .now(pytz.timezone('America/Chicago')) \
        .replace(tzinfo=None, second=0, microsecond=0)
    minuteago = minutenow - dt.timedelta(seconds=59)
    #minuteago = minutenow - dt.timedelta(hours=100)
    try:
        readcursor = readdb.cursor()
        writecursor = writedb.cursor()
        typeflag = 'main_flag'
        mailtitle='_main'
        if bkpstr == 'bkp':
            typeflag = 'BKP_flag'
            mailtitle='_BKP'

        query = "SELECT id, job_desc FROM schedulerDB.jobchain_table " \
                " WHERE tag='{}' AND switch='{}' AND {}=1;".format('important', 'ON', typeflag)
        readcursor.execute(query)
        tempans = readcursor.fetchall()
        if len(tempans) == 0:
            pass
        else:
            idlst = np.array(tempans)
            idstr = ', '.join(idlst[:, 0].astype(str))
            query = "SELECT jobchain_id, status, id FROM schedulerDB.jobchain_lst_run " \
                    " WHERE jobchain_id in ({}) AND DATE_FORMAT(last_run_end,\'%Y-%m-%d %H:%i:%s\') " \
                    "BETWEEN \'{}\' AND \'{}\';".format(idstr,
                                                        minuteago.strftime('%Y-%m-%d %H:%M:%S'),
                                                        minutenow.strftime('%Y-%m-%d %H:%M:%S'))
            writecursor.execute(query)
            tempans = writecursor.fetchall()
            failedidlst = []
            failedshowidlst = []
            if len(tempans) != 0:
                statuslst = np.array(tempans)
                for idx in range(statuslst.shape[0]):
                    if statuslst[idx, 1] == 'Failed':
                        failedidlst.append(str(statuslst[idx, 0]))
                        failedshowidlst.append(str(statuslst[idx, 2]))
                if len(failedidlst) != 0:
                    query = "SELECT run_id, msg FROM schedulerDB.schedulerMessage " \
                            "WHERE run_id in ({})".format(', '.join(failedshowidlst))
                    writecursor.execute(query)
                    retnp = np.array(writecursor.fetchall())
                    desc_msg_lst = []
                    mailstr = 'ATTENTION \n\n\n\n'
                    for idx in range(retnp.shape[0]):
                        if retnp[idx, 0] != None:
                            desc_msg_tpl = \
                                idlst[np.where((idlst[:, 0] == \
                                                failedidlst[failedshowidlst.index(retnp[idx, 0])]))[0][0], 1], \
                                retnp[idx, 1]
                            desc_msg_lst.append(desc_msg_tpl)
                            mailstr += 'jobchainId: {}'.format(desc_msg_tpl[0]) + ':\n' + desc_msg_tpl[1] + '\n\n'
                    sendemail.sendemail('Job with important tag failed {}'.format(mailtitle), mailstr,
                                        [
                                         'yiming.li2@searshc.com',
                                         ])

    finally:
        readdb.close()
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
        writeconnectioninfo = ['127.0.0.1', 3306, "root", "2212", "schedulerDB"]
        readconnectioninfo = ['35.184.121.179', 3306, "root", "openthegate", "schedulerDB"]
        bkpstr = 'bkp'
    else:  # main ping main
        writeconnectioninfo = ['35.188.14.115', 3306, "root", "openthegate", "schedulerDB"]
        readconnectioninfo = ['35.188.14.115', 3306, "root", "openthegate", "schedulerDB"]
        bkpstr = 'main'
    job_check(readconnectioninfo, writeconnectioninfo, bkpstr)
