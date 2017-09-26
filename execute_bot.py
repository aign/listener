import os
import time
import select
import psycopg2
import psycopg2.extensions
import json
#import syslog
from config import db_config
from subprocess import Popen


def execute_bot(data):
    DSN = " ".join("{0}='{1}'".format(k,v) for k,v in db_config.iteritems())
    conn = psycopg2.connect(DSN)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    bot_cur = conn.cursor()

    sqlstr = 'Select * from task_bots.bots where name = '+'\''+data['worker_initials']+'\''
    print(sqlstr)
    bot_cur.execute(sqlstr)
    rows = bot_cur.fetchall()
    for bot_row in rows:
        procname = bot_row[3]
        if bot_row[2] == 1:
            print('execute sql script')
            bot_cur.execute("update task_manager.tasks set task_status="+"'accepted'"+' where id ='+str(data['id']))
            bot_cur.execute('select '+procname+'('+str(data['id'])+')')
            print(bot_cur.query)
        elif bot_row[2] == 2:
            pass
        