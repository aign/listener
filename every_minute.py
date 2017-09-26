#Copyright Time at Task Aps 2014. All rights reserved. David Andersen owns all the code you create for this project.
# 
import os
import select
import psycopg2
import psycopg2.extensions
#import syslog
from time import sleep
from subprocess import Popen
from config import db_config


DSN = " ".join("{0}='{1}'".format(k,v) for k,v in db_config.iteritems())
        #Connect to database and set isolation level to autocommit, this is required to be able to LISTEN to Postgres notifications
conn = psycopg2.connect(DSN)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
curs = conn.cursor()
while True:
    try:
        print('executing')
        curs.execute('SELECT cron.every_minute()')
        print conn.notices
        del conn.notices[:]
        sleep(10)
    except Exception, e:
#        syslog.syslog(syslog.LOG_ERR,str(e))
        print "ERROR: ", str(e)   
conn.close()
