import os
import time
import select
import psycopg2
import psycopg2.extensions
import json
from config import db_config
#import syslog
from subprocess import Popen

if __name__== '__main__':
    try:
        # Connect to the database
        conn = psycopg2.connect(" ".join("{0}='{1}'".format(k,v) for k,v in db_config.iteritems()))
        # Create cursor object with results to be returned in dictionary format
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Attempt to obtain an advisory lock, exit if a lock is already held.
        cur.execute("SELECT pg_try_advisory_lock(1010101010101)")
        if not cur.fetchone()[0]:
            raise SystemExit(0)
        sqlstr="""select * from task_bots.execute_python where schedule_time <=now() and (completed=False or completed is null)"""
        cur.execute(sqlstr)
        rows=cur.fetchall()
        for row in rows:
            try:
                run_cmd ='python27 %s "%s" %s'%(row[1],row[2],row[0]) 
                Popen(run_cmd, shell=True).wait()
                
                #os.system("python %1 %2"%(row[1],row[2]))
            except Exception , e:
                print e
        
    except Exception, e:
        print e
    
    finally:
        #Release advisory lock, commit any pending transaction, close the database connection
        cur.execute("SELECT pg_advisory_unlock(1010101010101)")
        conn.commit()
        conn.close()