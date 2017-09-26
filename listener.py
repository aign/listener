#Copyright Time at Task Aps 2014. All rights reserved. David Andersen owns all the code you create for this project.
# 
# The purpose of this project is to be able to send emails from a postgres database. 
# To utilize this solution all one needs to do is to add a row to a postgres table (named "actions"), this 
# row will contain the email address of the receiver, the subject, content etc.
# To actually send the emails we use ultradox.com and we execute a "template" there by visiting its "run" url and passing a
# JSON document with all the parameters
#
# This file (emailer.py) will look for unprocessed rows is the "actions" table and execute these actions.
# You can test this solution by doing the following:
# 1. Use SSH to log onto our test-server, using the credentials found in the middle of this document: 
#    https://docs.google.com/document/d/1biEViRLFwlG7GnO_NHYa6m_U0YiUKU9oNpRoCcO4xak/edit
# 2. Go to the directory named /root/py_emailer
# 3. Write python listener.py to execute this file. This file will run continuously and wait for postgres to notify it of
#    a new even
# 4. Connect to the postgres database using for example PgAdmin III using the credentials found around line 30 in this file.
# 5. Run the SQL statement found in https://github.com/OO-developers/Automatic-emailing/blob/master/sql/test_data.sql 
#    to add a new action to the actions table. In that statement, please replace the example email address with your own.
# 6. Run the SQL statement "notify emailer;" to notify the python program
# 6. Wait for an email to arrive in your inbox.

#This is the listener component of the Automatic Emailing service
#It listens to any notifications sent by Postgre on a speicified channel and invokes the emailer.py script
#Any error messages are logged to syslog
#Startup, Shutdown and invoking of a duplicate instances are logged to syslog

import os
import time
import select
import psycopg2
import psycopg2.extensions
import json
from config import db_config
#import syslog
from subprocess import Popen
from logging.config import listen

db_config_local_test = {
 "host": "localhost",
 "port": "5432",
 "dbname": "sample",
}

if "DEBUG" in os.environ:
    print ("Using local DB configuration")
    db_config = db_config_local_test

#This is the channels names to which notifications will be sent from Postgre
channel = "emailer"
fetch_channel = "fetcher"
bot_channel = "bot_manager"
scheduler_channel="bot_scheduler"
execute_python_channel="execute_python_script"

DSN = " ".join("{0}='{1}'".format(k,v) for k,v in db_config.iteritems())
#function that executes bot script, later will be moved to separate file 'execute_bot.py'

while True:
    try:
        #syslog.syslog('Automatic emailer listener starting.')
        connection_reseted = False
        #Connect to database and set isolation level to autocommit, this is required to be able to LISTEN to Postgres notifications
        conn = psycopg2.connect(DSN)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        bot_cur = conn.cursor()
        curs.execute("SELECT pg_advisory_unlock(1234567890);")
        #Attempt to obtain an advisory lock to prevent duplicate processing
        curs.execute('SELECT pg_try_advisory_lock(1234567890);')
        if not curs.fetchone()[0]:
            #syslog.syslog('Automatic emailer instance already running. Exiting this instance.')
            raise SystemExit(0)
    
        curs.execute("LISTEN {0};".format(channel))
        curs.execute("LISTEN {0}".format(fetch_channel))
        curs.execute("LISTEN {0};".format(bot_channel))
        curs.execute("LISTEN {0}".format(scheduler_channel))
        curs.execute("LISTEN {0}".format(execute_python_channel))
        print( "listen to {0},{1},{2},{3},{4}".format(channel,fetch_channel,bot_channel,scheduler_channel,execute_python_channel))
        #syslog.syslog('Automatic emailer/fetcher listener started.')

        #Listen for notifcations from Postgres and execute the emailer script or bot script or scheduler script in reponse, 
        # or quit if the 'quit' command is sent via payload
        while 1:
            if not select.select([conn],[],[],5) == ([],[],[]):
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop()
                    if notify.payload.lower() == 'quit':
                        raise SystemExit(0)
                    elif notify.channel == channel:
                        #if emailer.py has already obtained a lock, then don't invoke the script
                        curs.execute("SELECT granted FROM pg_locks WHERE locktype='advisory' AND objid='987654321';")
                        locks = curs.fetchone()
                        if not locks or not locks[0]:
                            print ("Emailer Invoked")
                            #Invoke emailer script
                            Popen(["python27", "emailer.py"])
                    elif notify.channel == fetch_channel:
                        # fetcher.py handles locks on its own
                        print ("Fetcher invoked")
                        os.system("python27 fetcher.py")
                    elif notify.channel == bot_channel:
                        #if notification is on a bot channel
                        print ("bot Invoked")
                        # print("bot_manager Invoked")
                        # print(notify.payload.lower())
                        # json_notify = json.loads(notify.payload)
                        # execute_bot(json_notify)
                        if notify.payload != '':
                            json_notify = json.loads(notify.payload)
                            os.system("python27 execute_bot.py %s %s"%(json_notify['id'],json_notify['worker_initials']))
                        
                    elif notify.channel == scheduler_channel:
                        #if notification is on a bot scheduler channel
                        pass
                    elif notify.channel == execute_python_channel:
                        print ("Execute_python_script Invoked")
                        #if notification is on a bot scheduler channel
                        os.system("python27 execute_python_scripts.py")
                        pass
                    

    except Exception, e:
        #syslog.syslog(syslog.LOG_ERR,str(e))
        print "ERROR: ", str(e)
        #If connection to database reseted, reconnect and continue
        if type(e) is psycopg2.OperationalError:
            connection_reseted = True
    finally:
        if not connection_reseted:
            #Release advisory lock, unlisten for notifications, close the database connection
            curs.execute("SELECT pg_advisory_unlock(1234567890);")
            curs.execute("UNLISTEN {0};".format(channel))
            curs.execute("UNLISTEN {0}".format(fetch_channel))
            conn.close()
            print "shutdown"
            #syslog.syslog('Automatic emailer listener shutdown.')

    if not connection_reseted:
        break
    time.sleep( 5 )


# vi: ts=4:sw=4:et: