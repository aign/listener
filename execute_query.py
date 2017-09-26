import os
import sys
import time
import select
import psycopg2
import psycopg2.extensions
import json
from config import db_config


def main(argv):
    print(argv[1])
    conn = psycopg2.connect(" ".join("{0}='{1}'".format(k,v) for k,v in db_config.iteritems()))
    # Create cursor object with results to be returned in dictionary format
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute("%s"%argv[1])
        cur.execute('update task_bots.execute_python set completed=true,error=false where id = %s'%argv[2])
    except Exception as e:
        cur.execute('update task_bots.execute_python set completed=true, error=true where id = %s'%argv[2])
        print(e)



if __name__== '__main__':
    try:
        if len(sys.argv)>1:
            print('Executing query')
            main(sys.argv)
            print('Query executed')
    finally:
        pass