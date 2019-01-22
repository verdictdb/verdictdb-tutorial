import pyverdict
import pymysql
import pandas as pd
from py4j import Py4JError
import time
import sys

host = "localhost";
port = "3306";
user = "root";
password = "";

def create():
    try:
        verdict_conn = pyverdict.mysql(
            host=host,
            user=user,
            password=password,
            port=port
        )
        print('Creating a scrambled table for lineitem...')
        start = time.time()
        verdict_conn.sql('CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem')
        duration = time.time() - start
        verdict_conn.close()
        print('Scrambled table for lineitem has been created.')
        print('Time Taken = {:d} s'.format(duration))
    except Py4JError as e:
        if e.cause:
            print(e.cause)

def run():
    try:
        mysql_conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            passwd=password,
            autocommit=True
        )
        cur = mysql_conn.cursor()
        cur.execute('SET GLOBAL query_cache_size=0')
        start = time.time()
        cur.execute('SELECT avg(l_extendedprice) FROM tpch1g.lineitem')
        duration = time.time() - start
        print('Without VerdictDB: average(l_extendedprice) = {:f}'.format(cur.fetchone()))
        cur.close()
        print('Time Taken = {:d} s'.format(duration))

        verdict_conn = pyverdict.mysql(
            host=host,
            user=user,
            password=password,
            port=port
        )
        start = time.time()
        df = verdict_conn.sql('SELECT avg(l_extendedprice) FROM tpch1g.lineitem')
        duration = time.time() - start
        verdict_conn.close()
        print('With VerdictDB: average(l_extendedprice) = {:f}'.format(df.values[0]))
        print('Time Taken = {:d} s'.format(duration))
    except Py4JError as e:
        if e.cause:
            print(e.cause)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python tutorial.py <command>')
        print('Supported command: create, run')
        return
    if sys.argv[4] == 'create':
        create()
    elif sys.argv[4] == 'run':
        run()
