import pyverdict
import pymysql
import pandas as pd
import time
import sys

host = 'localhost';
port = 3306;
user = 'root';
password = '';

def create():
    verdict_conn = pyverdict.mysql(
        host=host,
        user=user,
        password=password,
        port=port
    )
    print('Creating a scrambled table for lineitem...')
    verdict_conn.sql('DROP ALL SCRAMBLE tpch1g.lineitem')
    start = time.time()
    verdict_conn.sql('CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem BLOCKSIZE 100')
    duration = time.time() - start
    verdict_conn.close()
    print('Scrambled table for lineitem has been created.')
    print('Time Taken = {:f} s'.format(duration))

def run():
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
    print('Without VerdictDB: average(l_extendedprice) = {:.5f}'.format(cur.fetchone()[0]))
    cur.close()
    print('Time Taken = {:f} s'.format(duration))

    verdict_conn = pyverdict.mysql(
        host=host,
        user=user,
        password=password,
        port=port
    )
    start = time.time()
    df = verdict_conn.sql('SELECT avg(l_extendedprice) FROM tpch1g.lineitem_scramble')
    duration = time.time() - start
    verdict_conn.close()
    print(df.values)
    print('With VerdictDB: average(l_extendedprice) = {:.5f}'.format(df.values[0][0]))
    print('Time Taken = {:f} s'.format(duration))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python tutorial.py <command>')
        print('Supported command: create, run')
        exit(0) 
    if sys.argv[1] == 'create':
        create()
    elif sys.argv[1] == 'run':
        run()
