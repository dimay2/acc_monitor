from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from datetime import datetime
from binance.exceptions import *
from binance.enums import *
from collections import deque
import tzlocal  # $ pip install tzlocal
import numpy as np 
import json
import os
import sys
import io
import time
import win32api
import sqlite3
from sqlite3 import Error

#======== GLOBAL PART ====================
api_key = '44e7juYXhwCKodvroDRlEzJ4sL4ZjFPL5PFphtSCL2sth8szLphpjfZjaPFoKG7T'
api_secret = 'DUKrNGTgfAniKxTO9VjcjjRviPlxrDLTtvj9uRxtce0wvbWW61hcGF3THHyz0u8e'
client = Client(api_key, api_secret)


gt = client.get_server_time()
tt=time.gmtime(int((gt["serverTime"])/1000))
win32api.SetSystemTime(tt[0],tt[1],0,tt[2],tt[3],tt[4],tt[5],0)

info_file=open("info_file.txt",mode="w")

try: all_tickers_info = client.get_all_tickers()
except BinanceAPIException as e:
    print("API get_account_snapshot: ",e)

#============= SQLITE connection =================
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    
    return conn

#============= SQLITE create table=================
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

#============= SQLITE insert balance data =================
def add_balance(conn, balance_data):
    """
    Insert new balance info into the projects table
    :param conn:
    :param balance_data:
    :return: last row id
    """
    sql = 'INSERT INTO balance(user_id,date,balance_source,USDT_balance) VALUES(?,?,?,?)'
    cur = conn.cursor()
    cur.execute(sql, balance_data)
    conn.commit()
    return cur.lastrowid

#============= get_sudt_rate =============
def get_usdt_rate(usdt_pair):
    if usdt_pair!='USDTUSDT':
        for i in range(len(all_tickers_info)):
            if all_tickers_info[i]['symbol']==usdt_pair:
                usdt_rate=all_tickers_info[i]['price']
    else:
        usdt_rate=1
    return usdt_rate

#============= get account info =============
def get_acc():
    acc_info=[]
    total_usdt=0
    try: acc_info = client.get_account()
    except BinanceAPIException as e:
        print("API get_account: ",e)
        return
    for i in range(len(acc_info['balances'])):
        coin=acc_info['balances'][i]['asset']
        free=acc_info['balances'][i]['free']
        if float(free)>0:
            usdt_pair=coin+'USDT'
            usdt_rate=get_usdt_rate(usdt_pair)
            usdt_coin_value=float(free)*float(usdt_rate)
            total_usdt+=usdt_coin_value
            print('acc_info: coin=%s free=%s usdt_pair=%s usdt_rate=%s usdt_coin_value=%s' %(coin,free,usdt_pair,usdt_rate,usdt_coin_value))
    
    print('total_usdt=%f' %(total_usdt))

#============= acc_snap_spot_balances =============
def get_acc_snapshot(type_name):
    acc_snap_spot=[]
    total_usdt=0
    try: acc_snap_spot = client.get_account_snapshot(type=type_name)
    except BinanceAPIException as e:
        print("API get_account_snapshot_spot: ",e)
        return
    if type_name=='SPOT':
        acc_snap_spot_balances=acc_snap_spot['snapshotVos'][0]['data']['balances']
    elif type_name=='MARGIN':
        acc_snap_spot_balances=acc_snap_spot['snapshotVos'][0]['data']['userAssets']
    else:
        return
        
    for i in range(len(acc_snap_spot_balances)):
        coin=acc_snap_spot_balances[i]['asset']
        free=float(acc_snap_spot_balances[i]['free'])
        if free>0:
            usdt_pair=coin+'USDT'
            if coin!='USDT':
                for i in range(len(all_tickers_info)):
                    if all_tickers_info[i]['symbol']==usdt_pair:
                        usdt_rate=float(all_tickers_info[i]['price'])
            else:
                usdt_rate=1
            usdt_coin_value=free*usdt_rate
            total_usdt+=usdt_coin_value
            print('acc_snap_spot_balances[%s]: coin=%s free=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%f' %(type_name,coin,free,usdt_pair,usdt_rate,usdt_coin_value))
    
    if total_usdt>0: print('total_usdt=%f' %(total_usdt))
    return total_usdt
#============= get futures account =============
def get_futures():
    futures_acc_info=[]
    try: futures_acc_info = client.futures_account()
    except BinanceAPIException as e:
        print("API futures_account: ",e)
        return
    futures_acc_balance=float(futures_acc_info['totalWalletBalance'])
    info_file.write('\n====futures_acc_info======:\n'+str(futures_acc_info))
    if futures_acc_balance>0: print('futures_acc_info: futures_acc_balance=%f USD' %(futures_acc_balance))
    return futures_acc_balance

#============= get futures coin account =============
def get_futures_coin():
    futures_coin_acc_info=[]
    total_usdt=0
    try: futures_coin_acc_info = client.futures_coin_account()
    except BinanceAPIException as e:
        print("API futures_coin_account: ",e)
        return
    for i in range(len(futures_coin_acc_info['assets'])):
        coin=futures_coin_acc_info['assets'][i]['asset']
        walletBalance=float(futures_coin_acc_info['assets'][i]['walletBalance'])
        if float(walletBalance)>0:
            usdt_pair=coin+'USDT'
            usdt_rate=float(get_usdt_rate(usdt_pair))
            usdt_coin_value=walletBalance*usdt_rate
            total_usdt+=usdt_coin_value
            print('futures_coin_acc_info: coin=%s walletBalance=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%s' %(coin,walletBalance,usdt_pair,usdt_rate,usdt_coin_value))
    
    info_file.write('\n====futures_coin_acc_info======:\n'+str(futures_coin_acc_info))
    if total_usdt>0: print('total_usdt=%f' %(total_usdt))
    return total_usdt

#============= get futures account balance =============
def get_futures_acc_balance():
    futures_acc_balance=[]
    try: futures_acc_balance = client.futures_account_balance()
    except BinanceAPIException as e:
        print("API futures_coin_account: ",e)
        return
    info_file.write('\n====futures_acc_balance======:\n'+str(futures_acc_balance))

# ================= MAIN =====================
def main():
    user_id='u_'+api_key[-5:]
    cur_time=time.strftime('%Y%m%d_%H%M')

    database = r"c:\my_python\python-binance-master\Alex\pythonsqlite.db"
    # create a database connection
    conn = create_connection(database)

    sql_create_main_table = """ CREATE TABLE IF NOT EXISTS balance (
                                        user_id text,
                                        date text,
                                        balance_source text NOT NULL,
                                        USDT_balance real
                                    ); """
    # create tables
    if conn is not None:
        # create main table
        create_table(conn, sql_create_main_table)
    else:
        print("Error! cannot create the database connection.")
    

    try: my_margin_acc=client.get_margin_account()
    except BinanceAPIException as e:
        print("API get_margin_account: ",e)
    try: balance_BTC = client.get_asset_balance(asset='BTC')
    except BinanceAPIException as e:
        print("API get_asset_balance: ",e)
    try: asset_details = client.get_asset_details()
    except BinanceAPIException as e:
        print("API get_asset_details: ",e)
    try: exchange_info = client.get_exchange_info()
    except BinanceAPIException as e:
        print("API get_exchange_info: ",e)

    #info_file.write(str(all_tickers_info)+'\n')
    #info_file.write(str(acc_info)+'\n')

    # get_acc()
    # get_acc_snapshot('SPOT')
    # get_acc_snapshot('MARGIN')
    # get_acc_snapshot('FUTURES')
    # get_futures()
    # get_futures_coin()
    # get_futures_acc_balance()

    with conn:
        # insert new balance
        balance_futures_coin = (user_id, cur_time,'get_futures_coin',get_futures_coin());
        balance_futures_usd_m = (user_id, cur_time,'get_futures',get_futures());
        balance_id = add_balance(conn, balance_futures_coin)
        balance_id = add_balance(conn, balance_futures_usd_m)
    info_file.close()

if __name__ == "__main__":
    main()