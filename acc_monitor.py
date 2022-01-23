#! /opt/acc_monitor/python-env/bin/python

# from asyncio.windows_events import NULL
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from datetime import datetime
from binance.exceptions import *
from binance.enums import *
from collections import deque
from pyasn1.type.univ import Null
import tzlocal  # $ pip install tzlocal
import numpy as np 
import json
import os
import sys
import io
# import timepython Scripts/pywin32_postinstall.py -install
import time
# import win32api
import sqlite3
from sqlite3 import Error
import telebot
from telethon.sync import TelegramClient
from telethon.tl.types import InputPeerUser, InputPeerChannel, UpdateFavedStickers
from telethon import TelegramClient, sync, events

from telegram import Update, update
from telegram import Bot
from telegram.ext import CallbackContext
from telegram.ext import Updater
from telegram.ext import Filters
from telegram.ext import MessageHandler
from telegram.ext import CommandHandler
import requests
from telegram.ext.dispatcher import run_async

#======== GLOBAL PART ====================
#======== load settings from file =======
settings_file=open("settings")
settings_data=json.load(settings_file)
settings_file.close()

callbackcontext_1=CallbackContext
updater_1=Updater
chat_id_1=0
telegram_user_name_1=''
bot_1=Bot
telegram_users={}
acc_info=[]
open_orders_symbols=set()
symbol_futures_ticker={}
futures_open_orders=[]
orderbook_tickers=[]
futures_USD_M_lst=[]
futures_usdt_M=0.0
futures_coin_M=0.0
total_potential_PNL_usdt=0.0
total_potential_PNL_perc=0.0

# my acc
# api_key = '44e7juYXhwCKodvroDRlEzJ4sL4ZjFPL5PFphtSCL2sth8szLphpjfZjaPFoKG7T'
# api_secret = 'DUKrNGTgfAniKxTO9VjcjjRviPlxrDLTtvj9uRxtce0wvbWW61hcGF3THHyz0u8e'
# Roman test 100
# api_key = '3b1FSIm8ULaiKSxZKHKtabvgwtOk3Oc7k8WiuCvVTlJrcvz0vei7xPi4TyxI5SkF'
# api_secret = 'TURwiXtFY0ZUxSPbMfXpIzRMfzIpz86JgRbPWWdKuqjbccYWsLYmTyVQ0DfTVNxJ'

# a.mimdu@gmail.com
# Метка ключа API 2
# api_key = 'pf3lYR3Sh4A519d4mbrWzEXDTeml17TZhwY4u5DzDcjcdszDNOpKUe9hiQNYUEQc'
# api_secret = 'BLkXG1GfAcoW7XpzrVSSOehkpoqDyAPz5igfkmM45hpxfrxzDBaPLpTlYgrtJw99'

# a.mimdu@gmail.com
# Метка ключа API 3
# api_key = 'gNSkZoP11NeVF3D7krU3FH4mMKDBCtYcL0qnsY2PeqNVsK40xGGUCBlvb2whxpaV'
# api_secret = 'ixuoQiag9a5tTZdk56eEXq9I3HrkZHnSJyctX6DHrgQfRqq04mhPADqrDAU6MBNL'

# client = Client(NULL, NULL)

user_id=''
session_start_time=settings_data['midnight_session_start'] #start time in MSK timezone of trade session (Asia/Europe/US)
sample_period=int(settings_data['update_period']) #sample period in seconds
daily_low_thres=float(settings_data['daily_low_thres']) #% stop loss difference since start of day session
monthly_low_thres=float(settings_data['monthly_low_thres']) #% stop loss difference since start of month session
weekly_low_thres=float(settings_data['weekly_low_thres']) #% stop loss difference since start of week (last Mon) session
daily_high_thres=float(settings_data['daily_high_thres']) #% take profit difference since start of day session
monthly_high_thres=float(settings_data['monthly_high_thres']) #% take profit difference since start of month session
weekly_high_thres=float(settings_data['weekly_high_thres']) #% take profit difference since start of week (last Mon) session

#Telegram bot settings
api_id = '14490370'
api_hash = 'eefa159a84be87ac5087ff1586ad4e56'
bot_token = '2145626803:AAF3zhXeME5P6oVQiVbxi1VIkhIfdgPFiJU'
group_username = 'ddd2test_bot'

#======= sync time vs. Binance (must run as admin/root)=========
# os.system('net start w32time')
# os.system('w32tm /resync')
# gt = client.get_server_time()
# tt=time.gmtime(int((gt["serverTime"])/1000))
# win32api.SetSystemTime(tt[0],tt[1],0,tt[2],tt[3],tt[4],tt[5],0)

info_file=open("info_file.txt",mode="w")

#======== get SPOT exchange rates =======
# try: all_tickers_info = client.get_all_tickers()
# except BinanceAPIException as e:
#     print("API get_account_snapshot: ",e)
#     exit()
# info_file.write('\n====all_tickers_info======:\n'+str(all_tickers_info))
# info_file.close()

now = datetime.now()

#============= Telegram bot=================
def telegram_notifier(telegram_user_id,message):
    # chat_id=telegram_users[usr_name]
    chat_id=telegram_user_id
    url_req='https://api.telegram.org/bot%s/sendMessage?chat_id=%d&text=%s' %(bot_token,chat_id,message)
    receive=requests.get(url_req)


def do_start(update: Update, callbackcontext: CallbackContext):
    global callbackcontext_1
    global chat_id_1
    global telegram_user_name_1
    global updater_1
    global bot_1
    global telegram_users

    updater_1=update
    chat_id_1=update.message.chat_id
    telegram_user_name_1=update.message.chat.username
    # member=update.message.chat.get_member(user_name)
    callbackcontext_1=callbackcontext
    bot_1=callbackcontext_1.bot
    # chatmember=callbackcontext.bot.getChatMember()
    message_text='your chat_id=%d' %chat_id_1
    telegram_users[telegram_user_name_1]=chat_id_1
    # callbackcontext.bot.send_message(chat_id=update.message.chat_id, text=message_text)
    # print(chatmember)

# def do_echo(bot: Bot, update: Update):
def do_echo(update: Update, callbackcontext: CallbackContext):
    text = update.message.text
    callbackcontext.bot.sendMessage(chat_id=update.message.chat_id, text=text)

def telegram_handler(updater):
    global chat_id_1
    global bot_token
    global telegram_users
    global telegram_user_name_1
    global user_id

    # start_handler=CommandHandler('start', do_start, run_async=True)
    start_handler=CommandHandler('start', do_start)
    # message_handler_2=MessageHandler(Filters.text, do_echo)
    updater.dispatcher.add_handler(start_handler)
    # updater.dispatcher.add_handler(message_handler_2)

    # print(updater.bot.get_me())
    # updater.dispatcher.add_handler(MessageHandler(filters=Filters.all, callback=message_handler))
    updater.start_polling(poll_interval=10)
    # callbackcontext_1.bot.send_message(chat_id_1, text='DO_START AAAAAAAAAA')
    # updater.idle()
    return updater


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
        # print(e)
        info_file.write('\nCannot connect to DB: '+ e)
        exit()
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
        # print(e)
        info_file.write('\nCannot create DB table: '+ e)
        exit()
#============= SQLITE insert balance data =================
def add_balance(conn, balance_data):
    """
    Insert new balance info into the projects table
    :param conn:
    :param balance_data:
    :return: last row id
    """
    sql = 'INSERT INTO balance(user_id,date,time,time_label,balance_source,USDT_balance) VALUES(?,?,?,?,?,?)'
    cur = conn.cursor()
    cur.execute(sql, balance_data)
    conn.commit()
    return cur.lastrowid

#============= SQLITE find user start session entries =================
def fetch_user_balance(conn, user_id, date, time_label):
    cur = conn.cursor()
    cur.execute('SELECT * FROM balance WHERE user_id=? AND date=? AND time_label=?', (user_id,date,time_label))
    row=cur.fetchall()
    return row

#============= SQLITE find start session USDT difference =================
def fetch_user_perc_diff(conn, user_id, time_label, diff_type):     # diff_type = 'diff_since_start_of_day' | 'diff_since_last_monday' | 'diff_since_start_of_month'
    ret_val=dict()
    cur = conn.cursor()
    
    if diff_type=='diff_since_start_of_day':
        # sql="""  select round((a.USDT_balance-b.USDT_balance)/b.USDT_balance*100,4),b.USDT_balance,a.USDT_balance from 
        #             (select USDT_balance from balance where user_id=? and date=date('now','localtime') group by user_id having rowid=max(rowid) order by time asc) a,
        #             (select USDT_balance from balance where date=date('now','localtime') and user_id=? and time_label=?) b
        #     """
        
        # =================== % loss, initial usdt balance, current usd balance
        sql="""  select round((a.USDT_balance-b.USDT_balance)/b.USDT_balance*100,4),b.USDT_balance,a.USDT_balance from 
                    (select USDT_balance from balance where user_id=? and date=date('now','localtime') group by user_id having rowid=max(rowid) order by time asc) a,
                    (select USDT_balance from balance where date=date('now','localtime') and user_id=? and time_label=?) b
            """
    elif diff_type=='diff_since_last_monday':
        sql="""  select round((a.USDT_balance-b.USDT_balance)/b.USDT_balance*100,4),b.USDT_balance,a.USDT_balance from 
                    (select USDT_balance from balance where user_id=? and date=date('now','localtime') group by user_id having rowid=max(rowid) order by time asc) a,
                    (select USDT_balance from balance where date=date('now','-7 days','weekday 1') and user_id=? and time_label=?) b
            """
    elif diff_type=='diff_since_start_of_month':
        sql="""  select round((a.USDT_balance-b.USDT_balance)/b.USDT_balance*100,4),b.USDT_balance,a.USDT_balance from 
                    (select USDT_balance from balance where user_id=? and date=date('now','localtime') group by user_id having rowid=max(rowid) order by time asc) a,
                    (select USDT_balance from balance where date=date('now','start of month') and user_id=? and time_label=?) b
            """
    cur.execute(sql,(user_id,user_id,time_label))
    row=cur.fetchone()
    if row:
        # perc_balance_diff=float(row[0])
        # return perc_balance_diff
        ret_val['perc_diff']=float(row[0])
        ret_val['init_usdt_balance']=float(row[1])
        ret_val['cur_usdt_balance']=float(row[2])
        return ret_val
    else:
        ret_val['perc_diff']=0
        return ret_val

#============= get_usdt_rate =============
# def get_usdt_rate(usdt_pair):
#     if usdt_pair!='USDTUSDT':
#         for i in range(len(all_tickers_info)):
#             if all_tickers_info[i]['symbol']==usdt_pair:
#                 usdt_rate=all_tickers_info[i]['price']
#     else:
#         usdt_rate=1
#     return usdt_rate

#============= get account info =============
def get_acc():
    global acc_info
    total_usdt=0
    try: acc_info = client.get_account(recvWindow=5000)
    except BinanceAPIException as e:
        # print("API get_account: ",e)
        info_file.write('\nAPI get_account: '+ e)
        return -1
    for i in range(len(acc_info['balances'])):
        coin=acc_info['balances'][i]['asset']
        free=acc_info['balances'][i]['free']
        if float(free)>0:
            usdt_pair=coin+'USDT'
            usdt_rate=get_usdt_rate(usdt_pair)
            usdt_coin_value=float(free)*float(usdt_rate)
            total_usdt+=usdt_coin_value
            # print('acc_info: coin=%s free=%s usdt_pair=%s usdt_rate=%s usdt_coin_value=%s' %(coin,free,usdt_pair,usdt_rate,usdt_coin_value))
            info_file.write('\nacc_info: coin=%s free=%s usdt_pair=%s usdt_rate=%s usdt_coin_value=%s' %(coin,free,usdt_pair,usdt_rate,usdt_coin_value))
    
    # print('total_usdt=%f' %(total_usdt))
    info_file.write('\ntotal_usdt=%f' %(total_usdt))

#============= acc_snap_spot_balances =============
def get_acc_snapshot(type_name):
    acc_snap_spot=[]
    total_usdt=0
    try: acc_snap_spot = client.get_account_snapshot(type=type_name,recvWindow=5000)
    except BinanceAPIException as e:
        # print("API get_account_snapshot_spot: ",e)
        info_file.write('\nAPI get_account_snapshot_spot: '+ e)
        return -1
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
            # print('acc_snap_spot_balances[%s]: coin=%s free=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%f' %(type_name,coin,free,usdt_pair,usdt_rate,usdt_coin_value))
            info_file.write('\nacc_snap_spot_balances[%s]: coin=%s free=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%f' %(type_name,coin,free,usdt_pair,usdt_rate,usdt_coin_value))

    if total_usdt>0: info_file.write('\ntotal_usdt=%f' %(total_usdt)) #print('total_usdt=%f' %(total_usdt))
    return total_usdt
#============= get futures account =============
def get_futures_USD_M_lst():
    global futures_USD_M_lst
    futures_USD_M_lst.clear()
    try: futures_USD_M_lst = client.futures_account()
    except BinanceAPIException as e:
        # print("API futures_account: ",e)
        info_file.write('\nAPI futures_account: '+ e)
        return -1
    futures_acc_balance=float(futures_USD_M_lst['totalWalletBalance'])
    # info_file.write('\n====futures_USD_M_lst======:\n'+str(futures_USD_M_lst))
    if futures_acc_balance>0: print('futures_USD_M_lst: futures_acc_balance=%f USD' %(futures_acc_balance))
    return futures_acc_balance

#============= get futures coin account =============
def get_futures_coin_M():
    futures_coin_M=[]
    total_usdt=0.00
    try: futures_coin_M = client.futures_coin_account()
    except BinanceAPIException as e:
        # print("API futures_coin_account: ",e)
        info_file.write('\nAPI futures_coin_account: '+ e)
        return -1
    for i in range(len(futures_coin_M['assets'])):
        coin=futures_coin_M['assets'][i]['asset']
        walletBalance=float(futures_coin_M['assets'][i]['walletBalance'])
        if float(walletBalance)>0:
            usdt_pair=coin+'USDT'
            usdt_rate=float(get_usdt_rate(usdt_pair))
            usdt_coin_value=walletBalance*usdt_rate
            total_usdt+=float(usdt_coin_value)
            # print('futures_coin_M: coin=%s walletBalance=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%s' %(coin,walletBalance,usdt_pair,usdt_rate,usdt_coin_value))
            info_file.write('\nfutures_coin_M: coin=%s walletBalance=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%s' %(coin,walletBalance,usdt_pair,usdt_rate,usdt_coin_value))

    # info_file.write('\n====futures_coin_M======:\n'+str(futures_coin_M))
    if total_usdt>0: info_file.write('\ntotal_usdt=%f' %(total_usdt)) #print('total_usdt=%f' %(total_usdt))
    return total_usdt

#============= get futures account balance =============
def get_futures_acc_balance():
    futures_acc_balance=[]
    try: futures_acc_balance = client.futures_account_balance()
    except BinanceAPIException as e:
        # print("API futures_coin_account: ",e)
        info_file.write('\nAPI futures_coin_account: '+ e)
        return -1
    info_file.write('\n====futures_acc_balance======:\n'+str(futures_acc_balance))

#============= get all open orders =============
def get_all_open_orders():
    global futures_open_orders
    try: futures_open_orders = client.futures_get_open_orders(recvWindow=5000)
    except BinanceAPIException as e:
        # print("API futures_get_open_orders: ",e)
        info_file.write('\nAPI futures_get_open_orders: '+ e)
        return -1
    info_file.write('\n====futures_open_orders======:\n'+str(futures_open_orders))

#============= get all open orders tickers (USDT rate) =============
def futures_orderbook_ticker():
    global orderbook_tickers
    # get all orderbook tickers
    try: orderbook_tickers=client.futures_orderbook_ticker()
    except BinanceAPIException as e:
        # print("API futures_orderbook_ticker: ",e)
        info_file.write('\nAPI futures_orderbook_ticker: '+ e)
        return -1

#============= update open orders rates =============
def update_open_orders_usdt_rates():    # get all orderbook tickers
    global orderbook_tickers
    global futures_open_orders
    global open_orders_symbols
    global symbol_futures_ticker
    
    # Go over all open orders
    open_orders_symbols.clear()
    symbol_futures_ticker.clear()
    
    futures_orderbook_ticker() # update all future orders USDT rate

    for i in range(len(futures_open_orders)):   # detect open orders
        if futures_open_orders[i][' ']=='NEW':     # if NEW (open) order detected - add it to the "open_orders_symbols" set
            open_orders_symbols.add(futures_open_orders[i]['symbol'])   # add symbols from open orders
    for i in range(len(futures_USD_M_lst['positions'])):
        if float(futures_USD_M_lst['positions'][i]['initialMargin'])>0:	#got the open USD_M future position
            open_orders_symbols.add(futures_USD_M_lst['positions'][i]['symbol'])    # add symbols from open positions
    info_file.write('\n====open_orders_symbols =%s ======:\n' %(open_orders_symbols))   #open_orders_symbols has symbols both from open positions & open orders

    # for i in range(len(orderbook_tickers)):     # go over all orderbook_tickers of futures contracts/symbols prices
    #     if orderbook_tickers[i]['symbol'] in open_orders_symbols:   # if symbol found in open orders - add it to symbol_futures_ticker
    #         symbol_futures_ticker[orderbook_tickers[i]['symbol']]=orderbook_tickers[i]['bidPrice']  # symbol_futures_ticker[symbol] - symbol futures current USDT rate
    for i in open_orders_symbols:
        order_symbol=str(i)
        for i in range(len(orderbook_tickers)):
            # j=list(order_symbol.split('_'))
            if order_symbol==orderbook_tickers[i]['symbol']:
                symbol_futures_ticker[order_symbol]=orderbook_tickers[i]['bidPrice']
    info_file.write('\n====symbol_futures_ticker======:\n'+str(symbol_futures_ticker))

#============= Calculate all open orders current potential PNL =============
def calculate_open_orders_total_potential_PNL_usdt():
    global total_potential_PNL_usdt
    global futures_usdt_M
    global symbol_futures_ticker

    total_potential_PNL_usdt=0.0
    this_order_potential_PNL_usdt=0.0
    this_order_potential_PNL_perc=0.0

    for i in range(len(futures_USD_M_lst['positions'])):
        if float(futures_USD_M_lst['positions'][i]['initialMargin'])>0:	#got the open USD_M future position
            symbol=futures_USD_M_lst['positions'][i]['symbol']
            leverage=float(futures_USD_M_lst['positions'][i]['leverage'])
            entryPrice=float(futures_USD_M_lst['positions'][i]['entryPrice'])
            positionInitialMargin=float(futures_USD_M_lst['positions'][i]['positionInitialMargin'])
            currentPrice=float(symbol_futures_ticker[symbol])  # get current futures price for given symbol
            if float(futures_USD_M_lst['positions'][i]['notional'])>0: # BUY/LONG
                this_order_potential_PNL_perc=(currentPrice-entryPrice)/entryPrice*100*leverage
            elif float(futures_USD_M_lst['positions'][i]['notional'])<0: # SELL/SHORT
                this_order_potential_PNL_perc=-(currentPrice-entryPrice)/entryPrice*100*leverage
            this_order_potential_PNL_usdt=this_order_potential_PNL_perc*positionInitialMargin/100
            total_potential_PNL_usdt += float(this_order_potential_PNL_usdt)
            info_file.write('\nsymbol=%s, entryPrice=%f, positionInitialMargin=%f, currentPrice=%f, this_order_potential_PNL_perc=%f, this_order_potential_PNL_usdt=%f, total_potential_PNL_usdt=%f\n' %(symbol,entryPrice,positionInitialMargin,currentPrice, this_order_potential_PNL_perc,this_order_potential_PNL_usdt,total_potential_PNL_usdt))

# ================================================ MAIN =====================================================================
def main():
    global info_file
    global user_id
    global telegram_users
    global chat_id_1
    global bot_token
    global acc_info
    global futures_open_orders
    global orderbook_tickers
    global open_orders_symbols
    global futures_usdt_M
    global futures_coin_M
    global total_potential_PNL_perc
    global client

    telegram_users.clear()
    futures_usdt_M=0.0
    futures_coin_M=0.0
    total_potential_PNL_perc = 0.0

    # user_id='u_'+api_key[-5:]
    # populate users table manually
    # INSERT INTO users(user_id,user_name,exchange_type,api_key,api_secret,status,remarks) VALUES(?,?,?,?,?,?,?)
    loop_status='continue'
    # database = r"c:\my_python\python-binance-master\Alex\pythonsqlite.db"
    database = r"./pythonsqlite.db"
    # create a database connection
    conn = create_connection(database)

    sql_create_main_table = """ CREATE TABLE IF NOT EXISTS balance (
                                        user_id text,
                                        date text,
                                        time text,
                                        time_label text,
                                        balance_source text NOT NULL,
                                        USDT_balance real
                                    ); """

    sql_create_users_table = """ CREATE TABLE IF NOT EXISTS users (
                                        user_id text NOT NULL PRIMARY KEY,
                                        user_name text,
                                        telegram_user_id integer,
                                        exchange_type text,
                                        api_key text,
                                        api_secret text,
                                        status text,
                                        remarks text
                                    ); """
    # create tables
    if conn is not None:
        # create tables
        create_table(conn, sql_create_main_table)
        create_table(conn, sql_create_users_table)
    else:
        print("Error! cannot create the database connection.")

    # updater = Updater (token=bot_token, use_context=True,)
    # telegram_handler(updater)

    # c = conn.cursor()
    # c.execute('select * from users')
    # for r in c.fetchall():
    #     print(dict(r))

    #================ iterate all users in user table =============
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    fetch_users_sql = 'SELECT * FROM users'

    while loop_status!='exit_loop':
        for user in c.execute(fetch_users_sql):
            if user['status'] == 'ACTIVE':
                user_id = user['user_id']
                api_key = user['api_key']
                api_secret = user['api_secret']
                client = Client(api_key, api_secret)

                total_usdt=0.0
                
                cur_date=time.strftime('%Y-%m-%d')
                cur_time=time.strftime('%H:%M')
                time_label=''

                info_file=open("info_file.txt",mode="a")

                print(cur_date,cur_time)
                info_file.write('\n'+str(now)+'\nuser_id=%s status check' %user['user_id'])
                # print(telegram_users)

                try: my_margin_acc=client.get_margin_account(recvWindow=5000)
                except BinanceAPIException as e:
                    # print("API get_margin_account: ",e)
                    info_file.write('\nAPI get_margin_account: '+ e)
                    continue
                try: balance_BTC = client.get_asset_balance(asset='BTC',recvWindow=5000)
                except BinanceAPIException as e:
                    # print("API get_asset_balance: ",e)
                    info_file.write('\nAPI get_asset_balance: '+ e)
                    continue
                try: asset_details = client.get_asset_details(recvWindow=5000)
                except BinanceAPIException as e:
                    # print("API get_asset_details: ",e)
                    info_file.write('\nAPI get_asset_details: '+ e)
                    continue
                try: exchange_info = client.get_exchange_info()
                except BinanceAPIException as e:
                    # print("API get_exchange_info: ",e)
                    info_file.write('\nAPI get_exchange_info: '+ e)
                    continue
                # try: futures_coin_all_orders = client.futures_coin_get_all_orders()
                # except BinanceAPIException as e:
                #     print("API get_exchange_info: ",e)
                #     continue
                # try: futures_coin_open_orders = client.futures_coin_get_open_orders()
                # except BinanceAPIException as e:
                #     print("API futures_coin_get_open_orders: ",e)
                #     continue
                try: futures_all_orders = client.futures_get_all_orders(recvWindow=5000)
                except BinanceAPIException as e:
                    # print("API futures_get_all_orders: ",e)
                    info_file.write('\nAPI futures_get_all_orders: '+ e)
                    continue
                # try: futures_order_book = client.futures_order_book()
                # except BinanceAPIException as e:
                #     print("API futures_order_book: ",e)
                #     continue
                try: futures_position_information = client.futures_position_information(recvWindow=5000)
                except BinanceAPIException as e:
                    # print("API futures_position_information: ",e)
                    # info_file.write('\nAPI futures_position_information: '+ e)
                    continue
                # try: futures_coin_position_information = client.futures_coin_position_information(recvWindow=5000)
                # except BinanceAPIException as e:
                #     print("API futures_coin_position_information: ",e)
                #     continue
                # try: all_orders = client.get_all_orders(symbol=symbol,recvWindow=5000)
                # except BinanceAPIException as e:
                #     print("API get_all_orders: ",e)
                #     continue
                # info_file.write('\n====all_orders======:\n'+str(all_orders))
                # info_file.write('\n====futures_coin_all_orders======:\n'+str(futures_coin_all_orders))
                # info_file.write('\n====futures_coin_open_orders======:\n'+str(futures_coin_open_orders))
                info_file.write('\n====futures_all_orders======:\n'+str(futures_all_orders))
                # info_file.write('\n====futures_order_book======:\n'+str(futures_order_book))
                # info_file.write('\n====futures_position_information======:\n'+str(futures_position_information))
                # info_file.write('\n====futures_coin_position_information======:\n'+str(futures_coin_position_information))

                # info_file.write(str(all_tickers_info)+'\n')
                info_file.write(str(acc_info)+'\n')

                #==========================================
                # get_acc()
                # get_acc_snapshot('SPOT')
                # get_acc_snapshot('MARGIN')
                # get_acc_snapshot('FUTURES')
                
                futures_usdt_M=get_futures_USD_M_lst()
                if futures_usdt_M<0: continue
                # futures_coin_M=get_futures_coin_M()
                # if futures_coin_M<0: continue

                total_usdt += futures_usdt_M
                # total_usdt += futures_coin_M
                
                get_futures_acc_balance()
                get_all_open_orders()
                update_open_orders_usdt_rates()
                calculate_open_orders_total_potential_PNL_usdt()
                total_potential_PNL_perc=((futures_usdt_M+total_potential_PNL_usdt)/futures_usdt_M*100 - 100)    # calculate current potential total PNL in perc
                potential_USD_M_futures_balance=futures_usdt_M+total_potential_PNL_usdt # potential_USD_M_futures_balance = current_USD_M_futures_balance (on wallet) + total potential PNL

                with conn:
                    time_label='midnight_session_start'
                    session_start_entries=fetch_user_balance(conn,user_id,cur_date,time_label)
                    if len(session_start_entries)<1 and cur_time>=settings_data[time_label]:
                        #no session start entries yet for this date
                        balance_total_futures = (user_id, cur_date, cur_time,time_label,'pot_USD_M_fut_bal',potential_USD_M_futures_balance)
                        balance_id = add_balance(conn, balance_total_futures)
                    else:
                        balance_total_futures = (user_id, cur_date, cur_time,'regular_sample','pot_USD_M_fut_bal',potential_USD_M_futures_balance)
                        balance_id = add_balance(conn, balance_total_futures)
                
                info_file.write('\nfutures_usdt_M=%f, potential_USD_M_futures_balance=%f, total_potential_PNL_usdt=%f, total_potential_PNL_perc=%f\n' %(futures_usdt_M,potential_USD_M_futures_balance,total_potential_PNL_usdt,total_potential_PNL_perc))
                # Get LOSS/PROFIT statistics on daily/monthly/weekly basis and check vs. configured thresholds
                diff_day_start=fetch_user_perc_diff(conn,user_id,time_label,'diff_since_start_of_day')
                diff_month_start=fetch_user_perc_diff(conn,user_id,time_label,'diff_since_start_of_month')
                diff_monday_start=fetch_user_perc_diff(conn,user_id,time_label,'diff_since_last_monday')
                
                if diff_day_start['perc_diff']<daily_low_thres:  # if threshold is reached - cancel all Active orders of logged symbols
                    # print('!Warning!: diff_day_start=%f%% exceeded LOSS threshold=%f%%' %(diff_day_start['perc_diff'],daily_low_thres))
                    info_file.write('\n!Warning!: diff_day_start=%f%% exceeded LOSS threshold=%f%%' %(diff_day_start['perc_diff'],daily_low_thres))
                    # print('symbols to cancel=%s' %open_orders_symbols)
                    info_file.write('\nsymbols to cancel=%s' %open_orders_symbols)
                    for symbol in open_orders_symbols:
                        # print('cancelling orders with symbol=%s' %symbol)
                        info_file.write('\ncancelling orders with symbol=%s' %symbol)
                        try: info_file.write('\nFAKE futures_cancel_all_open_orders') #futures_cancel_all_open_orders = client.futures_cancel_all_open_orders(symbol=symbol,recvWindow=5000)
                        except BinanceAPIException as e:
                            # print("API futures_cancel_all_open_orders: ",e)
                            info_file.write('\nAPI futures_cancel_all_open_orders: '+ e)
                            continue
                        futures_cancel_all_open_orders=''
                        # print('!Warning!: All orders canceled %s' %(futures_cancel_all_open_orders))
                        info_file.write('\n!Warning!: All orders canceled %s' %(futures_cancel_all_open_orders))
                        side='NONE'
                        for i in range(len(futures_USD_M_lst['positions'])):
                            if float(futures_USD_M_lst['positions'][i]['initialMargin'])>0 and futures_USD_M_lst['positions'][i]['symbol']==symbol:
                                if float(futures_USD_M_lst['positions'][i]['notional'])>0: # BUY/LONG
                                    side='SELL'
                                    quantity=str(abs(float(futures_USD_M_lst['positions'][i]['positionAmt'])))
                                    break
                                elif float(futures_USD_M_lst['positions'][i]['notional'])<0: # SELL/SHORT
                                    side='BUY'
                                    quantity=str(abs(float(futures_USD_M_lst['positions'][i]['positionAmt'])))
                                    break

                        # create order to cancel the pending Positions
                        if side!='NONE':
                            try: info_file.write('\nFAKE futures_create_order_CANCEL') #futures_create_order_CANCEL = client.futures_create_order(symbol=symbol, side=side, type='MARKET', quantity=quantity, recvWindow=5000)
                            except BinanceAPIException as e:
                                # print("API futures_create_order: ",e)
                                info_file.write('\nAPI futures_create_order: '+ e)
                                continue
                            # futures_cancel_all_open_orders=''
                            # message_str=('!Warning!: All orders canceled %s' %(futures_cancel_all_open_orders))
                            info_file.write('\n====futures_create_order_CANCEL ======:\n%s' %(futures_create_order_CANCEL))

                    info_file.write('\n====futures_cancel_all_open_orders for symbols ======:\n'+ str(open_orders_symbols))
                    telegram_user_id=116156904
                    message=('!!!WARNING!!!: User =%s, Your positions and orders are canceled due to LOSS threshold reached. \
                        LOSS since day start=%f%%, USD_M futures balance initial=%fUSDT, potential for now=%fUSDT' %(user_id, diff_day_start['perc_diff'],diff_day_start['init_usdt_balance'],potential_USD_M_futures_balance))
                    telegram_notifier(telegram_user_id,message)
                elif diff_month_start['perc_diff']<monthly_low_thres:
                    # print('!Warning!: diff_month_start=%f%% exceeded LOSS threshold=%f%%' %(diff_month_start['perc_diff'],monthly_low_thres))
                    info_file.write('\n!Warning!: diff_month_start=%f%% exceeded LOSS threshold=%f%%' %(diff_month_start['perc_diff'],monthly_low_thres))
                elif diff_monday_start['perc_diff']<weekly_low_thres:
                    # print('!Warning!: diff_monday_start=%f%% exceeded LOSS threshold=%f%%' %(diff_monday_start['perc_diff'],weekly_low_thres))
                    info_file.write('\n!Warning!: diff_monday_start=%f%% exceeded LOSS threshold=%f%%' %(diff_monday_start['perc_diff'],weekly_low_thres))

                # info_file.write('\nusdt_diff_day=%f%%, usdt_diff_month_start=%f%%, usdt_diff_last_Mon=%f%%\n' %(diff_day_start,diff_month_start,diff_monday_start))

                info_file.close()
                time.sleep(sample_period)
        # loop_status='exit_loop'

    # updater.close()

#======= MAIN execution===========
if __name__ == "__main__":
    main()