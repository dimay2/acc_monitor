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
import win32api
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

api_key = '44e7juYXhwCKodvroDRlEzJ4sL4ZjFPL5PFphtSCL2sth8szLphpjfZjaPFoKG7T'
api_secret = 'DUKrNGTgfAniKxTO9VjcjjRviPlxrDLTtvj9uRxtce0wvbWW61hcGF3THHyz0u8e'
client = Client(api_key, api_secret)
user_id=''
session_start_time=settings_data['asian_session_start'] #start time in MSK timezone of trade session (Asia/Europe/US)
sample_period=int(settings_data['update_period']) #sample period in seconds
usdt_daily_diff_thres=float(settings_data['usdt_daily_diff_thres']) #USDT difference since start of day session
usdt_month_start_diff_thres=float(settings_data['usdt_month_start_diff_thres']) #USDT difference since start of month session
usdt_week_start_thres=float(settings_data['usdt_week_start_thres']) #USDT difference since start of week (last Mon) session

api_id = '14490370'
api_hash = 'eefa159a84be87ac5087ff1586ad4e56'
bot_token = '2145626803:AAF3zhXeME5P6oVQiVbxi1VIkhIfdgPFiJU'
group_username = 'ddd2test_bot'

#======= sync time vs. Binance (must run as admin/root)=========
os.system('net start w32time')
os.system('w32tm /resync')
gt = client.get_server_time()
tt=time.gmtime(int((gt["serverTime"])/1000))
win32api.SetSystemTime(tt[0],tt[1],0,tt[2],tt[3],tt[4],tt[5],0)

info_file=open("info_file.txt",mode="w")

#======== get exchange rates =======
try: all_tickers_info = client.get_all_tickers()
except BinanceAPIException as e:
    print("API get_account_snapshot: ",e)
    exit()
info_file.write('\n====all_tickers_info======:\n'+str(all_tickers_info))
info_file.close()

#============= Telegram bot=================
def message_handler(update: Update, context: CallbackContext):
    update.message.reply_text(text='Text for example',)

def hello(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(f'Hello {update.effective_user.first_name}')

# def do_start(bot: Bot, update: Update):
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

def start(bot, update):
    # print('json file update : ' ,update)
    # print("json file bot : ', bot)
    chat_id = update.message.chat_id
    first_name = update.message.chat.first_name
    last_name = update.message.chat.last_name
    username = update.message.chat.username
    print("chat_id : {} and firstname : {} lastname : {}  username {}". format(chat_id, first_name, last_name , username))
    bot.sendMessage(chat_id, 'text')

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

def send_to_telegram(message):
    # get your api_id, api_hash, token
    # from telegram as described above
    message2 = "Working..."
    firstname =[]
    lastname = []
    username = []


    # your phone number
    phone = '+79152897767'
    # creating a telegram session and assigning
    # it to a variable client
    client = TelegramClient('session', api_id, api_hash)
    # connecting and building the session
    client.connect()
    # in case of script ran first time it will
    # ask either to input token or otp sent to
    # number or sent or your telegram id
    if not client.is_user_authorized():
        client.send_code_request(phone)
        # signing in the client
        client.sign_in(phone, input('Enter the code: '))
    try:
        # receiver user_id and access_hash, use
        # my user_id and access_hash for reference
        receiver = InputPeerUser('user_id', 'user_hash')
        # sending message using telegram client
        bot_participants=client.get_participants(group_username)

        if len(bot_participants):
            for x in bot_participants:
                firstname.append(x.first_name)
                lastname.append(x.last_name)
                username.append(x.username)
    
       # list to data frame conversion
        data ={'first_name' :firstname, 'last_name':lastname, 'user_name':username}
        userdetails = pd.DataFrame(data)

        client.send_message(receiver, message2, parse_mode='html')
    except Exception as e:
        # there may be many error coming in while like peer
        # error, wrong access_hash, flood_error, etc
        print(e)
    # disconnecting the telegram session
    client.disconnect()

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
        print(e)
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
def fetch_user_usdt_diff(conn, user_id, time_label, diff_type):
    # diff_type = 'diff_since_last_monday' | 'diff_since_start_of_month' | 'diff_since_start_of_day'
    cur = conn.cursor()
    
    if diff_type=='diff_since_start_of_day':
        sql="""  select round((a.USDT_balance-b.USDT_balance)/100*a.USDT_balance,4) from 
                    (select USDT_balance from balance where user_id=? and date=date('now') group by user_id having rowid=max(rowid) order by time asc) a,
                    (select USDT_balance from balance where date=date('now') and user_id=? and time_label=?) b
            """
    elif diff_type=='diff_since_last_monday':
        sql="""  select round((a.USDT_balance-b.USDT_balance)/100*a.USDT_balance,4) from 
                    (select USDT_balance from balance where user_id=? and date=date('now') group by user_id having rowid=max(rowid) order by time asc) a,
                    (select USDT_balance from balance where date=date('now','-7 days','weekday 1') and user_id=? and time_label=?) b
            """
    elif diff_type=='diff_since_start_of_month':
        sql="""  select round((a.USDT_balance-b.USDT_balance)/100*a.USDT_balance,4) from 
                    (select USDT_balance from balance where user_id=? and date=date('now') group by user_id having rowid=max(rowid) order by time asc) a,
                    (select USDT_balance from balance where date=date('now','start of month') and user_id=? and time_label=?) b
            """
    cur.execute(sql,(user_id,user_id,time_label))
    row=cur.fetchone()
    if row:
        USDT_balance_diff=float(row[0])
        return USDT_balance_diff
    else:
        return 0.00

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
        return -1
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
            print('acc_snap_spot_balances[%s]: coin=%s free=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%f' %(type_name,coin,free,usdt_pair,usdt_rate,usdt_coin_value))
    
    if total_usdt>0: print('total_usdt=%f' %(total_usdt))
    return total_usdt
#============= get futures account =============
def get_futures_USD_M():
    futures_USD_M=[]
    try: futures_USD_M = client.futures_account()
    except BinanceAPIException as e:
        print("API futures_account: ",e)
        return -1
    futures_acc_balance=float(futures_USD_M['totalWalletBalance'])
    info_file.write('\n====futures_USD_M======:\n'+str(futures_USD_M))
    if futures_acc_balance>0: print('futures_USD_M: futures_acc_balance=%f USD' %(futures_acc_balance))
    return futures_acc_balance

#============= get futures coin account =============
def get_futures_coin_M():
    futures_coin_M=[]
    total_usdt=0.00
    try: futures_coin_M = client.futures_coin_account()
    except BinanceAPIException as e:
        print("API futures_coin_account: ",e)
        return -1
    for i in range(len(futures_coin_M['assets'])):
        coin=futures_coin_M['assets'][i]['asset']
        walletBalance=float(futures_coin_M['assets'][i]['walletBalance'])
        if float(walletBalance)>0:
            usdt_pair=coin+'USDT'
            usdt_rate=float(get_usdt_rate(usdt_pair))
            usdt_coin_value=walletBalance*usdt_rate
            total_usdt+=float(usdt_coin_value)
            print('futures_coin_M: coin=%s walletBalance=%f usdt_pair=%s usdt_rate=%f usdt_coin_value=%s' %(coin,walletBalance,usdt_pair,usdt_rate,usdt_coin_value))
    
    info_file.write('\n====futures_coin_M======:\n'+str(futures_coin_M))
    if total_usdt>0: print('total_usdt=%f' %(total_usdt))
    return total_usdt

#============= get futures account balance =============
def get_futures_acc_balance():
    futures_acc_balance=[]
    try: futures_acc_balance = client.futures_account_balance()
    except BinanceAPIException as e:
        print("API futures_coin_account: ",e)
        return -1
    info_file.write('\n====futures_acc_balance======:\n'+str(futures_acc_balance))

# ================= MAIN =====================
def main():
    global info_file
    global user_id
    global telegram_users
    global chat_id_1
    global bot_token

    telegram_users.clear()

    user_id='u_'+api_key[-5:]
    loop_status='continue'
    database = r"c:\my_python\python-binance-master\Alex\pythonsqlite.db"
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
    # create tables
    if conn is not None:
        # create main table
        create_table(conn, sql_create_main_table)
    else:
        print("Error! cannot create the database connection.")

    updater = Updater (token=bot_token, use_context=True,)
    telegram_handler(updater)

    while loop_status!='exit_loop':
        total_usdt=0.0
        cur_date=time.strftime('%Y-%m-%d')
        cur_time=time.strftime('%H:%M')
        time_label=''

        info_file=open("info_file.txt",mode="w")

        print(cur_date,cur_time)
        print(telegram_users)

        try: my_margin_acc=client.get_margin_account()
        except BinanceAPIException as e:
            print("API get_margin_account: ",e)
            continue
        try: balance_BTC = client.get_asset_balance(asset='BTC')
        except BinanceAPIException as e:
            print("API get_asset_balance: ",e)
            continue
        try: asset_details = client.get_asset_details()
        except BinanceAPIException as e:
            print("API get_asset_details: ",e)
            continue
        try: exchange_info = client.get_exchange_info()
        except BinanceAPIException as e:
            print("API get_exchange_info: ",e)
            continue

        #info_file.write(str(all_tickers_info)+'\n')
        #info_file.write(str(acc_info)+'\n')

        #==========================================
        # get_acc()
        # get_acc_snapshot('SPOT')
        # get_acc_snapshot('MARGIN')
        # get_acc_snapshot('FUTURES')
        
        usr_name='dimay2'
        if usr_name in telegram_users.keys():
            chat_id=telegram_users[usr_name]
            message_text='Hi %s, your chat_id=%d' %(usr_name, chat_id)
            url_req='https://api.telegram.org/bot%s/sendMessage?chat_id=%d&text=%s' %(bot_token,chat_id,message_text)
            receive=requests.get(url_req)

        # futures_usdt_M=get_futures_USD_M()
        # if futures_usdt_M<0: continue
        # futures_coin_M=get_futures_coin_M()
        # if futures_coin_M<0: continue

        # total_usdt += futures_usdt_M
        # total_usdt += futures_coin_M
        # get_futures_acc_balance()


        with conn:
            time_label='asian_session_start'
            # time_label='us_session_start'
            session_start_entries=fetch_user_balance(conn,user_id,cur_date,time_label)
            if len(session_start_entries)<1 and cur_time>=settings_data[time_label]:
                #no asian session start entries yet for this date
                balance_total_futures = (user_id, cur_date, cur_time,time_label,'total_futures_usdt',total_usdt)
                balance_id = add_balance(conn, balance_total_futures)
            else:
                balance_total_futures = (user_id, cur_date, cur_time,'regular_sample','total_futures_usdt',total_usdt)
                balance_id = add_balance(conn, balance_total_futures)

        # Get LOSS/PROFIT statistics on daily/monthly/weekly basis and check vs. configured thresholds
        usdt_diff_since_start_of_day=fetch_user_usdt_diff(conn,user_id,time_label,'diff_since_start_of_day')
        usdt_diff_since_start_of_month=fetch_user_usdt_diff(conn,user_id,time_label,'diff_since_start_of_month')
        usdt_diff_since_last_monday=fetch_user_usdt_diff(conn,user_id,time_label,'diff_since_last_monday')
        if usdt_diff_since_start_of_day<usdt_daily_diff_thres:
            print('!Warning!: usdt_diff_since_start_of_day=%f exceeded LOSS threshold=%f' %(usdt_diff_since_start_of_day,usdt_daily_diff_thres))
        elif usdt_diff_since_start_of_month<usdt_month_start_diff_thres:
            print('!Warning!: usdt_diff_since_start_of_month=%f exceeded LOSS threshold=%f' %(usdt_diff_since_start_of_month,usdt_month_start_diff_thres))
        elif usdt_diff_since_last_monday<usdt_week_start_thres:
            print('!Warning!: usdt_diff_since_last_monday=%f exceeded LOSS threshold=%f' %(usdt_diff_since_last_monday,usdt_week_start_thres))

        print('usdt_diff_day=%f, usdt_diff_month_start=%f, usdt_diff_last_Mon=%f' %(usdt_diff_since_start_of_day,usdt_diff_since_start_of_month,usdt_diff_since_last_monday))

        info_file.close()
        # loop_status='exit_loop'
        time.sleep(sample_period)

    updater.close()

#======= MAIN execution===========
if __name__ == "__main__":
    main()