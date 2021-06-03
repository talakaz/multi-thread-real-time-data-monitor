from websocket import create_connection
from stocktrends import indicators
from datetime import timedelta
from datetime import datetime
import pandas as pd
import numpy as np
import timeloop
import sqlite3
import logger
import ccxt
import talib
import json
import time
import sys

# 迴圈頻率及系統參數、臨時變數
interval = {'bm_d': 15, 'bm_h': 5, 'bm_testnet_d': 15, 'bm_testnet_h': 5, 'bybit_testnet_d': 5, 'bybit_testnet_h': 3, 'bnc_d': 15, 'bnc_h': 5, 'bnc_testnet_d': 15, 'bnc_testnet_h': 5, 'print': 1}
systemParams = {'counter': 0, 'printSqlReader': None}

# BitMEX 系統功能參數及狀態
bmParams = {'ema': 24, 'atr': 24, 'atr_d': 7, 'rsi': 8, 'renko1': 100, 'renko2': 50, 'mdd': 12}
bmDayTrend = {'sqlWriter': None, 'haColor': None, 'atr': None}
bmHourTrend = {'sqlWriter': None, 'haColor': None, 'lastPrice': None, 'ema': None, 'atr': None,
               'rsi': None, 'trendUp': None, 'overBuy': None, 'overSell': None, 'renko1Up': None,
               'renko2Up': None, 'mddh': None, 'mddl': None, 'mdd': None, 'overmdd': None,
               'lowermdd': None, 'flat': None}
bmRenkoBar1 = {'1': None, '2': None, '3': None, '4': None, '5': None,
               '6': None, '7': None, '8': None, '9': None, '10': None,
               '11': None, '12': None, '13': None, '14': None, '15': None,
               '16': None, '17': None, '18': None, '19': None, '20': None,
               'last': None, 'now': None, 'change': None}
bmRenkoBar2 = {'1': None, '2': None, '3': None, '4': None, '5': None,
               '6': None, '7': None, '8': None, '9': None, '10': None,
               '11': None, '12': None, '13': None, '14': None, '15': None,
               '16': None, '17': None, '18': None, '19': None, '20': None,
               'last': None, 'now': None, 'change': None}

# Bybit 系統功能參數及狀態
bybitParams = {'ema': 24, 'atr': 24, 'atr_d': 7, 'rsi': 8, 'renko1': 100, 'renko2': 50, 'mdd': 12}
bybitDayTrend = {'sqlWriter': None, 'haColor': None, 'atr': None}
bybitHourTrend = {'sqlWriter': None, 'haColor': None, 'lastPrice': None, 'ema': None, 'atr': None,
                  'rsi': None, 'trendUp': None, 'overBuy': None, 'overSell': None, 'renko1Up': None,
                  'renko2Up': None, 'mddh': None, 'mddl': None, 'mdd': None, 'overmdd': None,
                  'lowermdd': None, 'flat': None}
bybitRenkoBar1 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                  '6': None, '7': None, '8': None, '9': None, '10': None,
                  '11': None, '12': None, '13': None, '14': None, '15': None,
                  '16': None, '17': None, '18': None, '19': None, '20': None,
                  'last': None, 'now': None, 'change': None}
bybitRenkoBar2 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                  '6': None, '7': None, '8': None, '9': None, '10': None,
                  '11': None, '12': None, '13': None, '14': None, '15': None,
                  '16': None, '17': None, '18': None, '19': None, '20': None,
                  'last': None, 'now': None, 'change': None}

# Bybit Testnet 系統功能參數及狀態
bybitTestnetParams = {'ema': 24, 'atr': 24, 'atr_d': 7, 'rsi': 8, 'renko1': 100, 'renko2': 50, 'mdd': 12}
bybitTestnetDayTrend = {'sqlWriter': None, 'haColor': None, 'atr': None}
bybitTestnetHourTrend = {'sqlWriter': None, 'haColor': None, 'lastPrice': None, 'ema': None, 'atr': None,
                         'rsi': None, 'trendUp': None, 'overBuy': None, 'overSell': None, 'renko1Up': None,
                         'renko2Up': None, 'mddh': None, 'mddl': None, 'mdd': None, 'overmdd': None,
                         'lowermdd': None, 'flat': None}
bybitTestnetRenkoBar1 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                         '6': None, '7': None, '8': None, '9': None, '10': None,
                         '11': None, '12': None, '13': None, '14': None, '15': None,
                         '16': None, '17': None, '18': None, '19': None, '20': None,
                         'last': None, 'now': None, 'change': None}
bybitTestnetRenkoBar2 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                         '6': None, '7': None, '8': None, '9': None, '10': None,
                         '11': None, '12': None, '13': None, '14': None, '15': None,
                         '16': None, '17': None, '18': None, '19': None, '20': None,
                         'last': None, 'now': None, 'change': None}

# Binance 系統功能參數及狀態
bncParams = {'ema': 24, 'atr': 24, 'atr_d': 7, 'rsi': 8, 'renko1': 100, 'renko2': 50, 'mdd': 12}
bncDayTrend = {'sqlWriter': None, 'haColor': None, 'atr': None}
bncHourTrend = {'sqlWriter': None, 'wss': None, 'wssSqlWriter': None, 'wssTimeOut': 0, 'haColor': None, 'lastPrice': None, 'trade': None, 'bestBid': None, 'bestAsk': None, 'ema': None, 'atr': None,
                'rsi': None, 'trendUp': None, 'overBuy': None, 'overSell': None, 'renko1Up': None,
                'renko2Up': None, 'mddh': None, 'mddl': None, 'mdd': None, 'overmdd': None,
                'lowermdd': None, 'flat': None}
bncRenkoBar1 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                '6': None, '7': None, '8': None, '9': None, '10': None,
                '11': None, '12': None, '13': None, '14': None, '15': None,
                '16': None, '17': None, '18': None, '19': None, '20': None,
                'last': None, 'now': None, 'change': None}
bncRenkoBar2 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                '6': None, '7': None, '8': None, '9': None, '10': None,
                '11': None, '12': None, '13': None, '14': None, '15': None,
                '16': None, '17': None, '18': None, '19': None, '20': None,
                'last': None, 'now': None, 'change': None}

# Binance Testnet 系統功能參數及狀態
bncTestnetParams = {'ema': 24, 'atr': 24, 'atr_d': 7, 'rsi': 8, 'renko1': 100, 'renko2': 50, 'mdd': 12}
bncTestnetDayTrend = {'sqlWriter': None, 'haColor': None, 'atr': None}
bncTestnetHourTrend = {'sqlWriter': None, 'wss': None, 'wssSqlWriter': None, 'wssTimeOut': 0, 'haColor': None, 'lastPrice': None, 'trade': None, 'bestBid': None, 'bestAsk': None, 'ema': None, 'atr': None,
                       'rsi': None, 'trendUp': None, 'overBuy': None, 'overSell': None, 'renko1Up': None,
                       'renko2Up': None, 'mddh': None, 'mddl': None, 'mdd': None, 'overmdd': None,
                       'lowermdd': None, 'flat': None}
bncTestnetRenkoBar1 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                       '6': None, '7': None, '8': None, '9': None, '10': None,
                       '11': None, '12': None, '13': None, '14': None, '15': None,
                       '16': None, '17': None, '18': None, '19': None, '20': None,
                       'last': None, 'now': None, 'change': None}
bncTestnetRenkoBar2 = {'1': None, '2': None, '3': None, '4': None, '5': None,
                       '6': None, '7': None, '8': None, '9': None, '10': None,
                       '11': None, '12': None, '13': None, '14': None, '15': None,
                       '16': None, '17': None, '18': None, '19': None, '20': None,
                       'last': None, 'now': None, 'change': None}

# 實例化物件 
bm_excg_long = ccxt.bitmex({
    'apiKey': BitMEX['api_key'],
    'secret': BitMEX['api_secret'],
    'enableRateLimit': True,
    'rateLimit': 100,
    'options': {'adjustForTimeDifference': True}, })

bm_excg_short = ccxt.bitmex({
    'apiKey': BitMEX['api_key'],
    'secret': BitMEX['api_secret'],
    'enableRateLimit': True,
    'rateLimit': 100,
    'options': {'adjustForTimeDifference': True}, })

bybit_excg_long = ccxt.bybit({
    'apiKey': Bybit['api_key'],
    'secret': Bybit['api_secret'],
    'enableRateLimit': True,
    'rateLimit': 100,
    'options': {'adjustForTimeDifference': True}, })

bybit_excg_short = ccxt.bybit({
    'apiKey': Bybit['api_key'],
    'secret': Bybit['api_secret'],
    'enableRateLimit': True,
    'rateLimit': 100,
    'options': {'adjustForTimeDifference': True}, })

bybit_testnet_excg = ccxt.bybit({
    'apiKey': Bybit['testnet_api_key'],
    'secret': Bybit['testnet_api_secret'],
    'enableRateLimit': True,
    'rateLimit': 100,
    'options': {'adjustForTimeDifference': True}, })

bnc_excg_long = ccxt.binance({
    'apiKey': Binance['api_key'],
    'secret': Binance['api_secret'],
    'enableRateLimit': True,
    'rateLimit': 200,
    # 'urls': {
    #     'api': {
    #         'fapiPublic': 'https://testnet.binancefuture.com/fapi/v1',
    #         'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
    #     }},
    'options': {
        'adjustForTimeDifference': True,
        'defaultType': 'future', }})

bnc_excg_short = ccxt.binance({
    'apiKey': Binance['api_key'],
    'secret': Binance['api_secret'],
    'enableRateLimit': True,
    'rateLimit': 200,
    # 'urls': {
    #     'api': {
    #         'fapiPublic': 'https://testnet.binancefuture.com/fapi/v1',
    #         'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
    #     }},
    'options': {
        'adjustForTimeDifference': True,
        'defaultType': 'future', }})

bnc_testnet_excg_long = ccxt.binance({
    'apiKey': Binance['testnet_api_key'],
    'secret': Binance['testnet_api_secret'],
    'enableRateLimit': True,
    'rateLimit': 200,
    'urls': {
        'api': {
            'fapiPublic': 'https://testnet.binancefuture.com/fapi/v1',
            'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
        }},
    'options': {
        'adjustForTimeDifference': True,
        'defaultType': 'future', }})

bnc_testnet_excg_short = ccxt.binance({
    'apiKey': Binance['testnet_api_key'],
    'secret': Binance['testnet_api_key'],
    'enableRateLimit': True,
    'rateLimit': 200,
    'urls': {
        'api': {
            'fapiPublic': 'https://testnet.binancefuture.com/fapi/v1',
            'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
        }},
    'options': {
        'adjustForTimeDifference': True,
        'defaultType': 'future', }})

# 實例化多執行緒管理物件
tl = timeloop.Timeloop()


#  HeikinAshi/ATR
@tl.job(interval=timedelta(seconds=interval['bm_d']))
def bm_day_trend_update():
    try:
        # 建立資料庫寫入器
        if bmDayTrend['sqlWriter'] is None:
            bmDayTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        kline = bm_excg_short.fetch_ohlcv(symbol=BitMEX['short_symbol'], timeframe='1d', limit=500, params={'reverse': True})
        raw = pd.DataFrame(kline)
        df = pd.DataFrame()
        df['timestamp'] = raw[0]
        df['open'] = raw[1]
        df['high'] = raw[2]
        df['low'] = raw[3]
        df['close'] = raw[4]
        df['volume'] = raw[5]
        # df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp']
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(df.copy())
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2

        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bmDayTrend['haColor'] = df_ha['color'][-1]

        # 計算技術指標
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bmParams['atr_d'])
        bmDayTrend['atr'] = np.around(df_atr[-1], decimals=1)

        # 更新資料庫
        CMD = "UPDATE BitMEX SET color_d=%d, atr_d=%.1f" % (bmDayTrend['haColor'], bmDayTrend['atr'])
        cusor = bmDayTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bmDayTrend['sqlWriter'].commit()
    except Exception as e:
        logger.log('bm_dtrend', 'warning', str(e.args))


# HeikinAshi/ATR/EMA/RSI/MDD/RenkoBar/Flat
@tl.job(interval=timedelta(seconds=interval['bm_h']))
def bm_hour_trend_update():
    try:
        # 建立資料庫寫入器
        if bmHourTrend['sqlWriter'] is None:
            bmHourTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        kline = bm_excg_short.fetch_ohlcv(symbol=BitMEX['short_symbol'], timeframe='1h', limit=500, params={'reverse': True})
        df = pd.DataFrame(kline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df2 = pd.DataFrame(df.copy())  # 還沒重設index前複制給renko用
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        bmHourTrend['lastPrice'] = df['close'][-1]

        # 計算mdd及破前高破前低
        bmHourTrend['mddh'] = pd.Series(df['high'][500 - bmParams['mdd']:-1]).max()
        bmHourTrend['mddl'] = pd.Series(df['low'][500 - bmParams['mdd']:-1]).min()
        bmHourTrend['mdd'] = bmHourTrend['mddh'] - bmHourTrend['mddl']
        if bmHourTrend['lastPrice'] > bmHourTrend['mddh']:
            bmHourTrend['overmdd'] = 1
            bmHourTrend['lowermdd'] = 1
        elif bmHourTrend['lastPrice'] < bmHourTrend['mddl']:
            bmHourTrend['overmdd'] = 0
            bmHourTrend['lowermdd'] = 1
        else:
            bmHourTrend['overmdd'] = 0
            bmHourTrend['lowermdd'] = 0

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(index=df.index.values, columns=['open', 'high', 'low', 'close'])
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2
        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bmHourTrend['haColor'] = df_ha['color'][-1]

        # 計算 Renko1 及轉折訊號
        df2['date'] = df2['timestamp']
        renko1 = indicators.Renko(df2)
        renko1.brick_size = bmParams['renko1']
        renko1.chart_type = indicators.Renko.PERIOD_CLOSE
        data1 = renko1.get_ohlc_data()
        data1 = data1.set_index('date')
        # data1 = data1.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        if data1['uptrend'][-2] is False and data1['uptrend'][-1] is True:
            bmHourTrend['renko1Up'] = 1
        elif data1['uptrend'][-2] is True and data1['uptrend'][-1] is False:
            bmHourTrend['renko1Up'] = 0
        else:
            bmHourTrend['renko1Up'] = 99

        # 顯示最後10筆 RenkoBar
        bmRenkoBar1['1'] = data1['uptrend'][-20]
        bmRenkoBar1['2'] = data1['uptrend'][-19]
        bmRenkoBar1['3'] = data1['uptrend'][-18]
        bmRenkoBar1['4'] = data1['uptrend'][-17]
        bmRenkoBar1['5'] = data1['uptrend'][-16]
        bmRenkoBar1['6'] = data1['uptrend'][-15]
        bmRenkoBar1['7'] = data1['uptrend'][-14]
        bmRenkoBar1['8'] = data1['uptrend'][-13]
        bmRenkoBar1['9'] = data1['uptrend'][-12]
        bmRenkoBar1['10'] = data1['uptrend'][-11]
        bmRenkoBar1['11'] = data1['uptrend'][-10]
        bmRenkoBar1['12'] = data1['uptrend'][-9]
        bmRenkoBar1['13'] = data1['uptrend'][-8]
        bmRenkoBar1['14'] = data1['uptrend'][-7]
        bmRenkoBar1['15'] = data1['uptrend'][-6]
        bmRenkoBar1['16'] = data1['uptrend'][-5]
        bmRenkoBar1['17'] = data1['uptrend'][-4]
        bmRenkoBar1['18'] = data1['uptrend'][-3]
        bmRenkoBar1['19'] = data1['uptrend'][-2]
        bmRenkoBar1['20'] = data1['uptrend'][-1]
        bmRenkoBar1['now'] = data1['close'][-1]

        # 偵測最後一根bar是否異動
        if bmRenkoBar1['now'] != bmRenkoBar1['last'] and bmRenkoBar1['last'] is not None:
            bmRenkoBar1['change'] = 'Market Moved!'
        elif bmRenkoBar1['now'] == bmRenkoBar1['last']:
            bmRenkoBar1['change'] = None
        bmRenkoBar1['last'] = bmRenkoBar1['now']

        diff = df.index[-1] - data1.index[-1]
        bmHourTrend['flat'] = diff.seconds / 3600

        # 計算 Renko2 及轉折訊號
        renko2 = indicators.Renko(df2)
        renko2.brick_size = bmParams['renko2']
        renko2.chart_type = indicators.Renko.PERIOD_CLOSE
        data2 = renko2.get_ohlc_data()
        data2 = data2.set_index('date')

        # 用時間降低突波誤判 (counter % catch)
        if data2['uptrend'][-2] is False and data2['uptrend'][-1] is True:
            bmHourTrend['renko2Up'] = 1

        elif data2['uptrend'][-2] is True and data2['uptrend'][-1] is False:
            bmHourTrend['renko2Up'] = 0
        else:
            bmHourTrend['renko2Up'] = 99

        # 顯示最後10筆 RenkoBar
        bmRenkoBar2['1'] = data2['uptrend'][-20]
        bmRenkoBar2['2'] = data2['uptrend'][-19]
        bmRenkoBar2['3'] = data2['uptrend'][-18]
        bmRenkoBar2['4'] = data2['uptrend'][-17]
        bmRenkoBar2['5'] = data2['uptrend'][-16]
        bmRenkoBar2['6'] = data2['uptrend'][-15]
        bmRenkoBar2['7'] = data2['uptrend'][-14]
        bmRenkoBar2['8'] = data2['uptrend'][-13]
        bmRenkoBar2['9'] = data2['uptrend'][-12]
        bmRenkoBar2['10'] = data2['uptrend'][-11]
        bmRenkoBar2['11'] = data2['uptrend'][-10]
        bmRenkoBar2['12'] = data2['uptrend'][-9]
        bmRenkoBar2['13'] = data2['uptrend'][-8]
        bmRenkoBar2['14'] = data2['uptrend'][-7]
        bmRenkoBar2['15'] = data2['uptrend'][-6]
        bmRenkoBar2['16'] = data2['uptrend'][-5]
        bmRenkoBar2['17'] = data2['uptrend'][-4]
        bmRenkoBar2['18'] = data2['uptrend'][-3]
        bmRenkoBar2['19'] = data2['uptrend'][-2]
        bmRenkoBar2['20'] = data2['uptrend'][-1]

        bmRenkoBar2['now'] = data2['close'][-1]
        # 偵測最後一根bar是否異動
        if bmRenkoBar2['now'] != bmRenkoBar2['last'] and bmRenkoBar2['last'] is not None:
            bmRenkoBar2['change'] = 'Market Moved!'
        elif bmRenkoBar2['now'] == bmRenkoBar2['last']:
            bmRenkoBar2['change'] = None
        bmRenkoBar2['last'] = bmRenkoBar2['now']

        # 計算指標
        df_ema = talib.EMA(df['close'].values, bmParams['ema'])
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bmParams['atr'])
        df_rsi = talib.RSI(df['close'], bmParams['rsi'])

        bmHourTrend['ema'] = np.around(df_ema[-1], decimals=1)
        bmHourTrend['atr'] = np.around(df_atr[-1], decimals=1)
        bmHourTrend['rsi'] = np.around(df_rsi[-1], decimals=1)

        if bmHourTrend['lastPrice'] > bmHourTrend['ema']:
            bmHourTrend['trendUp'] = 1
        else:
            bmHourTrend['trendUp'] = 0
        if bmHourTrend['rsi'] > 70:
            bmHourTrend['overBuy'] = 1
            bmHourTrend['overSell'] = 0
        elif bmHourTrend['rsi'] < 30:
            bmHourTrend['overBuy'] = 0
            bmHourTrend['overSell'] = 1
        else:
            bmHourTrend['overBuy'] = 0
            bmHourTrend['overSell'] = 0

        # 更新資料庫
        CMD = "UPDATE BitMEX SET " \
              "lastprice=%.1f, mddh=%.1f, mddl=%.1f, mdd=%.1f, overmdd=%d, lowermdd=%d, color_h=%d, renko1up=%d, " \
              "renko2up=%d, flat=%d, ema=%.1f, atr_h=%.1f, rsi=%.1f, trend=%d, overbuy=%d, oversell=%d" % \
              (bmHourTrend['lastPrice'], bmHourTrend['mddh'], bmHourTrend['mddl'], bmHourTrend['mdd'], bmHourTrend['overmdd'], bmHourTrend['lowermdd'],
               bmHourTrend['haColor'], bmHourTrend['renko1Up'], bmHourTrend['renko2Up'], bmHourTrend['flat'], bmHourTrend['ema'], bmHourTrend['atr'],
               bmHourTrend['rsi'], bmHourTrend['trendUp'], bmHourTrend['overBuy'], bmHourTrend['overSell'])
        cusor = bmHourTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bmHourTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('bm_htrend', 'warning', str(e.args))


# HeikinAshi/ATR
def bybit_day_trend_update():
    try:
        # 建立資料庫寫入器
        if bybitDayTrend['sqlWriter'] is None:
            bybitDayTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        kline = bybit_excg.fetch_ohlcv(symbol=Bybit['short_symbol'], timeframe='1d', limit=500,
                                       params={'reverse': True})
        raw = pd.DataFrame(kline)
        df = pd.DataFrame()
        df['timestamp'] = raw[0]
        df['open'] = raw[1]
        df['high'] = raw[2]
        df['low'] = raw[3]
        df['close'] = raw[4]
        df['volume'] = raw[5]
        # df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp']
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(df.copy())
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2

        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bybitDayTrend['haColor'] = df_ha['color'][-1]

        # 計算技術指標
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bybitParams['atr_d'])
        bybitDayTrend['atr'] = np.around(df_atr[-1], decimals=1)

        # 更新資料庫
        CMD = "UPDATE Bybit SET color_d=%d, atr_d=%.1f" % (bybitDayTrend['haColor'], bybitDayTrend['atr'])
        cusor = bybitDayTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bybitDayTrend['sqlWriter'].commit()
    except Exception as e:
        logger.log('bybit_dtrend', 'warning', str(e.args))


# HeikinAshi/ATR/EMA/RSI/MDD/RenkoBar/Flat
@tl.job(interval=timedelta(seconds=interval['bybit_h']))
def bybit_hour_trend_update():
    try:
        # 建立資料庫寫入器
        if bybitHourTrend['sqlWriter'] is None:
            bybitHourTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        kline = bybit_excg_short.fetch_ohlcv(symbol=Bybit['short_symbol'], timeframe='1h', limit=500,
                                             params={'reverse': True})
        df = pd.DataFrame(kline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df2 = pd.DataFrame(df.copy())  # 還沒重設index前複制給renko用
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        bybitHourTrend['lastPrice'] = df['close'][-1]

        # 計算mdd及破前高破前低
        bybitHourTrend['mddh'] = pd.Series(df['high'][500 - bybitParams['mdd']:-1]).max()
        bybitHourTrend['mddl'] = pd.Series(df['low'][500 - bybitParams['mdd']:-1]).min()
        bybitHourTrend['mdd'] = bybitHourTrend['mddh'] - bybitHourTrend['mddl']
        if bybitHourTrend['lastPrice'] > bybitHourTrend['mddh']:
            bybitHourTrend['overmdd'] = 1
            bybitHourTrend['lowermdd'] = 1
        elif bybitHourTrend['lastPrice'] < bybitHourTrend['mddl']:
            bybitHourTrend['overmdd'] = 0
            bybitHourTrend['lowermdd'] = 1
        else:
            bybitHourTrend['overmdd'] = 0
            bybitHourTrend['lowermdd'] = 0

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(index=df.index.values, columns=['open', 'high', 'low', 'close'])
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2
        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bybitHourTrend['haColor'] = df_ha['color'][-1]

        # 計算 Renko1 及轉折訊號
        df2['date'] = df2['timestamp']
        renko1 = indicators.Renko(df2)
        renko1.brick_size = bybitParams['renko1']
        renko1.chart_type = indicators.Renko.PERIOD_CLOSE
        data1 = renko1.get_ohlc_data()
        data1 = data1.set_index('date')
        # data1 = data1.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        if data1['uptrend'][-2] is False and data1['uptrend'][-1] is True:
            bybitHourTrend['renko1Up'] = 1
        elif data1['uptrend'][-2] is True and data1['uptrend'][-1] is False:
            bybitHourTrend['renko1Up'] = 0
        else:
            bybitHourTrend['renko1Up'] = 99

        # 顯示最後10筆 RenkoBar
        bybitRenkoBar1['1'] = data1['uptrend'][-20]
        bybitRenkoBar1['2'] = data1['uptrend'][-19]
        bybitRenkoBar1['3'] = data1['uptrend'][-18]
        bybitRenkoBar1['4'] = data1['uptrend'][-17]
        bybitRenkoBar1['5'] = data1['uptrend'][-16]
        bybitRenkoBar1['6'] = data1['uptrend'][-15]
        bybitRenkoBar1['7'] = data1['uptrend'][-14]
        bybitRenkoBar1['8'] = data1['uptrend'][-13]
        bybitRenkoBar1['9'] = data1['uptrend'][-12]
        bybitRenkoBar1['10'] = data1['uptrend'][-11]
        bybitRenkoBar1['11'] = data1['uptrend'][-10]
        bybitRenkoBar1['12'] = data1['uptrend'][-9]
        bybitRenkoBar1['13'] = data1['uptrend'][-8]
        bybitRenkoBar1['14'] = data1['uptrend'][-7]
        bybitRenkoBar1['15'] = data1['uptrend'][-6]
        bybitRenkoBar1['16'] = data1['uptrend'][-5]
        bybitRenkoBar1['17'] = data1['uptrend'][-4]
        bybitRenkoBar1['18'] = data1['uptrend'][-3]
        bybitRenkoBar1['19'] = data1['uptrend'][-2]
        bybitRenkoBar1['20'] = data1['uptrend'][-1]
        bybitRenkoBar1['now'] = data1['close'][-1]

        # 偵測最後一根bar是否異動
        if bybitRenkoBar1['now'] != bybitRenkoBar1['last'] and bybitRenkoBar1['last'] is not None:
            bybitRenkoBar1['change'] = 'Market Moved!'
        elif bybitRenkoBar1['now'] == bybitRenkoBar1['last']:
            bybitRenkoBar1['change'] = None
        bybitRenkoBar1['last'] = bybitRenkoBar1['now']

        # 偵測停止訊號
        diff = df.index[-1] - data1.index[-1]
        bybitHourTrend['flat'] = diff.seconds / 3600

        # 計算 Renko2 及轉折訊號
        renko2 = indicators.Renko(df2)
        renko2.brick_size = bybitParams['renko2']
        renko2.chart_type = indicators.Renko.PERIOD_CLOSE
        data2 = renko2.get_ohlc_data()
        data2 = data2.set_index('date')

        # 用時間降低突波誤判 (counter % catch)
        if data2['uptrend'][-2] is False and data2['uptrend'][-1] is True:
            bybitHourTrend['renko2Up'] = 1

        elif data2['uptrend'][-2] is True and data2['uptrend'][-1] is False:
            bybitHourTrend['renko2Up'] = 0
        else:
            bybitHourTrend['renko2Up'] = 99

        # 顯示最後10筆 RenkoBar
        bybitRenkoBar2['1'] = data2['uptrend'][-20]
        bybitRenkoBar2['2'] = data2['uptrend'][-19]
        bybitRenkoBar2['3'] = data2['uptrend'][-18]
        bybitRenkoBar2['4'] = data2['uptrend'][-17]
        bybitRenkoBar2['5'] = data2['uptrend'][-16]
        bybitRenkoBar2['6'] = data2['uptrend'][-15]
        bybitRenkoBar2['7'] = data2['uptrend'][-14]
        bybitRenkoBar2['8'] = data2['uptrend'][-13]
        bybitRenkoBar2['9'] = data2['uptrend'][-12]
        bybitRenkoBar2['10'] = data2['uptrend'][-11]
        bybitRenkoBar2['11'] = data2['uptrend'][-10]
        bybitRenkoBar2['12'] = data2['uptrend'][-9]
        bybitRenkoBar2['13'] = data2['uptrend'][-8]
        bybitRenkoBar2['14'] = data2['uptrend'][-7]
        bybitRenkoBar2['15'] = data2['uptrend'][-6]
        bybitRenkoBar2['16'] = data2['uptrend'][-5]
        bybitRenkoBar2['17'] = data2['uptrend'][-4]
        bybitRenkoBar2['18'] = data2['uptrend'][-3]
        bybitRenkoBar2['19'] = data2['uptrend'][-2]
        bybitRenkoBar2['20'] = data2['uptrend'][-1]

        bybitRenkoBar2['now'] = data2['close'][-1]
        # 偵測最後一根bar是否異動
        if bybitRenkoBar2['now'] != bybitRenkoBar2['last'] and bybitRenkoBar2['last'] is not None:
            bybitRenkoBar2['change'] = 'Market Moved!'
        elif bybitRenkoBar2['now'] == bybitRenkoBar2['last']:
            bybitRenkoBar2['change'] = None
        bybitRenkoBar2['last'] = bybitRenkoBar2['now']

        # 計算指標
        df_ema = talib.EMA(df['close'].values, bybitParams['ema'])
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bybitParams['atr'])
        df_rsi = talib.RSI(df['close'], bybitParams['rsi'])

        bybitHourTrend['ema'] = np.around(df_ema[-1], decimals=1)
        bybitHourTrend['atr'] = np.around(df_atr[-1], decimals=1)
        bybitHourTrend['rsi'] = np.around(df_rsi[-1], decimals=1)

        # 判斷首倉及進場時機
        # ema
        if bybitHourTrend['lastPrice'] > bybitHourTrend['ema']:
            bybitHourTrend['trendUp'] = 1
        else:
            bybitHourTrend['trendUp'] = 0
        # rsi
        if bybitHourTrend['rsi'] > 70:
            bybitHourTrend['overBuy'] = 1
            bybitHourTrend['overSell'] = 0
        elif bybitHourTrend['rsi'] < 30:
            bybitHourTrend['overBuy'] = 0
            bybitHourTrend['overSell'] = 1
        else:
            bybitHourTrend['overBuy'] = 0
            bybitHourTrend['overSell'] = 0

        # 更新資料庫
        CMD = "UPDATE Bybit SET " \
              "lastprice=%.1f, mddh=%.1f, mddl=%.1f, mdd=%.1f, overmdd=%d, lowermdd=%d, color_h=%d, renko1up=%d, " \
              "renko2up=%d, flat=%d, ema=%.1f, atr_h=%.1f, rsi=%.1f, trend=%d, overbuy=%d, oversell=%d" % \
              (bybitHourTrend['lastPrice'], bybitHourTrend['mddh'], bybitHourTrend['mddl'], bybitHourTrend['mdd'],
               bybitHourTrend['overmdd'], bybitHourTrend['lowermdd'],
               bybitHourTrend['haColor'], bybitHourTrend['renko1Up'], bybitHourTrend['renko2Up'], bybitHourTrend['flat'],
               bybitHourTrend['ema'], bybitHourTrend['atr'],
               bybitHourTrend['rsi'], bybitHourTrend['trendUp'], bybitHourTrend['overBuy'], bybitHourTrend['overSell'])
        cusor = bybitHourTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bybitHourTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('bybit_htrend', 'warning', str(e.args))


# Bybit Testnet 日線級別技術指標及趨勢計算 (HeikinAshi/ATR)
@tl.job(interval=timedelta(seconds=interval['bybit_testnet_d']))
def bybit_testnet_day_trend_update():
    try:
        # 建立資料庫寫入器
        if bybitTestnetDayTrend['sqlWriter'] is None:
            bybitTestnetDayTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        kline = bybit_testnet_excg.fetch_ohlcv(symbol=Bybit['symbol'], since=int(time.time()) - (86400 * 199), timeframe='1d')  # (往回推199天)bybit的日k線要輸入timestamp，原始api也要改動
        raw = pd.DataFrame(kline)
        ts_to_date = [time.strftime("%Y-%m-%d", time.gmtime(x)) for x in raw[0]]
        df = pd.DataFrame()
        df['timestamp'] = raw[0]
        df['open'] = raw[1]
        df['high'] = raw[2]
        df['low'] = raw[3]
        df['close'] = raw[4]
        df['volume'] = raw[5]
        df['timestamp'] = pd.DataFrame(ts_to_date)
        df['date'] = df['timestamp']
        df = df.set_index('timestamp')

        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(df.copy())
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2

        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bybitTestnetDayTrend['haColor'] = df_ha['color'][-1]
        # 計算技術指標
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bybitParams['atr_d'])
        bybitTestnetDayTrend['atr'] = np.around(df_atr[-1], decimals=1)
        # 更新資料庫
        CMD = "UPDATE Bybit_Testnet SET color_d=%d, atr_d=%.1f" % (
            bybitTestnetDayTrend['haColor'], bybitTestnetDayTrend['atr'])
        cusor = bybitTestnetDayTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bybitTestnetDayTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('bybit_testnet_dtrend', 'warning', str(e.args))


# Bybit Testnet 小時線級別技術指標及趨勢計算 (HeikinAshi/ATR/EMA/RSI/MDD/RenkoBar/Flat)
@tl.job(interval=timedelta(seconds=interval['bybit_testnet_h']))
def bybit_testnet_hour_trend_update():
    try:
        # 建立資料庫寫入器
        if bybitTestnetHourTrend['sqlWriter'] is None:
            bybitTestnetHourTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        kline = bybit_testnet_excg.fetch_ohlcv(symbol=Bybit['symbol'], since=int(time.time()) - (3600 * 199), timeframe='1h')
        ts_to_date = [time.strftime("%Y-%m-%d %H:%S", time.gmtime(x[0])) for x in kline]  # 轉換 timestamp to datatime
        df = pd.DataFrame(kline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(ts_to_date)
        df2 = pd.DataFrame(df.copy())  # 還沒重設index前複制給renko用
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        bybitTestnetHourTrend['lastPrice'] = df['close'][-1]
        # 計算mdd及破前高破前低
        bybitTestnetHourTrend['mddh'] = pd.Series(df['high'][200 - bybitParams['mdd']:-1]).max()
        bybitTestnetHourTrend['mddl'] = pd.Series(df['low'][200 - bybitParams['mdd']:-1]).min()
        bybitTestnetHourTrend['mdd'] = bybitTestnetHourTrend['mddh'] - bybitTestnetHourTrend['mddl']
        if bybitTestnetHourTrend['lastPrice'] > bybitTestnetHourTrend['mddh']:
            bybitTestnetHourTrend['overmdd'] = 1
            bybitTestnetHourTrend['lowermdd'] = 1
        elif bybitTestnetHourTrend['lastPrice'] < bybitTestnetHourTrend['mddl']:
            bybitTestnetHourTrend['overmdd'] = 0
            bybitTestnetHourTrend['lowermdd'] = 1
        else:
            bybitTestnetHourTrend['overmdd'] = 0
            bybitTestnetHourTrend['lowermdd'] = 0

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(index=df.index.values, columns=['open', 'high', 'low', 'close'])
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2
        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bybitTestnetHourTrend['haColor'] = df_ha['color'][-1]

        # 計算 Renko1 及轉折訊號
        df2['date'] = df2['timestamp']
        renko1 = indicators.Renko(df2)
        renko1.brick_size = bybitParams['renko1']
        renko1.chart_type = indicators.Renko.PERIOD_CLOSE
        data1 = renko1.get_ohlc_data()
        data1 = data1.set_index('date')

        # data1 = data1.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        uptrend = list(data1['uptrend'])
        # 用時間降低突波誤判 (counter % catch)
        if uptrend[-2] is False and uptrend[-1] is True:
            bybitTestnetHourTrend['renko1Up'] = 1

        elif uptrend[-2] is True and uptrend[-1] is False:
            bybitTestnetHourTrend['renko1Up'] = 0
        else:
            bybitTestnetHourTrend['renko1Up'] = 99

        for index, i in enumerate(reversed(uptrend)):
            bybitTestnetRenkoBar1[str(index + 1)] = i

        # 偵測最後一根bar是否異動
        if bybitTestnetRenkoBar1['now'] != bybitTestnetRenkoBar1['last'] and bybitTestnetRenkoBar1['last'] is not None:
            bybitTestnetRenkoBar1['change'] = 'Market Moved!'
        elif bybitTestnetRenkoBar1['now'] == bybitTestnetRenkoBar1['last']:
            bybitTestnetRenkoBar1['change'] = None
        bybitTestnetRenkoBar1['last'] = bybitTestnetRenkoBar1['now']

        # 偵測停止訊號
        diff = df.index[-1] - data1.index[-1]
        bybitTestnetHourTrend['flat'] = diff.seconds / 3600

        # 計算 Renko2 及轉折訊號
        renko2 = indicators.Renko(df2)
        renko2.brick_size = bybitParams['renko2']
        renko2.chart_type = indicators.Renko.PERIOD_CLOSE
        data2 = renko2.get_ohlc_data()
        data2 = data2.set_index('date')

        # 用時間降低突波誤判 (counter % catch)
        if data2['uptrend'][-2] is False and data2['uptrend'][-1] is True:
            bybitTestnetHourTrend['renko2Up'] = 1

        elif data2['uptrend'][-2] is True and data2['uptrend'][-1] is False:
            bybitTestnetHourTrend['renko2Up'] = 0
        else:
            bybitTestnetHourTrend['renko2Up'] = 99

        # 顯示最後20筆 RenkoBar
        uptrend2 = list(data2['uptrend'])
        for index, i in enumerate(uptrend2):
            bybitTestnetRenkoBar2[str(index + 1)] = i
        bybitTestnetRenkoBar2['now'] = data2['close'][-1]
        # 偵測最後一根bar是否異動
        if bybitTestnetRenkoBar2['now'] != bybitTestnetRenkoBar2['last'] and bybitTestnetRenkoBar2['last'] is not None:
            bybitTestnetRenkoBar2['change'] = 'Market Moved!'
        elif bybitTestnetRenkoBar2['now'] == bybitTestnetRenkoBar2['last']:
            bybitTestnetRenkoBar2['change'] = None
        bybitTestnetRenkoBar2['last'] = bybitTestnetRenkoBar2['now']
        # 計算指標
        df_ema = talib.EMA(df['close'].values, bybitParams['ema'])
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bybitParams['atr'])
        df_rsi = talib.RSI(df['close'], bybitParams['rsi'])

        bybitTestnetHourTrend['ema'] = np.around(df_ema[-1], decimals=1)
        bybitTestnetHourTrend['atr'] = np.around(df_atr[-1], decimals=1)
        bybitTestnetHourTrend['rsi'] = np.around(df_rsi[-1], decimals=1)
        # 判斷首倉及進場時機
        # ema
        if bybitTestnetHourTrend['lastPrice'] > bybitTestnetHourTrend['ema']:
            bybitTestnetHourTrend['trendUp'] = 1
        else:
            bybitTestnetHourTrend['trendUp'] = 0
        # rsi
        if bybitTestnetHourTrend['rsi'] > 70:
            bybitTestnetHourTrend['overBuy'] = 1
            bybitTestnetHourTrend['overSell'] = 0
        elif bybitTestnetHourTrend['rsi'] < 30:
            bybitTestnetHourTrend['overBuy'] = 0
            bybitTestnetHourTrend['overSell'] = 1
        else:
            bybitTestnetHourTrend['overBuy'] = 0
            bybitTestnetHourTrend['overSell'] = 0
        # 更新資料庫
        CMD = "UPDATE Bybit_Testnet SET " \
              "lastprice=%.1f, mddh=%.1f, mddl=%.1f, mdd=%.1f, overmdd=%d, lowermdd=%d, color_h=%d, renko1up=%d, " \
              "renko2up=%d, flat=%d, ema=%.1f, atr_h=%.1f, rsi=%.1f, trend=%d, overbuy=%d, oversell=%d" % \
              (bybitTestnetHourTrend['lastPrice'], bybitTestnetHourTrend['mddh'], bybitTestnetHourTrend['mddl'],
               bybitTestnetHourTrend['mdd'], bybitTestnetHourTrend['overmdd'], bybitTestnetHourTrend['lowermdd'],
               bybitTestnetHourTrend['haColor'], bybitTestnetHourTrend['renko1Up'], bybitTestnetHourTrend['renko2Up'],
               bybitTestnetHourTrend['flat'], bybitTestnetHourTrend['ema'], bybitTestnetHourTrend['atr'],
               bybitTestnetHourTrend['rsi'], bybitTestnetHourTrend['trendUp'], bybitTestnetHourTrend['overBuy'],
               bybitTestnetHourTrend['overSell'])
        cusor = bybitTestnetHourTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bybitTestnetHourTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('htrend', 'warning', str(e.args))
    except KeyboardInterrupt:
        tl.stop()
        sys.exit()


# Binance 日線級別技術指標及趨勢計算 (HeikinAshi/ATR)
@tl.job(interval=timedelta(seconds=interval['bnc_d']))
def bnc_day_trend_update():
    try:
        # 建立資料庫寫入器
        if bncDayTrend['sqlWriter'] is None:
            bncDayTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        bnc_excg_long.load_markets()
        kline = bnc_excg_long.fetch_ohlcv(symbol='BTC/USDT', timeframe='1d')
        raw = pd.DataFrame(kline)
        df = pd.DataFrame()
        df['timestamp'] = raw[0]
        df['open'] = raw[1]
        df['high'] = raw[2]
        df['low'] = raw[3]
        df['close'] = raw[4]
        df['volume'] = raw[5]
        # df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp']
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(df.copy())
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2

        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bncDayTrend['haColor'] = df_ha['color'][-1]

        # 計算技術指標
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bncParams['atr_d'])
        bncDayTrend['atr'] = np.around(df_atr[-1], decimals=1)

        # 更新資料庫
        CMD = "UPDATE Binance SET color_d=%d, atr_d=%.1f" % (bncDayTrend['haColor'], bncDayTrend['atr'])
        cusor = bncDayTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bncDayTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('bnc_dtrend', 'warning', str(e.args))


# Binance 小時線級別技術指標及趨勢計算 (HeikinAshi/ATR/EMA/RSI/MDD/RenkoBar/Flat)
@tl.job(interval=timedelta(seconds=interval['bnc_h']))
def hour_trend_update():
    try:
        # 建立資料庫寫入器
        if bncHourTrend['sqlWriter'] is None:
            bncHourTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        bnc_excg_long.load_markets()
        kline = bnc_excg_long.fetch_ohlcv(symbol='BTC/USDT', timeframe='1h')
        df = pd.DataFrame(kline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df2 = pd.DataFrame(df.copy())  # 還沒重設index前複制給renko用
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        bncHourTrend['lastPrice'] = df['close'][-1]

        # 計算mdd及破前高破前低
        bncHourTrend['mddh'] = pd.Series(df['high'][500 - bncParams['mdd']:-1]).max()
        bncHourTrend['mddl'] = pd.Series(df['low'][500 - bncParams['mdd']:-1]).min()
        bncHourTrend['mdd'] = np.round((bncHourTrend['mddh'] - bncHourTrend['mddl']) + 0.01, decimals=2)
        if bncHourTrend['lastPrice'] > bncHourTrend['mddh']:
            bncHourTrend['overmdd'] = 1
            bncHourTrend['lowermdd'] = 0
        elif bncHourTrend['lastPrice'] < bncHourTrend['mddl']:
            bncHourTrend['overmdd'] = 0
            bncHourTrend['lowermdd'] = 1
        else:
            bncHourTrend['overmdd'] = 0
            bncHourTrend['lowermdd'] = 0

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(index=df.index.values, columns=['open', 'high', 'low', 'close'])
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2
        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bncHourTrend['haColor'] = df_ha['color'][-1]

        # 計算 Renko1 及轉折訊號
        df2['date'] = df2['timestamp']
        renko1 = indicators.Renko(df2)
        renko1.brick_size = bncParams['renko1']
        renko1.chart_type = indicators.Renko.PERIOD_CLOSE
        data1 = renko1.get_ohlc_data()
        data1 = data1.set_index('date')
        if data1['uptrend'][-2] is False and data1['uptrend'][-1] is True:
            bncHourTrend['renko1Up'] = 1

        elif data1['uptrend'][-2] is True and data1['uptrend'][-1] is False:
            bncHourTrend['renko1Up'] = 0
        else:
            bncHourTrend['renko1Up'] = 99

        # 顯示最後10筆 RenkoBar
        bncRenkoBar1['1'] = data1['uptrend'][-20]
        bncRenkoBar1['2'] = data1['uptrend'][-19]
        bncRenkoBar1['3'] = data1['uptrend'][-18]
        bncRenkoBar1['4'] = data1['uptrend'][-17]
        bncRenkoBar1['5'] = data1['uptrend'][-16]
        bncRenkoBar1['6'] = data1['uptrend'][-15]
        bncRenkoBar1['7'] = data1['uptrend'][-14]
        bncRenkoBar1['8'] = data1['uptrend'][-13]
        bncRenkoBar1['9'] = data1['uptrend'][-12]
        bncRenkoBar1['10'] = data1['uptrend'][-11]
        bncRenkoBar1['11'] = data1['uptrend'][-10]
        bncRenkoBar1['12'] = data1['uptrend'][-9]
        bncRenkoBar1['13'] = data1['uptrend'][-8]
        bncRenkoBar1['14'] = data1['uptrend'][-7]
        bncRenkoBar1['15'] = data1['uptrend'][-6]
        bncRenkoBar1['16'] = data1['uptrend'][-5]
        bncRenkoBar1['17'] = data1['uptrend'][-4]
        bncRenkoBar1['18'] = data1['uptrend'][-3]
        bncRenkoBar1['19'] = data1['uptrend'][-2]
        bncRenkoBar1['20'] = data1['uptrend'][-1]
        bncRenkoBar1['now'] = data1['close'][-1]

        # 偵測最後一根bar是否異動
        if bncRenkoBar1['now'] != bncRenkoBar1['last'] and bncRenkoBar1['last'] is not None:
            bncRenkoBar1['change'] = 'Market Moved!'
        elif bncRenkoBar1['now'] == bncRenkoBar1['last']:
            bncRenkoBar1['change'] = None
        bncRenkoBar1['last'] = bncRenkoBar1['now']

        # 偵測停止訊號
        diff = df.index[-1] - data1.index[-1]
        bncHourTrend['flat'] = diff.seconds / 3600

        # 計算 Renko2 及轉折訊號
        renko2 = indicators.Renko(df2)
        renko2.brick_size = bncParams['renko2']
        renko2.chart_type = indicators.Renko.PERIOD_CLOSE
        data2 = renko2.get_ohlc_data()
        data2 = data2.set_index('date')
        if data2['uptrend'][-2] is False and data2['uptrend'][-1] is True:
            bncHourTrend['renko2Up'] = 1

        elif data2['uptrend'][-2] is True and data2['uptrend'][-1] is False:
            bncHourTrend['renko2Up'] = 0
        else:
            bncHourTrend['renko2Up'] = 99

        # 顯示最後10筆 RenkoBar
        bncRenkoBar2['1'] = data2['uptrend'][-20]
        bncRenkoBar2['2'] = data2['uptrend'][-19]
        bncRenkoBar2['3'] = data2['uptrend'][-18]
        bncRenkoBar2['4'] = data2['uptrend'][-17]
        bncRenkoBar2['5'] = data2['uptrend'][-16]
        bncRenkoBar2['6'] = data2['uptrend'][-15]
        bncRenkoBar2['7'] = data2['uptrend'][-14]
        bncRenkoBar2['8'] = data2['uptrend'][-13]
        bncRenkoBar2['9'] = data2['uptrend'][-12]
        bncRenkoBar2['10'] = data2['uptrend'][-11]
        bncRenkoBar2['11'] = data2['uptrend'][-10]
        bncRenkoBar2['12'] = data2['uptrend'][-9]
        bncRenkoBar2['13'] = data2['uptrend'][-8]
        bncRenkoBar2['14'] = data2['uptrend'][-7]
        bncRenkoBar2['15'] = data2['uptrend'][-6]
        bncRenkoBar2['16'] = data2['uptrend'][-5]
        bncRenkoBar2['17'] = data2['uptrend'][-4]
        bncRenkoBar2['18'] = data2['uptrend'][-3]
        bncRenkoBar2['19'] = data2['uptrend'][-2]
        bncRenkoBar2['20'] = data2['uptrend'][-1]

        bncRenkoBar2['now'] = data2['close'][-1]
        # 偵測最後一根bar是否異動
        if bncRenkoBar2['now'] != bncRenkoBar2['last'] and bncRenkoBar2['last'] is not None:
            bncRenkoBar2['change'] = 'Market Moved!'
        elif bncRenkoBar2['now'] == bncRenkoBar2['last']:
            bncRenkoBar2['change'] = None
        bncRenkoBar2['last'] = bncRenkoBar2['now']

        # 計算指標
        df_ema = talib.EMA(df['close'].values, bncParams['ema'])
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bncParams['atr'])
        df_rsi = talib.RSI(df['close'], bncParams['rsi'])

        bncHourTrend['ema'] = np.around(df_ema[-1], decimals=1)
        bncHourTrend['atr'] = np.around(df_atr[-1], decimals=1)
        bncHourTrend['rsi'] = np.around(df_rsi[-1], decimals=1)

        # 判斷首倉及進場時機
        # ema
        if bncHourTrend['lastPrice'] > bncHourTrend['ema']:
            bncHourTrend['trendUp'] = 1
        else:
            bncHourTrend['trendUp'] = 0
        # rsi
        if bncHourTrend['rsi'] > 70:
            bncHourTrend['overBuy'] = 1
            bncHourTrend['overSell'] = 0
        elif bncHourTrend['rsi'] < 30:
            bncHourTrend['overBuy'] = 0
            bncHourTrend['overSell'] = 1
        else:
            bncHourTrend['overBuy'] = 0
            bncHourTrend['overSell'] = 0
        # 更新資料庫
        CMD = "UPDATE Binance SET " \
              "lastprice=%.2f, mddh=%.1f, mddl=%.1f, mdd=%.1f, overmdd=%d, lowermdd=%d, color_h=%d, renko1up=%d, " \
              "renko2up=%d, flat=%d, ema=%.1f, atr_h=%.1f, rsi=%.1f, trend=%d, overbuy=%d, oversell=%d" % \
              (bncHourTrend['lastPrice'], bncHourTrend['mddh'], bncHourTrend['mddl'], bncHourTrend['mdd'], bncHourTrend['overmdd'], bncHourTrend['lowermdd'],
               bncHourTrend['haColor'], bncHourTrend['renko1Up'], bncHourTrend['renko2Up'], bncHourTrend['flat'], bncHourTrend['ema'], bncHourTrend['atr'],
               bncHourTrend['rsi'], bncHourTrend['trendUp'], bncHourTrend['overBuy'], bncHourTrend['overSell'])
        cusor = bncHourTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bncHourTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('htrend', 'warning', e.args)
    except KeyboardInterrupt:
        tl.stop()
        sys.exit()


# Binance Testnet 日線級別技術指標及趨勢計算 (HeikinAshi/ATR)
@tl.job(interval=timedelta(seconds=interval['bnc_testnet_d']))
def bnc_Testnet_day_trend_update():
    try:
        # 建立資料庫寫入器
        if bncTestnetDayTrend['sqlWriter'] is None:
            bncTestnetDayTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        bnc_testnet_excg_long.load_markets()
        kline = bnc_testnet_excg_long.fetch_ohlcv(symbol='BTC/USDT', timeframe='1d')
        raw = pd.DataFrame(kline)
        df = pd.DataFrame()
        df['timestamp'] = raw[0]
        df['open'] = raw[1]
        df['high'] = raw[2]
        df['low'] = raw[3]
        df['close'] = raw[4]
        df['volume'] = raw[5]
        # df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp']
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(df.copy())
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2

        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bncTestnetDayTrend['haColor'] = df_ha['color'][-1]

        # 計算技術指標
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bncTestnetParams['atr_d'])
        bncTestnetDayTrend['atr'] = np.around(df_atr[-1], decimals=1)

        # 更新資料庫
        CMD = "UPDATE Binance_Testnet SET color_d=%d, atr_d=%.1f" % (bncTestnetDayTrend['haColor'], bncTestnetDayTrend['atr'])
        cusor = bncTestnetDayTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bncTestnetDayTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('dtrend', 'warning', e.args)
    except KeyboardInterrupt:
        tl.stop()
        sys.exit()


# Binance Testnet 小時線級別技術指標及趨勢計算 (HeikinAshi/ATR/EMA/RSI/MDD/RenkoBar/Flat)
@tl.job(interval=timedelta(seconds=interval['bnc_testnet_h']))
def bnc_testnet_hour_trend_update():
    try:
        # 建立資料庫寫入器
        if bncTestnetHourTrend['sqlWriter'] is None:
            bncTestnetHourTrend['sqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 從API拉資訊下來處理成DF資料
        bnc_testnet_excg_long.load_markets()
        kline = bnc_testnet_excg_long.fetch_ohlcv(symbol='BTC/USDT', timeframe='1h')
        df = pd.DataFrame(kline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.reset_index(drop=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df2 = pd.DataFrame(df.copy())  # 還沒重設index前複制給renko用
        df = df.set_index('timestamp')
        # df = df.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        bncTestnetHourTrend['lastPrice'] = df['close'][-1]

        # 計算mdd及破前高破前低
        bncTestnetHourTrend['mddh'] = pd.Series(df['high'][500 - bncTestnetParams['mdd']:-1]).max()
        bncTestnetHourTrend['mddl'] = pd.Series(df['low'][500 - bncTestnetParams['mdd']:-1]).min()
        bncTestnetHourTrend['mdd'] = np.round((bncTestnetHourTrend['mddh'] - bncTestnetHourTrend['mddl']) + 0.01, decimals=2)
        if bncTestnetHourTrend['lastPrice'] > bncTestnetHourTrend['mddh']:
            bncTestnetHourTrend['overmdd'] = 1
            bncTestnetHourTrend['lowermdd'] = 0
        elif bncTestnetHourTrend['lastPrice'] < bncTestnetHourTrend['mddl']:
            bncTestnetHourTrend['overmdd'] = 0
            bncTestnetHourTrend['lowermdd'] = 1
        else:
            bncTestnetHourTrend['overmdd'] = 0
            bncTestnetHourTrend['lowermdd'] = 0

        # 計算HeikinAshi縫線資料及顏色
        df_ha = pd.DataFrame(index=df.index.values, columns=['open', 'high', 'low', 'close'])
        df_ha['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        for i in range(len(df)):
            if i == 0:
                df_ha.iat[0, 0] = df['open'].iloc[0]
            else:
                df_ha.iat[i, 0] = (df_ha.iat[i - 1, 0] + df_ha.iat[i - 1, 3]) / 2
        df_ha['high'] = df_ha.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        df_ha['low'] = df_ha.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        df_ha['color'] = np.where(df_ha['open'] < df_ha['close'], 1, 0)
        bncTestnetHourTrend['haColor'] = df_ha['color'][-1]

        # 計算 Renko1 及轉折訊號
        df2['date'] = df2['timestamp']
        renko1 = indicators.Renko(df2)
        renko1.brick_size = bncTestnetParams['renko1']
        renko1.chart_type = indicators.Renko.PERIOD_CLOSE
        data1 = renko1.get_ohlc_data()
        data1 = data1.set_index('date')
        # data1 = data1.tz_convert(None)  # 清除時區，pd新版本會偵測df的時區格式(tz_native or tz_aware)
        if data1['uptrend'][-2] is False and data1['uptrend'][-1] is True:
            bncTestnetHourTrend['renko1Up'] = 1
        elif data1['uptrend'][-2] is True and data1['uptrend'][-1] is False:
            bncTestnetHourTrend['renko1Up'] = 0
        else:
            bncTestnetHourTrend['renko1Up'] = 99

        # 顯示最後10筆 RenkoBar
        bncTestnetRenkoBar1['1'] = data1['uptrend'][-20]
        bncTestnetRenkoBar1['2'] = data1['uptrend'][-19]
        bncTestnetRenkoBar1['3'] = data1['uptrend'][-18]
        bncTestnetRenkoBar1['4'] = data1['uptrend'][-17]
        bncTestnetRenkoBar1['5'] = data1['uptrend'][-16]
        bncTestnetRenkoBar1['6'] = data1['uptrend'][-15]
        bncTestnetRenkoBar1['7'] = data1['uptrend'][-14]
        bncTestnetRenkoBar1['8'] = data1['uptrend'][-13]
        bncTestnetRenkoBar1['9'] = data1['uptrend'][-12]
        bncTestnetRenkoBar1['10'] = data1['uptrend'][-11]
        bncTestnetRenkoBar1['11'] = data1['uptrend'][-10]
        bncTestnetRenkoBar1['12'] = data1['uptrend'][-9]
        bncTestnetRenkoBar1['13'] = data1['uptrend'][-8]
        bncTestnetRenkoBar1['14'] = data1['uptrend'][-7]
        bncTestnetRenkoBar1['15'] = data1['uptrend'][-6]
        bncTestnetRenkoBar1['16'] = data1['uptrend'][-5]
        bncTestnetRenkoBar1['17'] = data1['uptrend'][-4]
        bncTestnetRenkoBar1['18'] = data1['uptrend'][-3]
        bncTestnetRenkoBar1['19'] = data1['uptrend'][-2]
        bncTestnetRenkoBar1['20'] = data1['uptrend'][-1]
        bncTestnetRenkoBar1['now'] = data1['close'][-1]

        # 偵測最後一根bar是否異動
        if bncTestnetRenkoBar1['now'] != bncTestnetRenkoBar1['last'] and bncTestnetRenkoBar1['last'] is not None:
            bncTestnetRenkoBar1['change'] = 'Market Moved!'
        elif bncTestnetRenkoBar1['now'] == bncTestnetRenkoBar1['last']:
            bncTestnetRenkoBar1['change'] = None
        bncTestnetRenkoBar1['last'] = bncTestnetRenkoBar1['now']

        # 偵測停止訊號
        diff = df.index[-1] - data1.index[-1]
        bncTestnetHourTrend['flat'] = diff.seconds / 3600

        # 計算 Renko2 及轉折訊號
        renko2 = indicators.Renko(df2)
        renko2.brick_size = bncTestnetParams['renko2']
        renko2.chart_type = indicators.Renko.PERIOD_CLOSE
        data2 = renko2.get_ohlc_data()
        data2 = data2.set_index('date')
        if data2['uptrend'][-2] is False and data2['uptrend'][-1] is True:
            bncTestnetHourTrend['renko2Up'] = 1

        elif data2['uptrend'][-2] is True and data2['uptrend'][-1] is False:
            bncTestnetHourTrend['renko2Up'] = 0
        else:
            bncTestnetHourTrend['renko2Up'] = 99

        # 顯示最後10筆 RenkoBar
        bncTestnetRenkoBar2['1'] = data2['uptrend'][-20]
        bncTestnetRenkoBar2['2'] = data2['uptrend'][-19]
        bncTestnetRenkoBar2['3'] = data2['uptrend'][-18]
        bncTestnetRenkoBar2['4'] = data2['uptrend'][-17]
        bncTestnetRenkoBar2['5'] = data2['uptrend'][-16]
        bncTestnetRenkoBar2['6'] = data2['uptrend'][-15]
        bncTestnetRenkoBar2['7'] = data2['uptrend'][-14]
        bncTestnetRenkoBar2['8'] = data2['uptrend'][-13]
        bncTestnetRenkoBar2['9'] = data2['uptrend'][-12]
        bncTestnetRenkoBar2['10'] = data2['uptrend'][-11]
        bncTestnetRenkoBar2['11'] = data2['uptrend'][-10]
        bncTestnetRenkoBar2['12'] = data2['uptrend'][-9]
        bncTestnetRenkoBar2['13'] = data2['uptrend'][-8]
        bncTestnetRenkoBar2['14'] = data2['uptrend'][-7]
        bncTestnetRenkoBar2['15'] = data2['uptrend'][-6]
        bncTestnetRenkoBar2['16'] = data2['uptrend'][-5]
        bncTestnetRenkoBar2['17'] = data2['uptrend'][-4]
        bncTestnetRenkoBar2['18'] = data2['uptrend'][-3]
        bncTestnetRenkoBar2['19'] = data2['uptrend'][-2]
        bncTestnetRenkoBar2['20'] = data2['uptrend'][-1]

        bncTestnetRenkoBar2['now'] = data2['close'][-1]
        # 偵測最後一根bar是否異動
        if bncTestnetRenkoBar2['now'] != bncTestnetRenkoBar2['last'] and bncTestnetRenkoBar2['last'] is not None:
            bncTestnetRenkoBar2['change'] = 'Market Moved!'
        elif bncTestnetRenkoBar2['now'] == bncTestnetRenkoBar2['last']:
            bncTestnetRenkoBar2['change'] = None
        bncTestnetRenkoBar2['last'] = bncTestnetRenkoBar2['now']

        # 計算指標
        df_ema = talib.EMA(df['close'].values, bncTestnetParams['ema'])
        df_atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, bncTestnetParams['atr'])
        df_rsi = talib.RSI(df['close'], bncTestnetParams['rsi'])

        bncTestnetHourTrend['ema'] = np.around(df_ema[-1], decimals=1)
        bncTestnetHourTrend['atr'] = np.around(df_atr[-1], decimals=1)
        bncTestnetHourTrend['rsi'] = np.around(df_rsi[-1], decimals=1)

        # 判斷首倉及進場時機
        # ema
        if bncTestnetHourTrend['lastPrice'] > bncTestnetHourTrend['ema']:
            bncTestnetHourTrend['trendUp'] = 1
        else:
            bncTestnetHourTrend['trendUp'] = 0
        # rsi
        if bncTestnetHourTrend['rsi'] > 70:
            bncTestnetHourTrend['overBuy'] = 1
            bncTestnetHourTrend['overSell'] = 0
        elif bncTestnetHourTrend['rsi'] < 30:
            bncTestnetHourTrend['overBuy'] = 0
            bncTestnetHourTrend['overSell'] = 1
        else:
            bncTestnetHourTrend['overBuy'] = 0
            bncTestnetHourTrend['overSell'] = 0
        # 更新資料庫
        CMD = "UPDATE Binance_Testnet SET " \
              "lastprice=%.2f, mddh=%.1f, mddl=%.1f, mdd=%.1f, overmdd=%d, lowermdd=%d, color_h=%d, renko1up=%d, " \
              "renko2up=%d, flat=%d, ema=%.1f, atr_h=%.1f, rsi=%.1f, trend=%d, overbuy=%d, oversell=%d" % \
              (bncTestnetHourTrend['lastPrice'], bncTestnetHourTrend['mddh'], bncTestnetHourTrend['mddl'], bncTestnetHourTrend['mdd'], bncTestnetHourTrend['overmdd'], bncTestnetHourTrend['lowermdd'],
               bncTestnetHourTrend['haColor'], bncTestnetHourTrend['renko1Up'], bncTestnetHourTrend['renko2Up'], bncTestnetHourTrend['flat'], bncTestnetHourTrend['ema'], bncTestnetHourTrend['atr'],
               bncTestnetHourTrend['rsi'], bncTestnetHourTrend['trendUp'], bncTestnetHourTrend['overBuy'], bncTestnetHourTrend['overSell'])
        cusor = bncTestnetHourTrend['sqlWriter'].cursor()
        cusor.execute(CMD)
        bncTestnetHourTrend['sqlWriter'].commit()

    except Exception as e:
        logger.log('htrend', 'warning', e.args)
    except KeyboardInterrupt:
        tl.stop()
        sys.exit()


# Binance Websocket 即時串流價格
# @tl.job(interval=timedelta(seconds=0))
def binance_wss():
    try:
        # 建立資料庫寫入器
        if bncHourTrend['wssSqlWriter'] is None:
            bncHourTrend['wssSqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 第一次連線
        if bncHourTrend['wss'] is None:
            bncHourTrend['wss'] = create_connection("wss://fstream.binance.com/ws")
            logger.log('wss', 'info', 'Binance Websocket 連線中...')
            time.sleep(0.1)
            bncHourTrend['wss'].send(json.dumps({"method": "SUBSCRIBE", "params": ["btcusdt@trade", "btcusdt@ticker"], "id": int(time.strftime('%Y%m%d%H%M%S', time.localtime()))}))
            time.sleep(0.1)
            if bncHourTrend['wss'].connected:
                logger.log('wss', 'info', 'Binance Websocket 已連線')
        # wss 物件存在時
        elif bncHourTrend['wss'] is not None:
            # 連線狀態時更新價格
            if bncHourTrend['wss'].connected or bncHourTrend['wss'].sock:
                raw = json.loads(bncHourTrend['wss'].recv())
                if 'e' in raw:
                    if raw['e'] == 'trade':
                        bncHourTrend['trade'] = float(raw['p'])
                        bncHourTrend['bestBid'] = np.round(bncHourTrend['trade'] - 0.01, decimals=2)
                        bncHourTrend['bestAsk'] = np.round(bncHourTrend['trade'] + 0.01, decimals=2)
            # 更新資料庫
            if bncHourTrend['trade']:
                CMD = "UPDATE Binance SET trade=%.2f, bestbid=%.2f, bestask=%.2f" % (bncHourTrend['trade'], bncHourTrend['bestBid'], bncHourTrend['bestAsk'])
                cusor = bncHourTrend['wssSqlWriter'].cursor()
                cusor.execute(CMD)
                bncHourTrend['wssSqlWriter'].commit()

            # 斷線時重新連線
            if bncHourTrend['wss'].connected is False or bncHourTrend['wss'].sock is None:
                logger.log('wss', 'info', 'Binance Websocket 已斷線，5秒後重新連線...' + str(5 - bncHourTrend['wssTimeOut']))
                bncHourTrend['wssTimeOut'] += 1
                time.sleep(1)
                if bncHourTrend['wssTimeOut'] == 5:
                    bncHourTrend['wss'].close()
                    bncHourTrend['wss'] = None
                    bncHourTrend['wssTimeOut'] = 0

    except Exception as e:
        logger.log('bnc_wss', 'warning', str(e.args))


# Websocket 即時串流
@tl.job(interval=timedelta(seconds=0))
def binance_testnet_wss():
    try:
        # 建立資料庫寫入器
        if bncTestnetHourTrend['wssSqlWriter'] is None:
            bncTestnetHourTrend['wssSqlWriter'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        # 第一次連線
        if bncTestnetHourTrend['wss'] is None:
            bncTestnetHourTrend['wss'] = create_connection("wss://fstream.binance.com/ws")
            logger.log('wss', 'info', 'Binance Testnet Websocket 連線中...')
            time.sleep(0.1)
            bncTestnetHourTrend['wss'].send(json.dumps({"method": "SUBSCRIBE", "params": ["btcusdt@trade", "btcusdt@ticker"], "id": int(time.strftime('%Y%m%d%H%M%S', time.localtime()))}))
            time.sleep(0.1)
            if bncTestnetHourTrend['wss'].connected:
                logger.log('wss', 'info', 'Binance Testnet Websocket 已連線')
        # wss 物件存在時
        elif bncTestnetHourTrend['wss'] is not None:
            # 連線狀態時更新價格
            if bncTestnetHourTrend['wss'].connected or bncTestnetHourTrend['wss'].sock:
                raw = json.loads(bncTestnetHourTrend['wss'].recv())
                if 'e' in raw:
                    if raw['e'] == 'trade':
                        bncTestnetHourTrend['trade'] = float(raw['p'])
                        bncTestnetHourTrend['bestBid'] = np.round(bncTestnetHourTrend['trade'] - 0.01, decimals=2)
                        bncTestnetHourTrend['bestAsk'] = np.round(bncTestnetHourTrend['trade'] + 0.01, decimals=2)
            # 更新資料庫
            if bncTestnetHourTrend['trade']:
                CMD = "UPDATE Binance_Testnet SET trade=%.2f, bestbid=%.2f, bestask=%.2f" % (bncTestnetHourTrend['trade'], bncTestnetHourTrend['bestBid'], bncTestnetHourTrend['bestAsk'])
                cusor = bncTestnetHourTrend['wssSqlWriter'].cursor()
                cusor.execute(CMD)
                bncTestnetHourTrend['wssSqlWriter'].commit()

            # 斷線時重新連線
            if bncTestnetHourTrend['wss'].connected is False or bncTestnetHourTrend['wss'].sock is None:
                logger.log('wss', 'info', 'Binance Testnet Websocket 已斷線，5秒後重新連線...' + str(5 - bncTestnetHourTrend['wssTimeOut']))
                bncTestnetHourTrend['wssTimeOut'] += 1
                time.sleep(1)
                if bncTestnetHourTrend['wssTimeOut'] == 5:
                    bncTestnetHourTrend['wss'].close()
                    bncTestnetHourTrend['wss'] = None
                    bncTestnetHourTrend['wssTimeOut'] = 0

    except Exception as e:
        logger.log('bnc_testnet_wss', 'warning', str(e.args))


# 資訊列印
@tl.job(interval=(timedelta(seconds=1)))
def print_info():
    try:
        # 建立資料庫讀取器
        if systemParams['printSqlReader'] is None:
            systemParams['printSqlReader'] = sqlite3.connect(r'SQLite\HAII_Server.db')
        else:
            reader = systemParams['printSqlReader'].cursor()
            reader.execute("SELECT * FROM BitMEX LIMIT 1")
            log = reader.fetchall()
            log_bitmex = '價格:%s|趨勢:%s|超標:%s|低標:%s|破前高:%s|破前低:%s|停止指數:%s\n' \
                         '日縫線:%s|時縫線:%s|Renko1:%s|Renko2:%s|EMA:%s|RSI:%s|mddh:%s|mddl:%s|mdd:%s' % \
                         (log[0][2], log[0][15], log[0][16], log[0][17], log[0][6], log[0][7], log[0][11],
                          log[0][0], log[0][8], log[0][9], log[0][10], log[0][12], log[0][14], log[0][3], log[0][4], log[0][5])
            reader.execute("SELECT * FROM Bybit_Testnet LIMIT 1")
            log = reader.fetchall()
            log_bybit_testnet = '價格:%s|趨勢:%s|超標:%s|低標:%s|破前高:%s|破前低:%s|停止指數:%s\n' \
                                '日縫線:%s|時縫線:%s|Renko1:%s|Renko2:%s|EMA:%s|RSI:%s|mddh:%s|mddl:%s|mdd:%s' % \
                                (log[0][2], log[0][15], log[0][16], log[0][17], log[0][6], log[0][7], log[0][11],
                                 log[0][0], log[0][8], log[0][9], log[0][10], log[0][12], log[0][14], log[0][3], log[0][4],
                                 log[0][5])
            # reader.execute("SELECT * FROM BitMEX_Testnet LIMIT 1")
            # log = reader.fetchall()
            # log_bitmex_testnet = '價格:%s|趨勢:%s|超標:%s|低標:%s|破前高:%s|破前低:%s|停止指數:%s\n' \
            #                      '日縫線:%s|時縫線:%s|Renko1:%s|Renko2:%s|EMA:%s|RSI:%s|mddh:%s|mddl:%s|mdd:%s' % \
            #                      (log[0][2], log[0][15], log[0][16], log[0][17], log[0][6], log[0][7], log[0][11],
            #                       log[0][0], log[0][8], log[0][9], log[0][10], log[0][12], log[0][14], log[0][3], log[0][4], log[0][5])
            reader.execute("SELECT * FROM Binance LIMIT 1")
            log = reader.fetchall()
            log_binance = '價格:%s|趨勢:%s|超標:%s|低標:%s|破前高:%s|破前低:%s|停止指數:%s\n' \
                          '日縫線:%s|時縫線:%s|Renko1:%s|Renko2:%s|EMA:%s|RSI:%s|mddh:%s|mddl:%s|mdd:%s' % \
                          (log[0][2], log[0][15], log[0][16], log[0][17], log[0][6], log[0][7], log[0][11],
                           log[0][0], log[0][8], log[0][9], log[0][10], log[0][12], log[0][14], log[0][3], log[0][4], log[0][5])
            reader.execute("SELECT * FROM Binance_Testnet LIMIT 1")
            log = reader.fetchall()
            log_binance_testnet = '價格:%s|趨勢:%s|超標:%s|低標:%s|破前高:%s|破前低:%s|停止指數:%s\n' \
                                  '日縫線:%s|時縫線:%s|Renko1:%s|Renko2:%s|EMA:%s|RSI:%s|mddh:%s|mddl:%s|mdd:%s' % \
                                  (log[0][2], log[0][15], log[0][16], log[0][17], log[0][6], log[0][7], log[0][11],
                                   log[0][0], log[0][8], log[0][9], log[0][10], log[0][12], log[0][14], log[0][3], log[0][4], log[0][5])
            msg = ' 【 HAII 3.3 | Server 】\n'
            msg += '【 BitMEX 】-------------------------------------------------------------------------------\n'
            msg += log_bitmex + '\n'
            msg += '【 Bybit Testnet 】----------------------------------------------------------------------\n'
            msg += log_bybit_testnet + '\n'
            # msg += '【 BitMEX Testnet 】-----------------------------------------------------------------------\n'
            # msg += log_bitmex_testnet + '\n'
            msg += '【 Binance 】------------------------------------------------------------------------------\n'
            msg += log_binance + '\n'
            msg += '【 Binance Testnet 】----------------------------------------------------------------------\n'
            msg += log_binance_testnet + '\n'
            msg += '-------------------------------------------------------------------------------------------'
            logger.log('print', 'info', msg)
    except Exception as e:
        logger.log('print', 'warning', str(e.args))


tl.start()

while True:
    try:
        pass
    except Exception as e:
        print(e.args)
    except KeyboardInterrupt:
        bncHourTrend['wss'].close()
        bncTestnetHourTrend['wss'].close()
        tl.stop()
        sys.exit()
