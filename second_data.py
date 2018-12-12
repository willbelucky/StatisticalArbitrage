# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-12-03
"""
import pandas as pd
import matplotlib.pyplot as plt

ASK = 'ASK'
BID = 'BID'
ASK_MIN = 'ASK_min'
BID_MAX = 'BID_max'

etfs = [
    # 'IVV',
    'SPY', 'SPYG', 'SPYV', 'VOO', 'VOOG', 'VOOV']

DATETIME = 'DATETIME'

periods = [
    '201601',
    '201602',
    '201603',
    '201604',
    '201605',
    '201606',
    '201607',
    '201608',
    '201609',
    '201610',
    '201611',
    '201612'
]

for etf in etfs:
    result = pd.DataFrame()
    for period in periods:
        print('{}_{}.csv'.format(etf, period))
        raw = pd.read_csv('raw_data/{}_{}.csv'.format(etf, period))

        code = raw['SYM_ROOT'][0]

        raw[DATETIME] = raw['DATE'].astype(str) + ' ' + raw['TIME_M']
        raw[DATETIME] = pd.to_datetime(raw[DATETIME], format='%Y%m%d %H:%M:%S.%f')

        raw.dropna(subset=[ASK, BID], inplace=True)
        # Remove 0 value in ask and bid price.
        raw = raw.loc[(raw[ASK] > 1.0) & (raw[BID] > 1.0), :]
        # Remove ask price bigger than bid price.
        raw = raw.loc[(raw[BID] < raw[ASK]), :]
        raw = raw[[DATETIME, BID, ASK]]

        seconds = raw.groupby(pd.Grouper(key=DATETIME, freq='1s')).median()
        seconds = seconds.dropna()

        # ASK가 60초 동안 2.5% 이상 오르지 않음
        # BID가 60초 동안 2.5% 이상 떨어지지 않음
        seconds[ASK_MIN] = seconds[ASK].rolling(60).min()
        seconds = seconds.loc[seconds[ASK] < seconds[ASK_MIN] * 1.025, :]
        seconds[BID_MAX] = seconds[BID].rolling(60).max()
        seconds = seconds.loc[seconds[BID] > seconds[BID_MAX] * 0.975, :]

        # ASK가 10초 동안 1.0% 이상 오름
        # BID가 10초 동안 1.0% 이상 떨어짐
        seconds[ASK_MIN] = seconds[ASK].rolling(10).min()
        seconds = seconds.loc[seconds[ASK] < seconds[ASK_MIN] * 1.01, :]
        seconds[BID_MAX] = seconds[BID].rolling(10).max()
        seconds = seconds.loc[seconds[BID] > seconds[BID_MAX] * 0.99, :]

        # Remove before 9:35 and after 15:55
        seconds = seconds.between_time('9:35', '15:55')
        seconds = seconds[[BID, ASK]]

        result = pd.concat([result, seconds])

    result.plot()
    plt.title(etf)
    plt.savefig('second_image/{}.png'.format(etf))
    plt.show()
    result.to_csv('second_data/{}.csv'.format(etf))


