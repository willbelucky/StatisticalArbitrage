# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-27
"""
import pandas as pd
import matplotlib.pyplot as plt

DATETIME = 'DATETIME'

periods = [
    # '201601',
    # '201602',
    # '201603',
    # '201604',
    # '201605',
    # '201606',
    # '201607',
    # '201608',
    # '201609',
    '201610',
    # '201611',
    # '201612'
]


def _filter_data(raw) -> pd.DataFrame:
    code = raw['SYM_ROOT'][0]

    raw[DATETIME] = raw['DATE'].astype(str) + ' ' + raw['TIME_M']
    raw[DATETIME] = pd.to_datetime(raw[DATETIME], format='%Y%m%d %H:%M:%S.%f')
    raw.set_index(keys=[DATETIME], inplace=True)

    # Remove before 9:35 and after 15:55
    raw = raw.between_time('9:35', '15:55')

    raw.dropna(subset=['ASK', 'BID'], inplace=True)
    # Remove 0 value in ask and bid price.
    raw = raw.loc[(raw['ASK'] > 1.0) & (raw['BID'] > 1.0), :]
    # Remove ask price bigger than bid price.
    raw = raw.loc[(raw['BID'] < raw['ASK']), :]

    # Remove outliers
    pre_len = len(raw)
    i = 1
    while True:
        raw['ASK_ASK'] = raw['ASK'].pct_change()
        raw['BID_BID'] = raw['BID'].pct_change()

        raw = raw.loc[
              (raw['ASK_ASK'] < 0.025) &
              (raw['ASK_ASK'] > -0.025) &
              (raw['BID_BID'] < 0.025) &
              (raw['BID_BID'] > -0.025), :]

        next_len = len(raw)

        if pre_len == next_len or i == 100:
            break

        i += 1
        pre_len = next_len

    raw.drop(
        columns=['DATE', 'TIME_M', 'SYM_ROOT', 'SYM_SUFFIX', 'BIDSIZ', 'ASKSIZ', 'ASK_ASK', 'BID_BID'],
        inplace=True
    )
    raw.rename(index=str, columns={
        'ASK': code + '_ask',
        'BID': code + '_bid',
    }, inplace=True)

    raw.plot()
    plt.show()

    raw.reset_index(inplace=True)
    return raw


def filter_data(etf):
    for period in periods:
        print('{}_{}.csv'.format(etf, period))
        raw = pd.read_csv('raw_data/{}_{}.csv'.format(etf, period))
        concated = _filter_data(raw)
        concated.to_hdf('raw_data2/{}_{}.h5'.format(etf, period), key='df', format='table', mode='w')


# filter_data('IVV')
# filter_data('VOO')
filter_data('SPY')
# filter_data('SPYG')
# filter_data('VOOG')
# filter_data('SPYV')
# filter_data('VOOV')
