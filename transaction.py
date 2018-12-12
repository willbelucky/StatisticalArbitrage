# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-28
"""
import pandas as pd
import numpy as np
from copy import deepcopy

CLOSE_ASK = 'close_ask'
CLOSE_BID = 'close_bid'
OPEN_ASK = 'open_ask'
OPEN_BID = 'open_bid'
CLOSE_TIME = 'close_time'
OPEN_SIGNAL = 'open_signal'
CLOSE_SIGNAL = 'close_signal'
OPEN_TIME = 'open_time'
TIME = 'time'


def get_table(data: pd.DataFrame, open_bid, open_ask, close_bid, close_ask, ratio_1, ratio_2, criteria, tol):
    table = data.copy()
    table[TIME] = table.index
    table[OPEN_SIGNAL] = table[open_bid] / (ratio_1 * table[open_ask]) - 1
    table[CLOSE_SIGNAL] = table[close_bid] / (ratio_2 * table[close_ask]) - 1
    table[OPEN_TIME] = table.index
    table[CLOSE_TIME] = table.index
    table.rename(columns={open_bid: OPEN_BID, open_ask: OPEN_ASK,
                          close_bid: CLOSE_BID, close_ask: CLOSE_ASK}, inplace=True)

    open_table = table.loc[table[OPEN_SIGNAL] > criteria, [TIME, OPEN_TIME, OPEN_BID, OPEN_ASK]]
    print('Open: {}({}%)'.format(len(open_table), len(open_table) * 100 / len(table)))

    close_table = table.loc[np.abs(table[CLOSE_SIGNAL]) < tol, [TIME, CLOSE_TIME, CLOSE_BID, CLOSE_ASK]]
    print('Close: {}({}%)'.format(len(close_table), len(close_table) * 100 / len(table)))

    concat_table = open_table.merge(close_table, how='outer', on=TIME, sort=True)
    concat_table = concat_table.sort_values(by=TIME)
    concat_table[CLOSE_TIME] = concat_table[CLOSE_TIME].fillna(method='bfill')
    concat_table[CLOSE_BID] = concat_table[CLOSE_BID].fillna(method='bfill')
    concat_table[CLOSE_ASK] = concat_table[CLOSE_ASK].fillna(method='bfill')
    concat_table.dropna(inplace=True)
    concat_table.drop(columns=[TIME], inplace=True)
    return concat_table


def save_transactions(etf_1, etf_2, number=None, criteria=0.002, tol=0.00001):
    if number:
        file_name = '{}_{}_{}'.format(etf_1, etf_2, number)
    else:
        file_name = '{}_{}'.format(etf_1, etf_2)
    print(file_name)
    screened_data = pd.read_hdf('screened_data/{}.h5'.format(file_name), key='df')

    #     VOO - 0.91 * IVV
    #     IVV - 1.10 * VOO
    if etf_1 == 'IVV' and etf_2 == 'VOO':
        ratio_etf1_per_etf2 = 1.103
        ratio_etf2_per_etf1 = 1 / ratio_etf1_per_etf2

    #     SPY - 1.09 * VOO
    #     VOO - 0.92 * SPY
    elif etf_1 == 'SPY' and etf_2 == 'VOO':
        ratio_etf1_per_etf2 = 1.086869
        ratio_etf2_per_etf1 = 0.920049
    elif etf_1 == 'VOO' and etf_2 == 'SPY':
        ratio_etf1_per_etf2 = 0.920049
        ratio_etf2_per_etf1 = 1.086869

    #     SPY - 0.99 * IVV
    #     IVV - 1.01 * SPY
    elif etf_1 == 'SPY' and etf_2 == 'IVV':
        ratio_etf1_per_etf2 = 0.989176
        ratio_etf2_per_etf1 = 1.010909
    elif etf_1 == 'IVV' and etf_2 == 'SPY':
        ratio_etf1_per_etf2 = 1.010909
        ratio_etf2_per_etf1 = 0.989176

    #     SPYG - 0.96 * VOOG
    #     VOOG - 1.04 * SPYG
    elif etf_1 == 'SPYG' and etf_2 == 'VOOG':
        ratio_etf1_per_etf2 = 0.964036
        ratio_etf2_per_etf1 = 1.037193

    #     SPYV - 1.12 * VOOV
    #     VOOV - 0.89 * SPYV
    elif etf_1 == 'SPYV' and etf_2 == 'VOOV':
        ratio_etf1_per_etf2 = 1.124
        ratio_etf2_per_etf1 = 1 / ratio_etf1_per_etf2
    else:
        raise NameError('여기 없는 코드를 입력했습니다.')

    bid_1 = '{}_bid'.format(etf_1)
    ask_1 = '{}_ask'.format(etf_1)
    bid_2 = '{}_bid'.format(etf_2)
    ask_2 = '{}_ask'.format(etf_2)

    print('ETF 1 Overvalued')
    table_a = get_table(screened_data, bid_1, ask_2, bid_2, ask_1, ratio_etf1_per_etf2,
                        ratio_etf2_per_etf1, criteria, tol)
    print('ETF 2 Overvalued')
    table_b = get_table(screened_data, bid_2, ask_1, bid_1, ask_2, ratio_etf2_per_etf1,
                        ratio_etf1_per_etf2, criteria, tol)
    print('Total: {}'.format(len(screened_data)))

    table_c = pd.concat([table_a, table_b])
    table_c.sort_values(by=OPEN_TIME, inplace=True)
    table_c.reset_index(drop=True, inplace=True)
    table_c.to_hdf('transaction/{}.h5'.format(file_name), key='df', format='table', mode='w')


# save_transactions('SPY', 'IVV', 1)
# save_transactions('SPY', 'IVV', 2)
# save_transactions('SPY', 'IVV', 3)

# save_transactions('SPY', 'IVV', 4)
# save_transactions('SPY', 'IVV', 5)
# save_transactions('SPY', 'IVV', 6)
# save_transactions('IVV', 'VOO', 1)
# save_transactions('IVV', 'VOO', 2)
# save_transactions('VOO', 'SPY', 1)
# save_transactions('VOO', 'SPY', 2)
# save_transactions('VOO', 'SPY', 3)
# save_transactions('VOO', 'SPY', 4)
# save_transactions('VOO', 'SPY', 5)
# save_transactions('VOO', 'SPY', 6)
save_transactions('SPYG', 'VOOG')
save_transactions('SPYV', 'VOOV')
