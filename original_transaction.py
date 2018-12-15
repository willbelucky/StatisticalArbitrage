# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-28
"""
import pandas as pd
import numpy as np

CLOSE_ASK = 'close_ask'
CLOSE_BID = 'close_bid'
OPEN_ASK = 'open_ask'
OPEN_BID = 'open_bid'
LAST_ASK = 'last_ask'
LAST_BID = 'last_bid'
CLOSE_TIME = 'close_time'
OPEN_SIGNAL = 'open_signal'
CLOSE_SIGNAL = 'close_signal'
OPEN_TIME = 'open_time'
LAST_TIME = 'last_time'
TIME = 'time'

BID_1 = 'BID_1'
ASK_1 = 'ASK_1'
BID_2 = 'BID_2'
ASK_2 = 'ASK_2'


def get_table(data: pd.DataFrame, open_bid, open_ask, close_bid, close_ask, ratio_1, ratio_2, criteria, tol,
              stop_loss=False):
    table = data.copy()
    table[TIME] = table.index
    table[OPEN_SIGNAL] = table[open_bid] / (ratio_1 * table[open_ask]) - 1
    table[CLOSE_SIGNAL] = table[close_bid] / (ratio_2 * table[close_ask]) - 1
    table[OPEN_TIME] = table.index
    table[CLOSE_TIME] = table.index
    table.rename(columns={
        'next_' + open_bid: OPEN_BID,
        'next_' + open_ask: OPEN_ASK,
        'next_' + close_bid: CLOSE_BID,
        'next_' + close_ask: CLOSE_ASK,
        'last_' + close_bid: LAST_BID,
        'last_' + close_ask: LAST_ASK
    }, inplace=True)

    open_table = table.loc[table[OPEN_SIGNAL] > criteria, [TIME, OPEN_TIME, OPEN_BID, OPEN_ASK, LAST_BID, LAST_ASK]]
    print('Open: {}({}%)'.format(len(open_table), len(open_table) * 100 / len(table)))

    close_table = table.loc[np.abs(table[CLOSE_SIGNAL]) < tol, [TIME, CLOSE_TIME, CLOSE_BID, CLOSE_ASK]]
    print('Close: {}({}%)'.format(len(close_table), len(close_table) * 100 / len(table)))

    concat_table = open_table.merge(close_table, how='outer', on=TIME, sort=True)
    concat_table = concat_table.sort_values(by=TIME)
    concat_table[CLOSE_TIME] = concat_table[CLOSE_TIME].fillna(method='bfill')
    concat_table[CLOSE_BID] = concat_table[CLOSE_BID].fillna(method='bfill')
    concat_table[CLOSE_ASK] = concat_table[CLOSE_ASK].fillna(method='bfill')
    concat_table.dropna(inplace=True)

    if stop_loss:
        concat_table.loc[
            concat_table[CLOSE_TIME].dt.normalize() != concat_table[OPEN_TIME].dt.normalize(), CLOSE_BID
        ] = concat_table.loc[
            concat_table[CLOSE_TIME].dt.normalize() != concat_table[OPEN_TIME].dt.normalize(), LAST_BID
        ]
        concat_table.loc[
            concat_table[CLOSE_TIME].dt.normalize() != concat_table[OPEN_TIME].dt.normalize(), CLOSE_ASK
        ] = concat_table.loc[
            concat_table[CLOSE_TIME].dt.normalize() != concat_table[OPEN_TIME].dt.normalize(), LAST_ASK
        ]
        concat_table.loc[
            concat_table[CLOSE_TIME].dt.normalize() != concat_table[OPEN_TIME].dt.normalize(), CLOSE_TIME
        ] = concat_table.loc[
            concat_table[CLOSE_TIME].dt.normalize() != concat_table[OPEN_TIME].dt.normalize(), OPEN_TIME
        ].apply(lambda dt: dt.replace(hour=15, minute=55, second=0))

    concat_table.drop(columns=[TIME, LAST_BID, LAST_ASK], inplace=True)
    return concat_table


# noinspection PyTypeChecker
def save_original_transactions(etf_1, etf_2, criteria=0.002, tol=0.00001):
    file_name = '{}_{}'.format(etf_1, etf_2)
    print(file_name)
    paired_data = pd.read_hdf('paired_data/{}.h5'.format(file_name), key='df')

    #     VOO - 0.91 * IVV
    #     IVV - 1.10 * VOO
    if etf_1 == 'IVV' and etf_2 == 'VOO':
        ratio_etf1_per_etf2 = 1.102
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

    print('ETF 1 Overvalued')
    table_a = get_table(paired_data, BID_1, ASK_2, BID_2, ASK_1, ratio_etf1_per_etf2,
                        ratio_etf2_per_etf1, criteria, tol)
    print('ETF 2 Overvalued')
    table_b = get_table(paired_data, BID_2, ASK_1, BID_1, ASK_2, ratio_etf2_per_etf1,
                        ratio_etf1_per_etf2, criteria, tol)
    print('Total: {}'.format(len(paired_data)))

    table_c = pd.concat([table_a, table_b])
    table_c.sort_values(by=OPEN_TIME, inplace=True)
    table_c.reset_index(drop=True, inplace=True)
    print(table_c)
    table_c.to_hdf('original_transactions/{}.h5'.format(file_name), key='df', format='table', mode='w')


# noinspection PyTypeChecker
def save_stop_loss_transactions(etf_1, etf_2, criteria=0.002, tol=0.00001):
    file_name = '{}_{}'.format(etf_1, etf_2)
    print(file_name)
    paired_data = pd.read_hdf('paired_data/{}.h5'.format(file_name), key='df')

    #     VOO - 0.91 * IVV
    #     IVV - 1.10 * VOO
    if etf_1 == 'IVV' and etf_2 == 'VOO':
        ratio_etf1_per_etf2 = 1.102
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

    print('ETF 1 Overvalued')
    table_a = get_table(paired_data, BID_1, ASK_2, BID_2, ASK_1, ratio_etf1_per_etf2,
                        ratio_etf2_per_etf1, criteria, tol, True)
    print('ETF 2 Overvalued')
    table_b = get_table(paired_data, BID_2, ASK_1, BID_1, ASK_2, ratio_etf2_per_etf1,
                        ratio_etf1_per_etf2, criteria, tol, True)
    print('Total: {}'.format(len(paired_data)))

    table_c = pd.concat([table_a, table_b])
    table_c.sort_values(by=OPEN_TIME, inplace=True)
    table_c.reset_index(drop=True, inplace=True)
    print(table_c)
    table_c.to_hdf('stop_loss_transactions/{}.h5'.format(file_name), key='df', format='table', mode='w')


save_original_transactions('IVV', 'VOO')
save_original_transactions('SPY', 'IVV')
save_original_transactions('SPYG', 'VOOG')
save_original_transactions('SPYV', 'VOOV')
save_original_transactions('VOO', 'SPY')

save_stop_loss_transactions('IVV', 'VOO')
save_stop_loss_transactions('SPY', 'IVV')
save_stop_loss_transactions('SPYG', 'VOOG')
save_stop_loss_transactions('SPYV', 'VOOV')
save_stop_loss_transactions('VOO', 'SPY')
