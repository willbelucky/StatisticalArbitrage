# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-28
"""
import pandas as pd
import numpy as np
from copy import deepcopy

maximum = 1000000

files = [
    'SPYG_VOOG',
    'SPYV_VOOV'
]

transactions = [pd.read_hdf('transaction/{}.h5'.format(file), key='df') for file in files]

concat = pd.concat(transactions)
concat.sort_values(by='open_time', inplace=True)

concat.to_csv('valid_transaction/concat.csv')
valid_transaction_list = []
while len(concat) != 0:
    valid_transaction_list.append(deepcopy(concat.head(1)))
    close_time = concat.iloc[0, 3]
    concat = concat.loc[concat['open_time'] > close_time, :]

valid_transactions = pd.concat(valid_transaction_list).reset_index(drop=True)

valid_transactions['long_count'] = np.floor(maximum / valid_transactions['open_ask'])
valid_transactions['short_count'] = np.floor(maximum / valid_transactions['open_bid'])
valid_transactions['profit'] = \
    valid_transactions['long_count'] * (valid_transactions['close_bid'] - valid_transactions['open_ask']) \
    - valid_transactions['short_count'] * (valid_transactions['close_ask'] - valid_transactions['open_bid'])

total_profit = np.sum(valid_transactions['profit'])
print('# of transactions: {}'.format(len(valid_transactions)))
print('Total profit: {}'.format(total_profit))
print('Total return: {}'.format(total_profit / maximum))
valid_transactions.to_csv('valid_transaction/valid_transaction.csv')
