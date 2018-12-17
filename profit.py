# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-28
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

directories = [
    # 'original_transactions',
    # 'original_transactions_with_stop_loss',
    # 'yesterday_transactions',
    # 'yesterday_transactions_with_stop_loss',
    'today_transactions',
    'today_transactions_with_stop_loss'
]

maximum = 1000000

files = [
    'IVV_VOO',
    'SPY_IVV',
    'SPYG_VOOG',
    'SPYV_VOOV',
    'VOO_SPY'
]

titles = [
    # 'Logic 1',
    # 'Logic 1 & 2',
    # 'Logic 3',
    # 'Logic 3 & 2',
    'Logic 4',
    'Logic 4 & 2'
]


def calculate_profit(directory, maximum, files, title):
    print(title)
    transactions = [pd.read_hdf('{}/{}.h5'.format(directory, file), key='df') for file in files]

    concat = pd.concat(transactions)
    concat.sort_values(by='open_time', inplace=True)
    concat.reset_index(drop=True, inplace=True)

    print('# of signal: {}'.format(len(concat)))

    valid_transactions = pd.DataFrame()
    while len(concat) != 0:
        valid_transactions = pd.concat([valid_transactions, concat.head(1)])
        close_time = concat.iloc[0, 3]
        concat = concat.loc[concat['open_time'] > close_time, :]

    valid_transactions.reset_index(drop=True, inplace=True)

    valid_transactions['long_count'] = np.floor(maximum / valid_transactions['open_ask'])
    valid_transactions['short_count'] = np.floor(maximum / valid_transactions['open_bid'])
    valid_transactions['profit'] = \
        valid_transactions['long_count'] * (valid_transactions['close_bid'] - valid_transactions['open_ask']) \
        - valid_transactions['short_count'] * (valid_transactions['close_ask'] - valid_transactions['open_bid']) \
        - 1000000 * 0.0026 - 1000000 * 0.0129 / len(valid_transactions)
    valid_transactions['balance'] = valid_transactions['profit']
    valid_transactions.loc[0, 'balance'] = valid_transactions.loc[0, 'balance'] + maximum
    valid_transactions['balance'] = valid_transactions['balance'].cumsum()

    total_profit = np.sum(valid_transactions['profit'])

    # Sharpe ratio
    daily_return = valid_transactions[['open_time', 'close_time', 'balance']]
    daily_return.loc[:, 'date'] = daily_return.loc[:, 'close_time'].dt.normalize()
    daily_return = daily_return[['date', 'balance']].groupby(by='date').last()
    daily_return.reset_index(drop=False, inplace=True)
    all_days = pd.DataFrame({'date': pd.date_range(start='2016-01-01', end='2016-12-31', freq='D')})
    daily_return = daily_return.merge(all_days, how='outer', on='date', sort=True)
    daily_return.loc[0, 'balance'] = maximum
    daily_return = daily_return.fillna(method='ffill')
    daily_return['excess_return'] = daily_return['balance'].pct_change() - 0.0022 / 365
    sharpe_ratio = daily_return['excess_return'].mean() / daily_return['excess_return'].std()

    # Maximum Drawdown
    daily_return['peak'] = np.maximum.accumulate(daily_return['balance'])
    daily_return['drawdown'] = - (daily_return['balance'] - daily_return['peak']) / daily_return['peak']
    maximum_drawdown = daily_return['drawdown'].max()

    print('# of valid signal: {}'.format(len(valid_transactions)))
    print('Total return: {}'.format(total_profit / maximum))
    print('Max return: {}'.format(valid_transactions['profit'].max() / maximum))
    print('Min return: {}'.format(valid_transactions['profit'].min() / maximum))
    print('Sharpe ratio: {}'.format(sharpe_ratio))
    print('MDD: {}'.format(maximum_drawdown))

    # Plot
    daily_plot = daily_return[['date', 'balance']]
    daily_plot.set_index('date', drop=False, inplace=True)
    plt.figure()
    daily_plot['balance'].plot()
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Balance')
    for close_time in valid_transactions['close_time']:
        plt.axvline(x=close_time, linewidth=0.25, color='b')
    for open_time in valid_transactions['open_time']:
        plt.axvline(x=open_time, linewidth=0.25, color='r')
    plt.savefig('{}/transaction.png'.format(directory))
    plt.show()
    valid_transactions.to_csv('{}/transaction.csv'.format(directory))


for directory, title in zip(directories, titles):
    calculate_profit(directory, maximum, files, title)
