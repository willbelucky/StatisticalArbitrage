# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-20
"""
import pandas as pd
import matplotlib.pyplot as plt

DATE = 'date'

DATETIME = 'DATETIME'


def pair_data(etf_1_name, etf_2_name):
    file_name = '{}_{}'.format(etf_1_name, etf_2_name)
    print(file_name)

    etf_1 = pd.read_csv('second_data/{}.csv'.format(etf_1_name))
    etf_2 = pd.read_csv('second_data/{}.csv'.format(etf_2_name))
    data = etf_1.merge(etf_2, how='outer', on=DATETIME, sort=True, copy=False,
                       suffixes=('_' + etf_1_name, '_' + etf_2_name))
    data.rename(index=str, columns={
        'ASK' + '_' + etf_1_name: 'ASK_1',
        'BID' + '_' + etf_1_name: 'BID_1',
        'ASK' + '_' + etf_2_name: 'ASK_2',
        'BID' + '_' + etf_2_name: 'BID_2',
    }, inplace=True)
    data.fillna(method='ffill', inplace=True)
    columns = ['ASK_1', 'BID_1', 'ASK_2', 'BID_2']
    for column in columns:
        data['next_' + column] = data[column].shift(-1)

    data[DATE] = pd.to_datetime(data[DATETIME]).dt.normalize()
    data.dropna(inplace=True)
    for column in columns:
        last_columns = data[[DATE, column]].groupby(by=DATE).last().reset_index(drop=False)
        last_columns.rename(index=str, columns={column: 'last_' + column}, inplace=True)
        data = data.merge(last_columns, on=DATE)

    data[DATETIME] = pd.to_datetime(data[DATETIME])
    data.set_index(DATETIME, inplace=True)

    data[columns].plot()
    plt.title(file_name)
    plt.savefig('paired_image/{}.png'.format(file_name))
    plt.show()
    data.to_hdf('paired_data/{}.h5'.format(file_name), key='df', format='table', mode='w')


pair_data('SPY', 'IVV')
pair_data('IVV', 'VOO')
pair_data('VOO', 'SPY')
pair_data('SPYG', 'VOOG')
pair_data('SPYV', 'VOOV')
