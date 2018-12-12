# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-20
"""
import pandas as pd
import matplotlib.pyplot as plt

DATETIME = 'DATETIME'


def concatenate_data(etf_1_name, etf_2_name):
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

    data['date'] = data[DATETIME].dt.to
    for column in columns:
        data['last_' + column] = data.groupby(by=DATETIME)[column].last()
    data.dropna(inplace=True)
    data[DATETIME] = pd.to_datetime(data[DATETIME])
    data.set_index(DATETIME, inplace=True)

    data[columns].plot()
    plt.title(file_name)
    plt.savefig('screened_image/{}.png'.format(file_name))
    plt.show()
    data.to_hdf('screened_data/{}.h5'.format(file_name), key='df', format='table', mode='w')


# concatenate_data('SPY', 'IVV')
# concatenate_data('IVV', 'VOO')
# concatenate_data('VOO', 'SPY')
concatenate_data('SPYG', 'VOOG')
# concatenate_data('SPYV', 'VOOV')
