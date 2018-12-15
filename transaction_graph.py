# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-28
"""
import pandas as pd
import matplotlib.pyplot as plt


def draw_graph(file_name):
    valid_transaction = pd.read_csv('valid_transactions/valid_transaction_{}.csv'.format(file_name))
    screened_data = pd.read_hdf('paired_data/{}.h5'.format(file_name), key='df')

    plt.figure()
    screened_data.plot()
    plt.title(file_name)
    plt.xlabel('Date')
    plt.ylabel('Price')
    for close_time in valid_transaction['close_time']:
        plt.axvline(x=close_time, linewidth=0.5, color='b')
    for open_time in valid_transaction['open_time']:
        plt.axvline(x=open_time, linewidth=0.5, color='r')
    plt.show()


draw_graph('SPYG_VOOG')
draw_graph('SPYV_VOOV')
