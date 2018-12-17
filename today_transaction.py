# -*- coding: utf-8 -*-
"""
:Author: Jaekyoung Kim
:Date: 2018-11-28
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm
from arch import arch_model
from tqdm import tqdm
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_model import ARIMA
from pyramid.arima import auto_arima
import warnings

warnings.filterwarnings('ignore')

DATETIME = 'DATETIME'

RATIO_1 = 'ratio_1'
RATIO_2 = 'ratio_2'
STD_1 = 'std_1'
STD_2 = 'std_2'
RESID_1 = 'resid_1'
RESID_2 = 'resid_2'
STD_1_HAT = 'std_1_hat'
STD_2_HAT = 'std_2_hat'

CLOSE_ASK = 'close_ask'
CLOSE_BID = 'close_bid'
OPEN_ASK = 'open_ask'
OPEN_BID = 'open_bid'
LAST_ASK = 'last_ask'
LAST_BID = 'last_bid'
CLOSE_TIME = 'close_time'
OPEN_TIME = 'open_time'
LAST_TIME = 'last_time'
TIME = 'time'
DATE = 'date'

BID_1 = 'BID_1'
ASK_1 = 'ASK_1'
BID_2 = 'BID_2'
ASK_2 = 'ASK_2'

rolling_window = 100


def get_table(data: pd.DataFrame, open_bid, open_ask, close_bid, close_ask,
              resid, std, stop_loss=False):
    table = data.copy()
    table[TIME] = table.index
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

    open_table = table.loc[table[resid] < -10 * table[std], [TIME, OPEN_TIME, OPEN_BID, OPEN_ASK, LAST_BID, LAST_ASK]]
    print('Open: {}({}%)'.format(len(open_table), len(open_table) * 100 / len(table)))

    close_table = table.loc[table[resid] > 10 * table[std], [TIME, CLOSE_TIME, CLOSE_BID, CLOSE_ASK]]
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


# noinspection PyTypeChecker,PyUnresolvedReferences
def save_today_transactions(etf_1, etf_2, stop_loss):
    file_name = '{}_{}'.format(etf_1, etf_2)
    print(file_name)
    paired_data = pd.read_hdf('paired_data/{}.h5'.format(file_name), key='df')

    # ETF 1을 사기 위해 필요한 ETF 2의 개수
    dates = []
    # ratio_etf1_per_etf2s
    ratio_1 = [np.nan]
    std_1 = [np.nan]
    # ratio_etf2_per_etf1s
    ratio_2 = [np.nan]
    std_2 = [np.nan]
    for date in paired_data[DATE].unique():
        current_data = paired_data.loc[paired_data[DATE] == date, :]
        dates.append(date)

        X = current_data[BID_1]
        y = current_data[ASK_2]
        model_2 = sm.OLS(y, X, hasconst=False).fit()
        coef = model_2.params
        ratio_1.append(coef[0])
        std = np.sqrt(np.sum(np.square(model_2.resid)) / (len(model_2.resid) - 1))
        std_1.append(std)

        X = current_data[BID_2]
        y = current_data[ASK_1]
        model_2 = sm.OLS(y, X, hasconst=False).fit()
        coef = model_2.params
        ratio_2.append(coef[0])
        std = np.sqrt(np.sum(np.square(model_2.resid)) / (len(model_2.resid) - 1))
        std_2.append(std)

    daily_ols = pd.DataFrame(data={
        DATE: dates,
        RATIO_1: ratio_1[:-1],
        STD_1: std_1[:-1],
        RATIO_2: ratio_2[:-1],
        STD_2: std_2[:-1]
    })
    daily_ols.dropna(inplace=True)

    # plot_acf(daily_ols[STD_1], title='{} & {} Autocorrelation'.format(etf_1, etf_2))
    # plt.show()
    # plot_pacf(daily_ols[STD_1], title='{} & {} Paritial Autocorrelation'.format(etf_1, etf_2))
    # plt.show()
    # plot_acf(daily_ols[STD_2], title='{} & {} Autocorrelation'.format(etf_2, etf_1))
    # plt.show()
    # plot_pacf(daily_ols[STD_2], title='{} & {} Paritial Autocorrelation'.format(etf_2, etf_1))
    # plt.show()

    # model = ARIMA(daily_ols[STD_1], order=(1, 0, 1))
    # model_fit = model.fit(trend='nc', full_output=True, disp=1)
    # print(model_fit.summary())
    # model_fit.plot_predict()
    # plt.title('{} & {} Standard Deviation ARIMA'.format(etf_1, etf_2))
    # plt.show()

    std_1_hats = [np.nan for _ in range(10)]
    std_2_hats = [np.nan for _ in range(10)]
    for i in tqdm(range(10, len(daily_ols))):
        stepwise_model = auto_arima(daily_ols[STD_1][:(i+1)], start_p=1, start_q=1,
                                    max_p=3, max_q=3, m=12,
                                    start_P=0, seasonal=False, trace=False,
                                    error_action='ignore',
                                    suppress_warnings=True,
                                    stepwise=True)
        stepwise_model.fit(daily_ols[STD_1][:(i+1)])
        future_forecast = stepwise_model.predict(n_periods=1)
        std_1_hats.append(future_forecast[0])

        stepwise_model = auto_arima(daily_ols[STD_2][:(i+1)], start_p=1, start_q=1,
                                    max_p=3, max_q=3, m=12,
                                    start_P=0, seasonal=False, trace=False,
                                    error_action='ignore',
                                    suppress_warnings=True,
                                    stepwise=True)
        stepwise_model.fit(daily_ols[STD_2][:(i+1)])
        future_forecast = stepwise_model.predict(n_periods=1)
        std_2_hats.append(future_forecast[0])

    daily_ols[STD_1_HAT] = std_1_hats
    daily_ols[STD_2_HAT] = std_2_hats

    paired_data.reset_index(drop=False, inplace=True)
    paired_data[DATETIME] = pd.to_datetime(paired_data[DATETIME])
    paired_data = paired_data.merge(daily_ols, on=DATE)
    paired_data.dropna(inplace=True)
    paired_data[RESID_1] = paired_data[ASK_2] - paired_data[RATIO_1] * paired_data[BID_1]
    paired_data[RESID_2] = paired_data[ASK_1] - paired_data[RATIO_2] * paired_data[BID_2]
    paired_data.set_index(DATETIME, inplace=True)

    print(RESID_1)
    print(paired_data[RESID_1].describe(percentiles=[0.05, 0.5, 0.95]))
    print('Skewness: {0:.4f}'.format(paired_data[RESID_1].skew()))
    print('Kurtosis: {0:.4f}'.format(paired_data[RESID_1].kurtosis()))
    print('Autocorr: {0:.4f}'.format(paired_data[RESID_1].autocorr()))
    print(RESID_2)
    print(paired_data[RESID_2].describe(percentiles=[0.05, 0.5, 0.95]))
    print('Skewness: {0:.4f}'.format(paired_data[RESID_2].skew()))
    print('Kurtosis: {0:.4f}'.format(paired_data[RESID_2].kurtosis()))
    print('Autocorr: {0:.4f}'.format(paired_data[RESID_2].autocorr()))

    print('ETF 1 Overvalued')
    table_a = get_table(paired_data, BID_1, ASK_2, BID_2, ASK_1, RESID_1, STD_1_HAT, stop_loss)
    print('ETF 2 Overvalued')
    table_b = get_table(paired_data, BID_2, ASK_1, BID_1, ASK_2, RESID_2, STD_2_HAT, stop_loss)
    print('Total: {}'.format(len(paired_data)))

    table_c = pd.concat([table_a, table_b])
    table_c.sort_values(by=OPEN_TIME, inplace=True)
    table_c.reset_index(drop=True, inplace=True)
    if stop_loss:
        table_c.to_hdf('today_transactions_with_stop_loss/{}.h5'.format(file_name), key='df', format='table',
                       mode='w')
    else:
        table_c.to_hdf('today_transactions/{}.h5'.format(file_name), key='df', format='table', mode='w')


save_today_transactions('IVV', 'VOO', False)
save_today_transactions('SPY', 'IVV', False)
save_today_transactions('SPYG', 'VOOG', False)
save_today_transactions('SPYV', 'VOOV', False)
save_today_transactions('VOO', 'SPY', False)

# save_today_transactions('IVV', 'VOO', True)
# save_today_transactions('SPY', 'IVV', True)
# save_today_transactions('SPYG', 'VOOG', True)
# save_today_transactions('SPYV', 'VOOV', True)
# save_today_transactions('VOO', 'SPY', True)
