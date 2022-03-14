API_KEY = "DV3U4RGOYLYSQHN8"

import requests

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&apikey=DV3U4RGOYLYSQHN8"
r = requests.get(url)
data = r.json()

# print(data)
# print("--------------")

# # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
# url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=TSLA&apikey=DV3U4RGOYLYSQHN8"
# r = requests.get(url)
# data = r.json()

# print(data)
# print("---------------")

# url_eod = "https://eodhistoricaldata.com/api/eod/TSLA?api_token=620960704248b5.65214433&period=d&from=2010-01-01&to=2011-02-13&fmt=json"
# r = requests.get(url_eod)
# data_eod = r.json()
# print(data_eod)
# print("---------------------")


import yfinance as yf
import pandas as pd
import plotly.express as px

try:
    # download the stock price
    stock_df = yf.download("CIVI", start="2017-01-01")
    stock_df.reset_index(level=0, inplace=True)
    fig = px.line(stock_df, x="Date", y="Close")
    fig.show()
    fig = px.line(stock_df, x="Date", y="Adj Close")
    fig.show()

    # stock_df.reset_index(level=0, inplace=True)
    # # stock_df["Date"].values
    # # append the individual stock prices
    # if len(stock_df) == 0:
    #     None
    # else:
    #     list_of_res = []
    #     for timestamp, open, high, low, close, adj_close, volume in zip(
    #         stock_df["Date"].values,
    #         stock_df["Open"].values,
    #         stock_df["High"].values,
    #         stock_df["Low"].values,
    #         stock_df["Close"].values,
    #         stock_df["Adj Close"].values,
    #         stock_df["Volume"].values,
    #     ):
    #         list_of_res.append({})
    #         pass

        # stock.to_csv("time_series/output/MSFT_max.csv")
except Exception as e:
    print(e)
