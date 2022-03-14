import requests
import datetime

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=CIVI&outputsize=full&apikey=DV3U4RGOYLYSQHN8"
r = requests.get(url)
data = r.json()

start = "2017-01-01"


list_of_dates = list(data["Time Series (Daily)"].keys())

while start not in list_of_dates:
    start_datetime = datetime.datetime.fromisoformat(start)
    start_datetime += datetime.timedelta(days=1)
    start = start_datetime.strftime("%Y-%m-%d")


idx = list_of_dates.index(start)
list_of_dates = list_of_dates[: idx + 1]

list_of_closes = []
list_of_adj_closes = []
list_of_dates_to_df = []
for date in sorted(list_of_dates):
    dict_data = data["Time Series (Daily)"][date]

    list_of_dates_to_df.append(date)
    list_of_closes.append(float(dict_data["4. close"]))
    list_of_adj_closes.append(float(dict_data["5. adjusted close"]))


import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


df = pd.DataFrame(
    list(zip(list_of_dates_to_df, list_of_closes, list_of_adj_closes)),
    columns=["date", "close", "adj_close"],
)

fig = px.line(df, x="date", y="close")
fig.show()
