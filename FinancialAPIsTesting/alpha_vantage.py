import requests

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=IBM&apikey=DV3U4RGOYLYSQHN8'
r = requests.get(url)
data = r.json()

print(data)