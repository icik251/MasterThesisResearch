import json
import requests


class TimeSeriesHandler:
    def __init__(self, url="http://localhost:8000/api/v1/stock_price/") -> None:
        self.url = url

    def add_timeseries(self):
        resp = requests.get(f"http://localhost:8000/api/v1/company")
        resp = json.loads(resp.text)
        if resp["code"] == 200:
            # we want to check the most common type for the year and send to update if needed
            data = resp["data"]

            set_of_unique_ciks = set([])
            for curr_company in data:
                if curr_company["cik"] not in set_of_unique_ciks:
                    resp = requests.post(
                        self.url,
                        data=json.dumps({"cik": curr_company["cik"]}),
                    )
                    resp = json.loads(resp.text)
                    if resp['code'] == 200:
                        print(f"Company time-series {curr_company['cik']} processing")
                    else:
                        print(f"Error with cik {curr_company['cik']}")
                        print(resp['message'])

                    set_of_unique_ciks.add(curr_company["cik"])
