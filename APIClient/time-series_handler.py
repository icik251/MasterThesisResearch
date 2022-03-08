import json
from company_handler import CompanyHandler
import requests


class TimeSeriesHandler(CompanyHandler):
    def __init__(self, path_companies_to_extract) -> None:
        super().__init__(path_companies_to_extract)

    def add_timeseries(self):
        for company_base in self.load_chosen_companies_from_db():
            resp = requests.post(
                "http://localhost:8000/api/v1/stock_price/",
                data=json.dumps({"cik": company_base["cik"]}),
            )
            if resp.status_code != 200:
                print(f"Error with cik {company_base['cik']}")
                print(resp.text)
                print(resp.status_code)
                print("-----------")
            else:
                print(f"Company time-series {company_base['cik']} processing")


# timeseries_handler = TimeSeriesHandler("APIClient/data/nasdaq/energy_all.csv")
# timeseries_handler.add_timeseries()
