import json
import time
import requests
import random
import pandas as pd


class FundamentalDataHandler:
    def __init__(
        self, path_to_cleaned_companies_df="APIClient/data/df_text_cleaned.csv"
    ) -> None:
        self.df_text_cleaned = pd.read_csv(path_to_cleaned_companies_df)
        self.list_of_ciks = list(set(self.df_text_cleaned["cik"]))

    def add_fundamental_data(self, req_per_min=33):
        for idx, cik in enumerate(self.list_of_ciks):
            resp = json.loads(
                requests.post(
                    f"http://localhost:8000/api/v1/fundamental_data/",
                    data=json.dumps({"cik": cik}),
                ).text
            )
            print(resp["code"], "|", resp["message"], f"cik: {cik}")
            if idx % req_per_min == 0 and idx != 0:
                time.sleep(60)
