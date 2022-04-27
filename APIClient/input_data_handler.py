import json
import time
import pandas as pd
import requests


class InputDataHandler:
    def __init__(
        self, path_to_cleaned_companies_df="APIClient/data/df_text_cleaned.csv"
    ) -> None:
        self.df_text_cleaned = pd.read_csv(path_to_cleaned_companies_df)
        self.list_of_ciks = list(set(self.df_text_cleaned["cik"]))

    def add_input_data(self):
        for cik in self.list_of_ciks:
            resp = json.loads(
                requests.post(
                    f"http://localhost:8000/api/v1/input_data/",
                    data=json.dumps({"cik": cik}),
                ).text
            )
            print(resp["code"], "|", resp["message"], f"cik: {cik}")
            
    def scale_data(self):
        for k_fold in range(1,5):
            resp = json.loads(
                requests.post(
                    f"http://localhost:8000/api/v1/input_data/scale/",
                    data=json.dumps({"k_fold": k_fold}),
                ).text
            )
            print(resp["code"], "|", resp["message"], f"k_fold: {k_fold}")
            time.sleep(15)
