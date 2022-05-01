import json
import time
import pandas as pd
import requests
import json
import requests
from collections import defaultdict


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
        for k_fold in range(1, 5):
            resp = json.loads(
                requests.post(
                    f"http://localhost:8000/api/v1/input_data/scale/",
                    data=json.dumps({"k_fold": k_fold}),
                ).text
            )
            print(resp["code"], "|", resp["message"], f"k_fold: {k_fold}")
            time.sleep(15)

    def set_is_used(self, list_of_ciks, threshold_count_industry_per_period=2):
        is_used_json_for_update = json.dumps({"is_used": False})
        # Update 2017 q1
        resp = json.loads(
            requests.put(
                f"http://localhost:8000/api/v1/input_data/",
                is_used_json_for_update,
                params={"year": 2017, "q": 1},
            ).text
        )
        if resp["code"] == 200:
            print(f"Successfully is_used updated for 2017 q 1")

        # Update for last quarter as no data in the future is available
        resp = json.loads(
            requests.put(
                f"http://localhost:8000/api/v1/input_data/",
                is_used_json_for_update,
                params={"year": 2021, "q": 4},
            ).text
        )
        if resp["code"] == 200:
            print(f"Successfully is_used updated for 2021 q 4")

        # Update for CIKs that we want to remove because of too many missing data for KPIs
        for cik in list_of_ciks:
            resp = json.loads(
                requests.put(
                    f"http://localhost:8000/api/v1/input_data/",
                    is_used_json_for_update,
                    params={"cik": cik},
                ).text
            )
            if resp["code"] == 200:
                print(f"Successfully is_used updated for cik {cik}")

        # Updating by industry should be last because before that we remove some companies by CIKs which influences
        # the number for the industries
        list_of_years = [2017, 2018, 2019, 2020, 2021]
        list_of_qs = [1, 2, 3, 4]

        dict_of_res = {}
        for year in list_of_years:
            for q in list_of_qs:
                # Exclude 2017 quarter 1:
                if year == 2017 and q == 1:
                    continue

                resp = json.loads(
                    requests.get(
                        f"http://localhost:8000/api/v1/input_data/{year}/{q}"
                    ).text
                )
                if resp["code"] == 200:
                    dict_of_res[str(year) + "_" + str(q)] = resp["data"]

        # dict_of_industry_counts = defaultdict(int)
        dict_of_industry_counts_per_year_q = defaultdict(lambda: defaultdict(int))
        for year_q_k, inputs_data in dict_of_res.items():
            for input_data in inputs_data:
                # dict_of_industry_counts[input_data["industry"]] += 1
                dict_of_industry_counts_per_year_q[year_q_k][
                    input_data["industry"]
                ] += 1

        # Update for industries
        list_of_updated_industries = []
        for year_q, industry_dict in dict_of_industry_counts_per_year_q.items():
            for industry, count in industry_dict.items():
                if (
                    count <= threshold_count_industry_per_period
                    and industry not in list_of_updated_industries
                ):
                    resp = json.loads(
                        requests.put(
                            f"http://localhost:8000/api/v1/input_data/",
                            is_used_json_for_update,
                            params={"industry": industry},
                        ).text
                    )
                    if resp["code"] == 200:
                        print(
                            f"Successfully is_used updated for {industry} with count {count}"
                        )
                        list_of_updated_industries.append(industry)
