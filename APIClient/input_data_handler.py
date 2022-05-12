import json
from os import kill
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

    def create_k_folds(self, k_folds):
        
        # Old logic
        # basically if we are 2018, we indicate that for K-FOLD 1, this is used for val
        # for K-FOLD 2, this is used for train, etc.
        # Example for 2018
        # self.k_fold_config_example = {1: "val", 2: "train", 3: "train"}

        # self.k_folds_rules = {
        #     # 2017 same as 2018 as they are gonna be in 1 fold
        #     2017: {1: "val", 2: "train", 3: "train"},
        #     2018: {1: "val", 2: "train", 3: "train"},
        #     2019: {1: "train", 2: "val", 3: "train"},
        #     2020: {1: "train", 2: "train", 3: "val"},
        #     2021: {1: "test", 2: "test", 3: "test"},
        # }
        resp = json.loads(
            requests.post(
                f"http://localhost:8000/api/v1/input_data/k_folds/",
                data=json.dumps({"k_folds": k_folds}),
            ).text
        )
        print(resp["code"], "|", resp["message"])

    def scaling_labels(self):
        for k_fold in range(1, 6):
            resp = json.loads(
                requests.post(
                    f"http://localhost:8000/api/v1/input_data/scaling_labels/",
                    data=json.dumps({"k_fold": k_fold}),
                ).text
            )
            print(resp["code"], "|", resp["message"], f"k_fold: {k_fold}")
            time.sleep(30)

    def scaling_labels_test_set(self):
        resp = json.loads(
            requests.post(
                f"http://localhost:8000/api/v1/input_data/scaling_labels_test/",
            ).text
        )
        print(resp["code"], "|", resp["message"])

    def set_is_used(
        self,
        list_of_ciks=[],
        list_of_ids_to_remove=[],
        lacking_features_is_used=False,
        outlier_target_is_used=False,
        threshold_count_industry_per_period=2,
    ):
        is_used_json_for_update = json.dumps({"is_used": False})
        if not lacking_features_is_used and not outlier_target_is_used:
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
            # Update for custom indutry "RAILROADS, LINE-HAUL OPERATING"
            # industry = "RAILROADS, LINE-HAUL OPERATING"
            # resp = json.loads(
            #     requests.put(
            #         f"http://localhost:8000/api/v1/input_data/",
            #         is_used_json_for_update,
            #         params={"industry": industry},
            #     ).text
            # )
            # if resp["code"] == 200:
            #     print(f"Successfully is_used updated for {industry} with count {count} | Custom one because of KNN imputation lacking")

        elif lacking_features_is_used:
            # Update for all that does not have features
            resp = json.loads(
                requests.put(
                    f"http://localhost:8000/api/v1/input_data/", is_used_json_for_update
                ).text
            )
            if resp["code"] == 200:
                print(f"Successfully is_used updated for all")
                for k_id, dict_tuple_size_zeros in resp["data"].items():
                    print(k_id, dict_tuple_size_zeros)

        elif outlier_target_is_used:
            for _id in list_of_ids_to_remove:
                resp = json.loads(
                    requests.put(
                        f"http://localhost:8000/api/v1/input_data/",
                        is_used_json_for_update,
                        params={"_id": _id},
                    ).text
                )
                if resp["code"] == 200:
                    print(resp["message"])
