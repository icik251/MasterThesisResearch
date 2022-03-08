import csv
import json
import os
import requests
import random
from sec_api import ExtractorApi

random.seed(50)

# https://sec-api.io/docs/sec-filings-item-extraction-api

# API is supporting this functionality so this is redundant.

"""
    10-Q sections
        2 - Management’s Discussion and Analysis of Financial Condition and Results of Operations
        21A - Risk Factors

    10-K sections
        7 - Management’s Discussion and Analysis of Financial Condition and Results of Operations
        1A - Risk Factors
        
    Before 2003 
        - There are only 10-K reports that can be extracted from the API and only the MDA section.
        - In some of them, the MDA is just a reference like in id "6202564cfa287977824d92a2" (check "consistency.txt")
        - Some of them are just undefined like the risk, id: "620256ad9ad1c6e6814d92ce"
    After 2006-7
        - Almost everything is alright.
        - 10-K reports are reliable, 10-Q not that much, there are some problems with getting the text from them.
"""

extractorApi = ExtractorApi(
    "0ed3ffd8daf8372a1162a4b5b45ff7ba576248459fd8f1cea6d42b1ab52ca12a"
)


def check_consistency_companies(
    path_to_files="app_client/data/edgar",
    possible_types=["10-K", "10-Q"],
    start_year=1993,
    end_year=2023,
):
    keys = list(range(start_year, end_year))
    values = [False] * len(keys)
    years_dict = dict(zip(keys, values))
    for file in os.listdir(path=path_to_files):
        curr_year = int(file.split("-")[0])
        curr_idx_to_get = random.randint(1, 20)
        with open(os.path.join(path_to_files, file), "r") as tsvIn:
            reader = csv.reader(tsvIn, delimiter="|")
            curr_count = 0
            for row_company in reader:
                if row_company[2] not in possible_types:
                    continue

                curr_count += 1

                if not years_dict[curr_year] and curr_count >= curr_idx_to_get:
                    res = requests.get(
                        f"http://localhost:8000/api/v1/company/{int(row_company[0])}?year={curr_year}"
                    )
                    list_company_quarters = json.loads(res.text)["data"][0]["quarters"]
                    rand_q = random.randint(1, len(list_company_quarters))
                    curr_info = list_company_quarters[rand_q - 1]["info"][0]
                    if curr_info["type"] == "10-Q" and curr_year > 2003:
                        mda = extractorApi.get_section(
                            curr_info["url_htm"], "2", "text"
                        )
                        risk_factors = extractorApi.get_section(
                            curr_info["url_htm"], "21A", "text"
                        )

                        years_dict[curr_year] = True

                    elif curr_info["type"] == "10-K":
                        mda = extractorApi.get_section(
                            curr_info["url_htm"], "7", "text"
                        )
                        risk_factors = extractorApi.get_section(
                            curr_info["url_htm"], "1A", "text"
                        )

                        years_dict[curr_year] = True

                    if years_dict[curr_year]:
                        with open("app_client/output/consistency.txt", "a") as f:
                            f.write(json.dumps(curr_info) + "\n")
                            f.write("MDA" + "\n")
                            f.write(mda + "\n")
                            f.write("RISK" + "\n")
                            f.write(risk_factors + "\n")
                            f.write(
                                "-------------------------------------------" + "\n"
                            )

                        break


check_consistency_companies()
