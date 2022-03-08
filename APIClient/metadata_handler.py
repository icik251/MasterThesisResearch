import json
import requests
import random

from company_handler import CompanyHandler

# https://sec-api.io/docs/sec-filings-item-extraction-api

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

# Current API KEY: 71c927f782a89eeb7b19ea545da729e3b888c0be8222ef208ef8e74d179ba95f


random.seed(13)


class MetadataHandler(CompanyHandler):
    def __init__(self, path_companies_to_extract) -> None:
        super().__init__(path_companies_to_extract)

    def add_metadata_mock(self, req_max=5):
        for idx, company_base in enumerate(self.load_chosen_companies_from_db()):
            resp = requests.get(
                f"http://localhost:8000/api/v1/company/{company_base['cik']}/"
            )
            response_model = json.loads(resp.text)
            if response_model["code"] != 200:
                print(
                    f"Error on company cik {company_base['cik']} | {response_model['message']}"
                )
                continue

            len_company_docs = len(response_model["data"])
            chosen_rand_idx = random.randint(0, len_company_docs - 1)

            # Add metadata for it, one request to API is 3 requests per company for SEC API
            chosen_company = response_model["data"][chosen_rand_idx]

            dict_to_add = {
                "cik": chosen_company["cik"],
                "year": chosen_company["year"],
                "quarter": 1,
                "type": chosen_company["quarters"][0]["info"][0]["type"],
            }
            _ = requests.post(
                f"http://localhost:8000/api/v1/metadata/", data=json.dumps(dict_to_add)
            )
            if idx >= req_max:
                break


metadata_handler = MetadataHandler("APIClient/data/nasdaq/energy_all.csv")
metadata_handler.add_metadata_mock()
