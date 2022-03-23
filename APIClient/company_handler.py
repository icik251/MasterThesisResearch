import csv
from operator import le
import os
import time
import requests
from datetime import datetime
import json
from mongo_handler import MongoHandler
from collections import Counter

"""
Scraper with celery of 12 workers process around 50 requests per minute.
Therefore, setting up 100 request per minute is good to not overload the API and
have connection issues.
"""

"""
There has to be sleeping after every file or some mechanism to make sure no race condition is 
happening when adding company. Because duplicate data can be added.    
    
"""


class CompanyHandler:
    def __init__(self, path_companies_to_extract) -> None:
        self.path_companies_to_extract = path_companies_to_extract
        self.av_api_key = "DV3U4RGOYLYSQHN8"

    def add_companies(self, list_of_companies, req_per_min=100, from_year=2017):
        # Send certain amount of messages per X amount of time to prevent errors on connections
        req_count = 0

        for company in list_of_companies:
            if not self.check_company_overview(
                ticker=company["ticker"], cik=company["cik"]
            ):
                continue
            for edgar_q_name, extracted_dict in company["available_quarters"].items():
                for key_type_date, index_url in extracted_dict.items():
                    curr_type, filing_date = key_type_date.split("_")

                    if (
                        curr_type.split("-")[0] == "10"
                        and int(edgar_q_name.split("-")[0]) >= from_year
                    ):

                        post_dict = {
                            "cik": company["cik"],
                            "name": company["name"],
                            "ticker": company["ticker"],
                            "year": int(edgar_q_name.split("-")[0]),
                            "quarter": int(edgar_q_name[-1]),
                            "type": curr_type,
                            "filing_date": datetime.fromisoformat(filing_date),
                            "index_url": index_url,
                        }

                        res = requests.post(
                            "http://localhost:8000/api/v1/company/",
                            data=json.dumps(post_dict, default=str),
                        )

                        req_count += 1
                        if req_count % req_per_min == 0:
                            print("Requests made", req_count)
                            print("Sleeping...")
                            time.sleep(60)

                    elif curr_type == "4":
                        pass

    def load_chosen_companies_from_db(self):
        mongo_handler = MongoHandler()
        while not mongo_handler.connect_to_mongo():
            time.sleep(5)

        db = mongo_handler.get_database()
        # load chosen companies csv
        with open(self.path_companies_to_extract, "r") as f:
            reader = csv.reader(f)

            for idx, row in enumerate(reader):
                if idx == 0:
                    continue

                curr_company = db["company_base"].find_one({"ticker": row[0]})
                if curr_company:
                    yield curr_company
                else:
                    print("Missing data in DB for ticker ", row[0])

        mongo_handler.close_mongo_connection()

    def check_company_overview(self, ticker, cik, country="USA"):
        resp = requests.get(
            f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={self.av_api_key}"
        )
        if resp.status_code == 200 and resp.text != "{}":
            dict_of_company = json.loads(resp.text)
            if int(dict_of_company["CIK"]) != cik:
                print(f"Not matching for {ticker} | {dict_of_company['CIK']} and {cik}")
                return False
            if dict_of_company["Country"] != country:
                print(
                    f"Not matching for {ticker} | {dict_of_company['Country']} and {country}"
                )
                return False
        else:
            print(
                f"Error for AV for {ticker}, status code {resp.status_code} or empty data"
            )
            return False

        return True

    def fix_duplicate_companies(self):
        resp = requests.get("http://localhost:8000/api/v1/company/duplicated/")
        if resp.status_code == 200:
            list_of_companies = json.loads(resp.text)
        else:
            return f"Error in request: {resp.status_code}"

        list_of_fixed_companies = []
        for company in list_of_companies:
            resp = requests.put(
                f"http://localhost:8000/api/v1/company/duplicated/{company['cik']}/{company['year']}"
            )
            resp = json.loads(resp.text)
            if resp["code"] == 200:
                list_of_fixed_companies.append(json.loads(resp["data"]))
            else:
                print(f"Error on company: {company['cik']} - {company['year']}")

        return list_of_fixed_companies

    def validate_filings_type(self):

        resp = requests.get(f"http://localhost:8000/api/v1/company")
        resp = json.loads(resp.text)
        if resp["code"] == 200:
            # we want to check the most common type for the year and send to update if needed
            data = resp["data"]

            for curr_company in data:

                dict_of_id_filings_type = {}
                for quarter in curr_company["quarters"]:
                    for metadata in quarter["metadata"]:
                        dict_of_id_filings_type[metadata["_id"]] = metadata[
                            "company_type"
                        ]

                # if there is not difference in the filing types through the year or filing already changed, then skip
                already_changed = [True if ';' in str(filer) else False for filer in dict_of_id_filings_type.values()]
                if len(set(dict_of_id_filings_type.values())) == 1 or True in already_changed:
                    continue

                counter = Counter(dict_of_id_filings_type.values())
                most_common_type = counter.most_common(1)[0][0]

                for idx_q, quarter in enumerate(curr_company["quarters"]):
                    for idx_m, metadata in enumerate(quarter["metadata"]):
                        if (
                            dict_of_id_filings_type.get(metadata["_id"])
                            != most_common_type
                        ):
                            # the first is the original one ; the second is the most common type
                            curr_company["quarters"][idx_q]["metadata"][idx_m][
                                "company_type"
                            ] = (
                                str(dict_of_id_filings_type.get(metadata["_id"]))
                                + ";"
                                + most_common_type
                            )

                resp = requests.put(
                    f"http://localhost:8000/api/v1/company/filing_type/{curr_company['cik']}/{curr_company['year']}",
                    data=json.dumps(curr_company),
                )
                resp = json.loads(resp.text)
                if resp["code"] == 200:
                    print(
                        f"Changed for CIK: {curr_company['cik']} and year: {curr_company['year']}"
                    )
                else:
                    print(resp["message"])

        else:
            print(
                resp["message"],
            )
