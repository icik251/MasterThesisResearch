import csv
import os
import time
import requests
from datetime import datetime
import json
from mongo_handler import MongoHandler

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

    def add_companies(self, list_of_companies, req_per_min=100):
        # Send certain amount of messages per X amount of time to prevent errors on connections
        req_count = 0

        for company in list_of_companies:
            for edgar_q_name, extracted_dict in company["available_quarters"].items():
                for key_type_date, index_url in extracted_dict.items():
                    curr_type, filing_date = key_type_date.split("_")

                    if curr_type.split("-")[0] == "10":

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

                # country = row[6]
                # if country != "United States":
                #     continue

                curr_company = db["company_base"].find_one({"ticker": row[0]})
                if curr_company:
                    yield curr_company
                else:
                    print("Missing data in DB for ticker ", row[0])

        mongo_handler.close_mongo_connection()

    def fix_duplicate_companies(self):
        resp = requests.get("http://localhost:8000/api/v1/company/")
        if resp.status_code == 200:
            list_of_companies = json.loads(resp.text)
        else:
            return f"Error in request: {resp.status_code}"

        list_of_fixed_companies = []
        for company in list_of_companies:
            resp = requests.get(
                f"http://localhost:8000/api/v1/company/{company['cik']}/{company['year']}"
            )
            if resp.status_code == 200:
                list_of_fixed_companies.append(json.loads(resp.text))
            else:
                print(f"Error on company: {company['cik']} - {company['year']}")

        return list_of_fixed_companies
