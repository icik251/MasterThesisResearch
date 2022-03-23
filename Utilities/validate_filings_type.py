from collections import defaultdict
import os
import sys

base_dir = os.getcwd()
sys.path.append(base_dir)
sys.path.append(os.path.join(base_dir, "APIClient"))

from APIClient.company_handler import CompanyHandler
import requests
import json


def get_companies():

    company_handler = CompanyHandler(
        path_companies_to_extract="D:/PythonProjects/MasterThesisResearch/APIClient/data/nasdaq/energy_all.csv",
    )

    list_of_companies_base = []
    for company in company_handler.load_chosen_companies_from_db():
        list_of_companies_base.append(company)

    list_of_companies = []
    for company in list_of_companies_base:
        resp = json.loads(
            requests.get(f"http://localhost:8000/api/v1/company/{company['cik']}").text
        )

        if resp["code"] == 200:
            list_of_companies.append(resp["data"])

    return list_of_companies


def validate_filings_type(list_of_companies):
    dict_of_cik_types = defaultdict(lambda: defaultdict(list))
    for company_list in list_of_companies:
        for company_for_year in company_list:
            for quarter in company_for_year["quarters"]:
                for metadata in quarter["metadata"]:
                    dict_of_cik_types[company_for_year["cik"]][
                        company_for_year["year"]
                    ].append(metadata["company_type"])

    with open(f"Utilities/data/validate_filings_type_{len(os.listdir('Utilities/data'))+1}.json", "w") as f:
        json.dump(dict_of_cik_types, f)


list_of_companies = get_companies()
validate_filings_type(list_of_companies)
