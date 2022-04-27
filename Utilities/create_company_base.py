from collections import defaultdict
import csv
import itertools
import json
import multiprocessing
import os
import time
import pandas as pd
import pymongo
from datetime import datetime

import multiprocessing as mp
from multiprocessing import Pool

from itertools import repeat

import requests


def create_cik_ticker_quarters(path_cik_ticker_final_json: str):
    with open(path_cik_ticker_final_json, "r") as fp:
        dict_cik_tickername = json.load(fp)

    result_dict = {}
    # Iterate through EDGAR files
    columns = ["cik", "name", "type", "year", "link1", "link2"]
    for file in os.listdir(path="APIClient\data\edgar"):
        start_time = datetime.now()
        file_name = file.split(".")[0]
        curr_df_edgar = pd.read_csv(
            os.path.join("APIClient/data/edgar", file), delimiter="|", names=columns
        )
        curr_list_of_ciks = curr_df_edgar.cik.to_list()

        print("Doing", file_name)
        for cik_k, tickname_val in dict_cik_tickername.items():
            cik_k = int(cik_k)
            ticket = tickname_val[0]
            if cik_k not in curr_list_of_ciks:
                continue

            if ticket in result_dict:
                if cik_k in result_dict[ticket]:
                    result_dict[ticket][cik_k].append(file_name)
                else:
                    result_dict[ticket] = {cik_k: [file_name]}
            else:
                result_dict[ticket] = {cik_k: [file_name]}

        with open(
            "APIClient/data/cik_to_ticker/res_cik_ticker_till_{}.json".format(
                file_name
            ),
            "w",
        ) as fp:
            json.dump(result_dict, fp, indent=4)

        print("Data for {} done in {}".format(file_name, datetime.now() - start_time))

def get_company_overview(ticker, api_key="DV3U4RGOYLYSQHN8"):
    resp = requests.get(
            f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
        )
    try:
        if resp.status_code == 200 and resp.text != "{}":
            dict_of_company = json.loads(resp.text)
            print(ticker, "successful overview")
            return dict_of_company
        else:
            print(ticker, resp.status_code)
            return None
    except Exception as e:
        print(ticker, e)
        return None

LIST_YEAR_TO_PROCESS = [2017, 2018, 2019, 2020, 2021, 2022]
def add_company_base_to_db(db, key_ticker, value_dict, dict_of_edgar):
    for key_cik, list_quarters in value_dict.items():
        # Make check using AlphaVantage API and the overview for each company
        
        dict_of_company_overview = get_company_overview(key_ticker)
        
        if not dict_of_company_overview or dict_of_company_overview.get("Country", None) != "USA":
            print(key_ticker, "skipping because empty or not in USA")
            return     
        
        try:
            av_cik = int(dict_of_company_overview.get("CIK", None)) if dict_of_company_overview.get("CIK", None) else None
        except:
            av_cik = None
            
        av_name = dict_of_company_overview.get("Name", None)
        asset_type = dict_of_company_overview.get("AssetType", None)
        exchange = dict_of_company_overview.get("Exchange", None)
        currency = dict_of_company_overview.get("Currency", None)
        country = dict_of_company_overview.get("Country", None)
        sector = dict_of_company_overview.get("Sector", None)
        industry = dict_of_company_overview.get("Industry", None)
        fiscal_year_end = dict_of_company_overview.get("FiscalYearEnd", None)
        
        
        dict_to_add_to_db = {
            "ticker": key_ticker,
            "cik": int(key_cik),
            "av_cik": av_cik,
            "av_name": av_name,
            "asset_type": asset_type,
            "exchange": exchange,
            "currency": currency,
            "country": country,
            "sector": sector,
            "industry": industry,
            "fiscal_year_end": fiscal_year_end,
            "available_quarters": {},
        }
        for quarter in sorted(list(set(list_quarters))):
            if int(quarter.split("-")[0]) not in LIST_YEAR_TO_PROCESS:
                continue
            
            curr_quarter_data = dict_of_edgar.get(quarter, None)
            if not curr_quarter_data:
                continue

            list_of_matches = list(
                filter(lambda x: key_cik == x.split("|")[0], curr_quarter_data)
            )

            if list_of_matches:
                dict_to_add_to_db["available_quarters"][quarter] = {}

            for match_row in list_of_matches:
                splitted_match_row = match_row.split("|")
                curr_name = splitted_match_row[1]
                curr_type = splitted_match_row[2]
                curr_date = splitted_match_row[3]
                curr_htm = "https://www.sec.gov/Archives/" + splitted_match_row[4]

                # make custom key by combining type and date. This is done
                # because some companies have 10-Q, 10-K in the same quarter
                # and this also allows us to support 8-K and other types which are
                # duplicating by quarter if we use just type as key
                curr_type_date_key = curr_type.replace("/", "-") + "_" + curr_date

                dict_to_add_to_db["name"] = curr_name
                dict_to_add_to_db["available_quarters"][quarter][
                    curr_type_date_key
                ] = curr_htm

        if len(dict_to_add_to_db["available_quarters"]) == 0:
            continue

        try:
            db["company_base"].insert_one(dict_to_add_to_db)
            print(f"{key_ticker} added successfully")
        except Exception as e:
            print("Problem with", key_cik, "and", key_ticker)


def finalize_company_base(
    path_res_cik_ticker_json="APIClient\data\cik_to_ticker/res_cik_ticker_till_2022-QTR1.json",
    path_edgar="APIClient/data/edgar",
):
    # load all files
    dict_of_edgar = get_edgar_data(path_edgar=path_edgar)

    with open(path_res_cik_ticker_json, "r") as fp:
        res_cik_ticker = json.load(fp)

    client = pymongo.MongoClient("mongodb://root:root@localhost:27017")
    db = client.SP500_DB

    # use multiprocessing and pool
    count = 0
    len_res_cik_ticker = len(res_cik_ticker)
    start_time = datetime.now()
    for key_ticker, dict_values in res_cik_ticker.items():
        res_db = db["company_base"].find_one({"ticker": key_ticker})
        if res_db:
            print(key_ticker, "already added, skipping!")
            print('-------------------------------------')
            count += 1
            continue
        
        add_company_base_to_db(db, key_ticker, dict_values, dict_of_edgar)
        print('-------------------------------------')
        count += 1
        if count % 50 == 0:
            # print("Sleeping 60 seconds...")
            # time.sleep(60)
            print(f"{count}/{len_res_cik_ticker} processed!")
            print(f"Time elapsed: {datetime.now() - start_time}")
    client.close()


def get_edgar_data(path_edgar="APIClient/data/edgar", possible_types=["10-Q", "10-K"]):
    if os.path.isfile("APIClient\data\cik_to_ticker\dict_edgar.json"):
        with open("APIClient\data\cik_to_ticker\dict_edgar.json", "r") as fp:
            dict_of_edgar = json.load(fp)
            return dict_of_edgar

    dict_of_edgar = dict()
    for file in os.listdir(path_edgar):
        dict_of_edgar[file.split(".")[0]] = []
        with open(os.path.join(path_edgar, file), "r") as tsvIn:
            reader = csv.reader(tsvIn, delimiter="|")
            for row_company in reader:
                for possible_type in possible_types:
                    if row_company[2] == "4" or (
                        possible_type in row_company[2] and row_company[2] != "4"
                    ):
                        dict_of_edgar[file.split(".")[0]].append(
                            row_company[0]
                            + "|"
                            + row_company[1]
                            + "|"
                            + row_company[2]
                            + "|"
                            + row_company[3]
                            + "|"
                            + row_company[5]
                        )
        print(file, "done")

    with open(
        "APIClient\data\cik_to_ticker\dict_edgar.json",
        "w",
    ) as fp:
        json.dump(dict_of_edgar, fp, indent=4)

    return dict_of_edgar


def reformat_cik_ticker_json(path_to_json, path_to_save):
    with open(path_to_json, "r") as fp:
        dict_cik_ticker = json.load(fp)

    res_dict_cik_ticker = {}
    for key_idx, value_dict in dict_cik_ticker.items():
        res_dict_cik_ticker[value_dict["cik_str"]] = (
            value_dict["ticker"],
            value_dict["title"],
        )

    with open(
        path_to_save,
        "w",
    ) as fp:
        json.dump(res_dict_cik_ticker, fp, indent=4)


# Reformat the CIK Ticker json
# reformat_cik_ticker_json(
#     "APIClient\data\cik_to_ticker\cik_ticker_sec_2.json",
#     "APIClient\data\cik_to_ticker\cik_ticker_sec_final.json",
# )

# Create the json that is going to be used to add companies in DB
# create_cik_ticker_quarters("APIClient\data\cik_to_ticker\cik_ticker_sec_final.json")

# Save to DB using multiprocessing
finalize_company_base(
    path_res_cik_ticker_json="APIClient/data/cik_to_ticker/res_cik_ticker_till_2022-QTR1.json",
    path_edgar="APIClient/data/edgar",
)
