from collections import defaultdict
import csv
import itertools
import json
import multiprocessing
import os
import pandas as pd
import pymongo
from datetime import datetime

import multiprocessing as mp
from multiprocessing import Pool

from itertools import repeat


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


def add_company_base_to_db(db, key_ticker, value_dict, dict_of_edgar):
    for key_cik, list_quarters in value_dict.items():
        dict_to_add_to_db = {
            "ticker": key_ticker,
            "cik": int(key_cik),
            "available_quarters": {},
        }
        for quarter in sorted(list(set(list_quarters))):
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
            print(f"{key_cik} added successfully")
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
    for key_ticker, dict_values in res_cik_ticker.items():
        add_company_base_to_db(db, key_ticker, dict_values, dict_of_edgar)

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
# finalize_company_base(
#     path_res_cik_ticker_json="APIClient/data/cik_to_ticker/res_cik_ticker_till_2022-QTR1.json",
#     path_edgar="APIClient/data/edgar",
# )
