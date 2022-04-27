from collections import defaultdict
import os
import traceback
import json
from mongo_handler import MongoHandler


def process_company(list_of_company):
    for company_year in list_of_company:
        for q_dict in company_year["quarters"]:
            for curr_metadata in q_dict["metadata"]:
                try:
                    # mda_section_splitted = curr_metadata["mda_section"].split("\n")
                    # dict_of_paragraphs_mda = reformat_section(mda_section_splitted)
                    # dict_of_data["mda_paragraphs"] = dict_of_paragraphs_mda
                    file_name = (
                        str(company_year["year"])
                        + "_"
                        + str(q_dict["q"])
                        + "_"
                        + curr_metadata["type"]
                    )
                    reformat_section_new(
                        curr_metadata["mda_section"], file_name, company_year["cik"]
                    )
                except Exception as e:
                    print(traceback.format_exc())
                    # dict_of_data["mda_paragraphs"] = {}


def reformat_section_new(mda_section_text, file_name, cik):
    mda_section_splitted = mda_section_text.split("\n")

    dict_of_res = {}
    for idx, splitted in enumerate(mda_section_splitted):
        if splitted != "":
            dict_of_res[idx] = splitted

    with open(
        os.path.join(
            "D:\PythonProjects\MasterThesisResearch\Services\data\poc_output",
            f"{cik}_{file_name}_poc_text_processing.json",
        ),
        "w",
    ) as f:
        json.dump(dict_of_res, f)


def reformat_section(section_splitted, threshold_for_paragraph=20):
    """
    Things we remove:
    - string ending on ':' (because a table follows),
    - [DATA_TABLE_REMOVED],
    - MDA and QQD section names

    """
    list_of_after_table_numbers = [
        "(1)",
        "(2)",
        "(3)",
        "(4)",
        "(5)",
        "(6)",
        "(7)",
        "(8)",
        "(9)",
    ]
    dict_of_paragraphs = defaultdict(str)
    curr_paragraph_key = None
    paragraph_title = False
    len_section_spitted = len(section_splitted)
    for idx in range(len_section_spitted):
        section_splitted[idx] = section_splitted[idx].strip()
        # print(section_splitted[idx])
        # captures what we want to skip
        if (
            len(section_splitted[idx]) <= 3
            or section_splitted[idx].endswith(":")
            or section_splitted[idx] == "[DATA_TABLE_REMOVED]"
            or "_" in section_splitted[idx]
            or idx == 0
            or idx == len_section_spitted - 1
            or section_splitted[idx].split()[0] in list_of_after_table_numbers
        ):
            continue
        elif len(section_splitted[idx].split()) <= threshold_for_paragraph:
            if paragraph_title:
                curr_paragraph_key += "|" + section_splitted[idx]
            else:
                curr_paragraph_key = section_splitted[idx]
            paragraph_title = True
        else:
            dict_of_paragraphs[str(curr_paragraph_key)] += (
                section_splitted[idx]
            ) + "\n"
            paragraph_title = False

    return dict_of_paragraphs


cik = 1537028
mongo_handler = MongoHandler()
client = mongo_handler.connect_to_mongo()
db = mongo_handler.get_database()
curr_company = db["company"].find({"cik": cik})
list_company = []
for company in curr_company:
    list_company.append(company)

process_company(list_company)
