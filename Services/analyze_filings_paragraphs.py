from collections import defaultdict
import json
import os

from mongo_handler import MongoHandler


def analyze_filings_paragraphs():
    mongo_handler_obj = MongoHandler()
    mongo_handler_obj.connect_to_mongo()
    db = mongo_handler_obj.get_database()
    data = db["input_data"].find({})

    dict_of_paragraphs_titles = defaultdict(int)
    for input in data:
        for k, v in input["risk_paragraphs"].items():
            dict_of_paragraphs_titles[k] += 1

    dict_of_paragraphs_titles = dict(
        sorted(dict_of_paragraphs_titles.items(), key=lambda item: item[1], reverse=True)
    )
    with open("Services/data/risk_paragraphs_count.json", "w") as f:
        json.dump(dict_of_paragraphs_titles, f)


analyze_filings_paragraphs()
