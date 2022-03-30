import json


def load_data(filepath) -> list:
    with open(filepath, "r") as f:
        json_str = f.read()

    return json.loads(json_str)


json_data = load_data("KnowledgeGraphs/data/re-nlg_0-10000.json")


for item in json_data:
    print(item)
