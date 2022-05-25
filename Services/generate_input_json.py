from collections import defaultdict
import json
import os

from sklearn.model_selection import KFold

from mongo_handler import MongoHandler


def generate(output_dir):
    mongo_handler_obj = MongoHandler()
    mongo_handler_obj.connect_to_mongo()
    db = mongo_handler_obj.get_database()
    data = db["input_data"].find({"is_used": True})

    dict_of_k_fold_1 = defaultdict(list)
    dict_of_k_fold_2 = defaultdict(list)
    dict_of_k_fold_3 = defaultdict(list)
    dict_of_k_fold_4 = defaultdict(list)
    dict_of_k_fold_5 = defaultdict(list)
    
    for input in data:
        for k_fold, split_type in input["k_fold_config"].items():
            if split_type == "test":
                continue
            curr_dict = {}
            # Text data
            curr_dict["mda_paragraphs"] = input["mda_paragraphs"]
            curr_dict["mda_sentences"] = input["mda_sentences"]
            # Numerical data and categorical
            curr_dict["features_scaled_min_max"] = input["features_scaled_min_max"][k_fold]
            curr_dict["features_scaled_standard"] = input["features_scaled_standard"][k_fold]
            curr_dict["features_scaled_robust"] = input["features_scaled_robust"][k_fold]
            curr_dict["is_filing_on_time"] = input["is_filing_on_time"]
            
            # Labels and split types
            curr_dict["percentage_change"] = input["percentage_change"]
            curr_dict["percentage_change_min_max"] = input["percentage_change_scaled_min_max"][
                k_fold
            ]
            curr_dict["percentage_change_standard"] = input[
                "percentage_change_scaled_standard"
            ][k_fold]
            curr_dict["percentage_change_robust"] = input[
                "percentage_change_scaled_robust"
            ][k_fold]
            
            curr_dict["label"] = input["label"]
            curr_dict["year"] = input["year"]
            curr_dict["split_type"] = split_type

            if int(k_fold) == 1:
                dict_of_k_fold_1[split_type].append(curr_dict)
            elif int(k_fold) == 2:
                dict_of_k_fold_2[split_type].append(curr_dict)
            elif int(k_fold) == 3:
                dict_of_k_fold_3[split_type].append(curr_dict)
            elif int(k_fold) == 4:
                dict_of_k_fold_4[split_type].append(curr_dict)
            elif int(k_fold) == 5:
                dict_of_k_fold_5[split_type].append(curr_dict)
                
                
    save_k_fold_input(dict_of_k_fold_1, 1, output_dir)
    save_k_fold_input(dict_of_k_fold_2, 2, output_dir)
    save_k_fold_input(dict_of_k_fold_3, 3, output_dir)
    save_k_fold_input(dict_of_k_fold_4, 4, output_dir)
    save_k_fold_input(dict_of_k_fold_5, 5, output_dir)


def save_k_fold_input(k_fold_dict, k_fold, output_dir):
    output_dir = os.path.join(output_dir, f'k_fold_{k_fold}')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(os.path.join(output_dir, "train.json"), 'w') as f:
        json.dump(k_fold_dict["train"], f)

    with open(os.path.join(output_dir, "val.json"), 'w') as f:
        json.dump(k_fold_dict["val"], f)
        

generate('Services/data/input')