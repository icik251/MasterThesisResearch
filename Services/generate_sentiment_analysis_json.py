from collections import defaultdict
import json
import os

from mongo_handler import MongoHandler


def generate(path_to_input_txt, output_dir):
    list_of_dicts_train_to_save = []
    list_of_dicts_val_to_save = []
    with open(path_to_input_txt, "r") as f:
        list_of_lines = f.readlines()
    for idx, line in enumerate(list_of_lines):
        text, label = line.split("@")
        if label.strip() == "neutral":
            continue
        # Only for test purposes creating train val splits
        if idx < 300:
            list_of_dicts_train_to_save.append({"text": text.strip(), "label": label.strip()})
        else:
            list_of_dicts_val_to_save.append({"text": text.strip(), "label": label.strip()})

    save_json_input(list_of_dicts_train_to_save, list_of_dicts_val_to_save, output_dir)
    

def save_json_input(list_of_train_dicts, list_of_val_dicts, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(os.path.join(output_dir, "train.json"), 'w') as f:
        json.dump(list_of_train_dicts, f)

    with open(os.path.join(output_dir, "val.json"), 'w') as f:
        json.dump(list_of_val_dicts, f)
        

generate("Services/data/financial_phrasebank/Sentences_AllAgree.txt", 'Services/data/financial_phrasebank_json')