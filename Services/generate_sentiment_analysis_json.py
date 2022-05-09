from collections import defaultdict
import json
import os

from mongo_handler import MongoHandler
import random

random.seed(42)

def generate(path_to_input_txt, output_dir, val_split_percentage=0.2):
    list_of_all_samples = []
    list_of_dicts_train_to_save = []
    list_of_dicts_val_to_save = []
    with open(path_to_input_txt, "r") as f:
        list_of_lines = f.readlines()
    for idx, line in enumerate(list_of_lines):
        text, label = line.split("@")
        if label.strip() == "neutral":
            continue
            
        list_of_all_samples.append({"text": text.strip(), "label": label.strip()})

    random.shuffle(list_of_all_samples)
    num_of_val_samples = int(len(list_of_all_samples) * val_split_percentage)
    list_of_dicts_val_to_save = list_of_all_samples[:num_of_val_samples]
    list_of_dicts_train_to_save = list_of_all_samples[num_of_val_samples:]
    save_json_input(list_of_dicts_train_to_save, list_of_dicts_val_to_save, output_dir)

def save_json_input(list_of_train_dicts, list_of_val_dicts, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(os.path.join(output_dir, "train.json"), 'w') as f:
        json.dump(list_of_train_dicts, f)

    with open(os.path.join(output_dir, "val.json"), 'w') as f:
        json.dump(list_of_val_dicts, f)
        

generate("Services/data/financial_phrasebank/Sentences_50Agree.txt", 'Services/data/financial_phrasebank_json')