from venv import create
from mongo_handler import MongoHandler
from sklearn.model_selection import KFold

def create_k_fold_configs(list_of_input_data, k_folds):
    list_of_input_data_train_val_corpus = []
    list_of_input_data_test_corpus = []
    for input_data in list_of_input_data:
        if input_data["year"] != 2021:
            list_of_input_data_train_val_corpus.append(input_data)
        else:
            list_of_input_data_test_corpus.append(input_data)

    # Create k_folds for train val
    k_fold_obj = KFold(k_folds, random_state=42, shuffle=True)
    # group_k_fold.get_n_splits(list_of_input_data_train_val_corpus)

    for k_fold_idx, (train_index, val_index) in enumerate(k_fold_obj.split(
        list_of_input_data_train_val_corpus)
    ):
        for idx in train_index:
            list_of_input_data_train_val_corpus[idx]["k_fold_config"][str(k_fold_idx+1)] = "train"
        for idx in val_index:
            list_of_input_data_train_val_corpus[idx]["k_fold_config"][str(k_fold_idx+1)] = "val"
    
    list_test_keys = list(range(1,k_folds+1))
    list_test_keys = [str(k) for k in list_test_keys]
    k_fold_config_test = dict(zip(list_test_keys, ["test"]*k_folds))
    
    for input_data in list_of_input_data_test_corpus:
        pass


mongo_handler = MongoHandler()
client = None
while not client:
    client = mongo_handler.connect_to_mongo()

db = mongo_handler.get_database()


company_res = db["input_data"].find({"is_used": True})
input_data_list = []
for item in company_res:
    input_data_list.append(item)

create_k_fold_configs(input_data_list, 5)
