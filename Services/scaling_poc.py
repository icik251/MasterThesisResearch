from matplotlib.pyplot import sca
from mongo_handler import MongoHandler
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import numpy as np
import pickle


def get_input_data(
    db,
    k_fold,
    split_type,
    exclude_without_label=True,
    input_data_collection: str = "input_data",
):
    input_data_list = []

    query = (
        {f"k_fold_config.{k_fold}": split_type}
        if exclude_without_label
        else {f"k_fold_config.{k_fold}": split_type, "label": None}
    )

    for input_data_dict in db[input_data_collection].find(query):
        input_data_list.append(input_data_dict)

    return input_data_list


def scaling(k_fold=1):
    mongo_handler_obj = MongoHandler()
    mongo_handler_obj.connect_to_mongo()
    db = mongo_handler_obj.get_database()

    input_data_collection = "input_data"

    list_of_train_input = get_input_data(
        db=db,
        k_fold=k_fold,
        split_type="train",
        input_data_collection=input_data_collection,
    )
    list_of_val_input = get_input_data(
        db=db,
        k_fold=k_fold,
        split_type="val",
        input_data_collection=input_data_collection,
    )

    list_of_train_perc_change = [x["percentage_change"] for x in list_of_train_input]
    list_of_val_perc_change = [x["percentage_change"] for x in list_of_val_input]

    scaler_min_max = MinMaxScaler()
    scaler_standard = StandardScaler()

    scaler_min_max.fit(np.array(list_of_train_perc_change).reshape(-1, 1))

    list_of_train_perc_change_scaled = scaler_min_max.transform(
        np.array(list_of_train_perc_change).reshape(-1, 1)
    )
    list_of_val_perc_change_scaled = scaler_min_max.transform(
        np.array(list_of_val_perc_change).reshape(-1, 1)
    )

    for idx, x in enumerate(list_of_train_perc_change_scaled):
        print(x)
        print(scaler_min_max.inverse_transform(x.reshape(1, -1)))
        print(list_of_train_input[idx]["percentage_change"])
        print(list_of_train_input[idx]["cik"])

        curr_id = list_of_train_input[idx]["_id"]
        curr_dict_min_max = list_of_train_input[idx]["percentage_change_scaled_min_max"]
        curr_dict_standard = list_of_train_input[idx][
            "percentage_change_scaled_standard"
        ]

        curr_dict_min_max[str(k_fold)] = x[0]
        # curr_dict_standard[str(k_fold)] = standard_scaled[0]

        update_query = {
            "percentage_change_scaled_min_max": curr_dict_min_max,
            "percentage_change_scaled_standard": curr_dict_standard,
        }
        db[input_data_collection].update_one(
            {"_id": curr_id}, {"$set": update_query}, upsert=False
        )
        break

    for idx, x in enumerate(list_of_val_perc_change_scaled):
        print(x)
        print(scaler_min_max.inverse_transform(x.reshape(1, -1)))
        print(list_of_val_input[idx]["percentage_change"])
        print(list_of_val_input[idx]["cik"])

        curr_id = list_of_val_input[idx]["_id"]
        curr_dict_min_max = list_of_val_input[idx]["percentage_change_scaled_min_max"]
        curr_dict_standard = list_of_val_input[idx]["percentage_change_scaled_standard"]

        curr_dict_min_max[str(k_fold)] = x[0]
        # curr_dict_standard[str(k_fold)] = standard_scaled[0]

        update_query = {
            "percentage_change_scaled_min_max": curr_dict_min_max,
            "percentage_change_scaled_standard": curr_dict_standard,
        }
        db[input_data_collection].update_one(
            {"_id": curr_id}, {"$set": update_query}, upsert=False
        )
        break

    # min_max_scaler_pkl = pickle.dumps(scaler_min_max)
    # db["storage"].insert_one({"dumped_object":min_max_scaler_pkl, "name":"MinMax", "k_fold":1})

    # doc = db["storage"].find_one({"name":"MinMax"})
    # min_max_loaded = pickle.loads(doc["dumped_object"])

    # print(min_max_loaded.inverse_transform(x.reshape(1,-1)))


scaling(3)
# scaling(1)
# scaling(2)
