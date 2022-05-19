import json

import requests


class AdapterDataHandler:
    def __init__(
        self,
        path_to_adapter_data="D:/PythonProjects/MasterThesisResearch/APIClient/data/adapter_dataset/extracted_manualy.json",
    ) -> None:
        self.path_to_adapter_data = path_to_adapter_data

    def add_samples(self):
        for idx, adapter_data in enumerate(self.unpack_samples()):
            resp = json.loads(
                requests.post(
                    f"http://localhost:8000/api/v1/adapter_data/",
                    data=json.dumps(adapter_data),
                ).text
            )
            if idx % 500 == 0:
                print(resp["code"], "|", resp["message"], idx, "added")
        print("All adapter samples added to queue")

    def unpack_samples(self):
        with open(self.path_to_adapter_data, "r") as f:
            dict_of_extracted_data = json.load(f)

        set_of_processed = set()
        
        for dict_extracted_data in dict_of_extracted_data.values():
            if dict_extracted_data["original_keep"] != "y":
                continue

            original_label = dict_extracted_data["original_label"]
            if dict_extracted_data["original"] not in set_of_processed:
                yield {
                    "text": dict_extracted_data["original"],
                    "data_type": "original",
                    "label": original_label,
                }
                set_of_processed.add(dict_extracted_data["original"])

            for i in range(3):
                curr_text, _, curr_keep = dict_extracted_data[f"similar_{i}"]
                if curr_text in set_of_processed:
                    continue

                if original_label == "positive" and curr_keep == "y":
                    yield {
                        "text": curr_text,
                        "data_type": "extracted",
                        "label": original_label,
                    }
                    set_of_processed.add(curr_text)
                elif (original_label == "positive" and curr_keep == "nn") or (
                    original_label == "negative" and curr_keep == "y"
                ):
                    yield {
                        "text": curr_text,
                        "data_type": "extracted",
                        "label": "negative",
                    }
                    set_of_processed.add(curr_text)
                

    def create_k_folds(self, k_folds):
        resp = json.loads(
            requests.post(
                f"http://localhost:8000/api/v1/adapter_data/k_folds/",
                data=json.dumps({"k_folds": k_folds}),
            ).text
        )
        print(resp["code"], "|", resp["message"])
