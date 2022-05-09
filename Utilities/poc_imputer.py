import json
import requests
from collections import defaultdict

from transformers import NerPipeline


dict_of_res = {}
year=2017
q=2
resp = json.loads(requests.get(f"http://localhost:8000/api/v1/input_data/{year}/{q}").text)
if resp["code"] == 200:
    dict_of_res[str(year) + "_" + str(q)] = resp["data"]
else:
    print(year, q, resp["code"])

# Take into account only companies on time
for year_q_k, list_of_data in dict_of_res.items():
    dict_of_industry_id_list_values = defaultdict(dict)
    for input_data in list_of_data:
        list_of_current_values_to_add = []
        for kpi in input_data["fundamental_data"]:
            list_of_current_values_to_add.append((kpi, input_data['fundamental_data_imputed_past'].get(kpi, None)))
        dict_of_industry_id_list_values[input_data['industry']][input_data['_id']] = (input_data["is_filing_on_time"], list_of_current_values_to_add)

from sklearn.impute import KNNImputer

for industry_key, id_dict_list_values in dict_of_industry_id_list_values.items():
    if industry_key == "REFUSE SYSTEMS":
        print(1)
    list_for_industry_imputer = []
    list_of_all_for_industry = []
    initial_kpi_idx_mappper = {}
    create_imputer_for_industry = False
    for id_k, (on_time, tuple_kpi_value) in id_dict_list_values.items():
        if id_k == "627948edda0566101f548285":
            print(1)
        curr_id_list = [None] * len(tuple_kpi_value)
        # Fill initial kpi idx mapper on first possible data
        if on_time and not initial_kpi_idx_mappper:
            for idx, (kpi, value) in enumerate(tuple_kpi_value):
                initial_kpi_idx_mappper[kpi] = idx
                curr_id_list[idx] = value
                if not value:
                    create_imputer_for_industry = True

            list_for_industry_imputer.append((id_k, curr_id_list))
            list_of_all_for_industry.append((id_k, curr_id_list))
            
        elif on_time and initial_kpi_idx_mappper:
            for idx, (kpi, value) in enumerate(tuple_kpi_value):
                curr_id_list[idx] = value
                if not value:
                    create_imputer_for_industry = True

            list_for_industry_imputer.append((id_k,curr_id_list))
            list_of_all_for_industry.append((id_k, curr_id_list))
            
        elif not on_time:
            for idx, (kpi, value) in enumerate(tuple_kpi_value):
                curr_id_list[idx] = value
                if not value:
                    create_imputer_for_industry = True
            list_of_all_for_industry.append((id_k, curr_id_list))
        
    # If there is None, create KNNImputer with the following list
    if create_imputer_for_industry:
        curr_imputer = KNNImputer(n_neighbors=1)
        list_for_imputation = [i[1] for i in list_for_industry_imputer]
        
        is_imputation_possible = False
        for list_of_values_curr_company in list_for_imputation:
            if None not in list_of_values_curr_company:
                is_imputation_possible = True
                break
        
        if not is_imputation_possible:
            # message and break
            break
            
        curr_imputer.fit(list_for_imputation)

        for id, list_of_values_for_id in list_of_all_for_industry:
            dict_of_curr_imputed_full = {}
            if None in list_of_values_for_id:
                imputed_list_of_values_for_id = curr_imputer.transform([list_of_values_for_id]).tolist()[0]
            else:
                imputed_list_of_values_for_id = list_of_values_for_id
            
            try:
                for kpi, idx in initial_kpi_idx_mappper.items():
                    dict_of_curr_imputed_full[kpi] = imputed_list_of_values_for_id[initial_kpi_idx_mappper[kpi]]
            except:
                print(1)
            # Save to DB using the _id
            
            
                
                
            
    