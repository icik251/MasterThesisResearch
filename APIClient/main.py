import time
from company_handler import CompanyHandler
from time_series_handler import TimeSeriesHandler
from input_data_handler import InputDataHandler
from fundamental_data_handler import FundamentalDataHandler
from adapter_data_handler import AdapterDataHandler


def time_series_logic(url):
    # Adding time series for companies

    timeseries_handler = TimeSeriesHandler(url)
    timeseries_handler.add_timeseries()


def companies_logic(sector="ENERGY & TRANSPORTATION", req_per_min=300):
    # Adding companies and fix duplicated ones

    company_handler = CompanyHandler(sector=sector)

    list_of_companies = []
    for company in company_handler.load_companies_by_sector_from_db():
        list_of_companies.append(company)

    company_handler.add_companies(
        list_of_companies, req_per_min=req_per_min, from_year=2017
    )

    print("Finished adding companies")
    input("Press Enter to continue when compranies are added...")
    company_handler.validate_filings_type()
    res = company_handler.fix_duplicate_companies()
    for company in res:
        print(company["cik"])
        print(company["year"])
        print("---------")


def input_data_logic(path_to_cleaned_df="APIClient/data/df_text_cleaned.csv"):
    input_data_obj = InputDataHandler(path_to_cleaned_df)
    input_data_obj.add_input_data()


def adapter_data_logic(k_folds=5):
    adapter_data_obj = AdapterDataHandler()
    adapter_data_obj.add_samples()
    input("Press Enter to continue to create k folds for Adapter data...")
    adapter_data_obj.create_k_folds(k_folds)


def fundamental_data_logic(path_to_cleaned_df="APIClient/data/df_text_cleaned.csv"):
    fundamental_data_obj = FundamentalDataHandler(path_to_cleaned_df)
    fundamental_data_obj.add_fundamental_data()


def fundamental_data_feature_engineering(
    url_modify="http://localhost:8000/api/v1/fundamental_data/average/",
    url_feature_engineer="http://localhost:8000/api/v1/fundamental_data/feature_engineering/",
):
    fundamental_data_obj = FundamentalDataHandler()
    fundamental_data_obj.modify_input_data_using_kpis(url_modify)
    input("Press Enter to continue to feature engineering...")
    fundamental_data_obj.feature_engineer(url_feature_engineer)


def fundamental_data_impute_using_knn(
    url="http://localhost:8000/api/v1/fundamental_data/impute_knn/",
):
    fundamental_data_obj = FundamentalDataHandler()
    fundamental_data_obj.modify_input_data_using_kpis(url)


def set_is_used_input_data(
    list_of_ciks_to_remove=[],
    list_of_ids_to_remove=[],
    lacking_features_is_used=False,
    outlier_target_is_used=False,
):
    input_data_obj = InputDataHandler()
    input_data_obj.set_is_used(
        list_of_ciks_to_remove,
        list_of_ids_to_remove,
        lacking_features_is_used=lacking_features_is_used,
        outlier_target_is_used=outlier_target_is_used,
    )


def create_k_folds_logic(k_folds=5):
    input_data_obj = InputDataHandler()
    input_data_obj.create_k_folds(k_folds)


def scaling_logic_features(list_of_features_to_scale, features_name):
    input_data_obj = InputDataHandler()
    input_data_obj.scaling_features(list_of_features_to_scale, features_name)


def scaling_logic():
    input_data_obj = InputDataHandler()
    input_data_obj.scaling_labels()


def scaling_logic_test_set():
    input_data_obj = InputDataHandler()
    input_data_obj.scaling_labels_test_set()


def add_adversarial_samples(
    dict_of_sentiment_sentences={
        "positive": "The company is doing awesome",
        "negative": "Everything is going extremely bad",
    }
):
    input_data_obj = InputDataHandler()
    input_data_obj.create_adversarial_samples(dict_of_sentiment_sentences)


if __name__ == "__main__":
    # companies_logic(sector="ENERGY & TRANSPORTATION", req_per_min=100)
    # input("Press Enter to continue to add stock prices...")
    # time_series_logic(url="http://localhost:8000/api/v1/stock_price/")
    # input("Press Enter to continue to add inflation adjusted stock prices...")
    # time_series_logic(url="http://localhost:8000/api/v1/stock_price/inflation/")
    """
    Generate df_text_cleaned before proceeding
    """
    # input("Press Enter to continue to add fundamental data...")
    # fundamental_data_logic(path_to_cleaned_df="APIClient/data/df_text_cleaned.csv")
    # input("Press Enter to continue to add input data...")
    # input_data_logic(path_to_cleaned_df="APIClient/data/df_text_cleaned.csv")
    # """
    # Analyze using the past data to see which CIKs to remove in this list
    # """
    # input("Press Enter to continue to set is_used initial...")
    # list_of_ciks_to_remove = [
    #     799233,
    #     746515,
    #     1134115,
    #     1160791,
    #     1509589,
    #     1383650,
    #     716006,
    #     917225,
    #     1000229,
    #     1011509,
    #     314203,
    #     742278,
    #     1575828,
    #     1612720,
    #     799167,
    #     783324,
    #     82020,
    #     1634447,
    #     1433270,
    #     352955,
    #     1021635,
    #     1110805,
    #     1064728,
    #     1172358,
    #     839470,
    #     1726126,
    # ]
    # set_is_used_input_data(list_of_ciks_to_remove)
    # input("Press Enter to continue to impute using knn...")
    # fundamental_data_impute_using_knn()
    # input("Press Enter to continue to calculate averages/medians...")
    # fundamental_data_feature_engineering()
    # input(
    #     "Press Enter to continue to set is_used_to false for all companies lacking features from the engineered ones..."
    # )
    # set_is_used_input_data(lacking_features_is_used=True)
    # """
    # Analyze using the results_analysis to see which input_ids to remove
    # """
    # input(
    #     "Press Enter to continue to set is_used_to false for the outliers with the percentage change..."
    # )
    # For sigma 3
    # list_of_ids_to_remove = [
    #     "628b9aa8d6c2de67e0c1a806",
    #     "628b9aa9cd3956a068c1a809",
    #     "628b9aaa800f2af4c0c1a809",
    #     "628b9abf3ea0e0086dc1a809",
    #     "628b9ac3d5ad578a08c1a809",
    #     "628b9ac40b1c4bfcd8c1a809",
    #     "628b9ac44745f6cc25c1a809",
    #     "628b9ac610db4138fdc1a809",
    #     "628b9aca308235fd01c1a809",
    #     "628b9ad500f4640003c1a809",
    #     "628b9ada6ae9b88e64c1a809",
    #     "628b9ae2f1b00d1f16c1a809",
    #     "628b9ae284b11b075cc1a807",
    #     "628b9ae284b11b075cc1a809",
    #     "628b9aea55569bea2cc1a808",
    #     "628b9af0270a8c5bc0c1a807",
    #     "628b9af684c8ce6b05c1a809",
    #     "628b9afba50aac7853c1a809",
    #     "628b9b00d872e5ece9c1a7fc",
    #     "628b9b0894a2c7834ec1a802",
    #     "628b9b0894a2c7834ec1a805",
    #     "628b9b091a9c0bbb15c1a809",
    #     "628b9b138c2aeac1cdc1a806",
    #     "628b9b143a0de5b1eac1a806",
    #     "628b9b161380c07e33c1a809",
    #     "628b9b1cad481bd293c1a809",
    #     "628b9b23d7066f2587c1a806",
    #     "628b9b267070d04c7ec1a809",
    #     "628b9b26aedd086a82c1a801",
    #     "628b9b2fba911aa031c1a802",
    #     "628b9b395f51f73a68c1a807",
    #     "628b9b395f51f73a68c1a809",
    #     "628b9b3cc18689d98bc1a809",
    #     "628b9b3dedbb1a2982c1a809",
    #     "628b9b3f9d8968d1e2c1a809",
    # ]
    # set_is_used_input_data(
    #     list_of_ids_to_remove=list_of_ids_to_remove, outlier_target_is_used=True
    # )
    # input("Press Enter to continue to create k-folds...")
    # create_k_folds_logic(k_folds=5)
    # input("Press Enter to continue to scale labels according to k_folds...")
    # scaling_logic()
    # input("Press Enter to continue to scale labels according for the test set...")
    # scaling_logic_test_set()
    # input("Press Enter to continue to scale engineered features according to k_folds...")
    # list_of_engineered_features=[
    #     "fundamental_data_diff_self_t_1",
    #     "fundamental_data_diff_self_t_2",
    #     "fundamental_data_diff_industry_t",
    #     "fundamental_data_diff_industry_t_1",
    #     "fundamental_data_diff_industry_t_2",
    # ]
    # scaling_logic_features(list_of_engineered_features, "engineered")
    # input("Press Enter to continue to scale kpis only features according to k_folds...")
    # list_of_engineered_features = ["fundamental_data_imputed_full"]
    # scaling_logic_features(list_of_engineered_features, "kpis_only")
    # input("Press Enter to continue to scale kpis and median features according to k_folds...")
    # list_of_engineered_features = ["fundamental_data_imputed_full", "fundamental_data_avg"]
    # scaling_logic_features(list_of_engineered_features, "kpis_median")
    # input("Press Enter to continue to scale labels according for the test set...")
    # scaling_logic_test_set()
    # input("Press Enter to continue to insert adversarial samples into the corpus...")
    # add_adversarial_samples()
    # input("Press Enter to continue to add adapter data to the db...")
    # adapter_data_logic()
