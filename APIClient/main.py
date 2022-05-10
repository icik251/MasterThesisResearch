import time
from company_handler import CompanyHandler
from time_series_handler import TimeSeriesHandler
from input_data_handler import InputDataHandler
from fundamental_data_handler import FundamentalDataHandler


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


def scaling_logic():
    input_data_obj = InputDataHandler()
    input_data_obj.scaling_labels()


def scaling_logic_test_set():
    input_data_obj = InputDataHandler()
    input_data_obj.scaling_labels_test_set()


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
    #     "Press Enter to continue to set is_used_to false for all companies lacking features from the engineered ones"
    # )
    # set_is_used_input_data(lacking_features_is_used=True)
    # input(
    #     "Press Enter to continue to set is_used_to false for the outliers with the percentage change"
    # )
    # list_of_ids_to_remove = [
    #     "62795156199c6e379f8994a8",
    #     "62795170c02560533c8994a8",
    #     "627951862e41d174508994a8",
    #     "6279518dcd1aecfde58994a8",
    #     "6279518f6759f8f7cb8994a8",
    #     "627951a321155cfd1b8994a8",
    #     "627951a624c07acf3f8994a8",
    # ]
    # set_is_used_input_data(list_of_ids_to_remove=list_of_ids_to_remove, outlier_target_is_used=True)
    # input("Press Enter to continue to create k-folds...")
    # create_k_folds_logic(k_folds=5)
    input("Press Enter to continue to scale labels according to k_folds...")
    scaling_logic()
    input("Press Enter to continue to scale labels according for the test set...")
    scaling_logic_test_set()

    pass
