from company_handler import CompanyHandler
from time_series_handler import TimeSeriesHandler


def time_series_logic():
    # Adding time series for companies

    timeseries_handler = TimeSeriesHandler("APIClient/data/nasdaq/energy_all.csv")
    timeseries_handler.add_timeseries()


def companies_logic(req_per_min=300):
    # Adding companies and fix duplicated ones

    company_handler = CompanyHandler(
        path_companies_to_extract="APIClient/data/nasdaq/energy_all.csv",
    )

    list_of_companies = []
    for company in company_handler.load_chosen_companies_from_db():
        list_of_companies.append(company)

    company_handler.add_companies(list_of_companies, req_per_min=req_per_min)

    res = company_handler.fix_duplicate_companies()
    for company in res:
        print(company["data"][0]["cik"])
        print(company["data"][0]["year"])
        print("---------")


if __name__ == "__main__":
    companies_logic()

    # time_series_logic()

    pass
