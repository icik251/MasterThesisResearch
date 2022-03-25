import time
from company_handler import CompanyHandler
from time_series_handler import TimeSeriesHandler


def time_series_logic(url):
    # Adding time series for companies

    timeseries_handler = TimeSeriesHandler(url)
    timeseries_handler.add_timeseries()

def companies_logic(req_per_min=300):
    # Adding companies and fix duplicated ones

    company_handler = CompanyHandler(
        path_companies_to_extract="APIClient/data/nasdaq/energy_all.csv",
    )

    list_of_companies = []
    for company in company_handler.load_chosen_companies_from_db():
        list_of_companies.append(company)

    company_handler.add_companies(
        list_of_companies, req_per_min=req_per_min, from_year=2017
    )
    
    # becuase they are sent to the queue and not yet processed
    time.sleep(90)
    company_handler.validate_filings_type()
    res = company_handler.fix_duplicate_companies()
    for company in res:
        print(company["cik"])
        print(company["year"])
        print("---------")


if __name__ == "__main__":
    companies_logic(req_per_min=100)

    time_series_logic(url="http://localhost:8000/api/v1/stock_price/")
    time.sleep(180)
    time_series_logic(url="http://localhost:8000/api/v1/stock_price/inflation/")

    pass
