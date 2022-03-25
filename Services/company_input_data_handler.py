from datetime import datetime
import pandas as pd
import json
import requests


class CompanyInputDataHandler:
    def __init__(self, cik, df_filings_deadlines) -> None:
        self.cik = cik
        self.df_filings_deadlines = df_filings_deadlines
        self.list_of_input_company = []

    def _is_filing_on_time(self, filing_type, quarter, company_type, filing_date):
        # check if company_type was modified
        res_type = company_type.split(";")
        company_type = res_type[1] if len(res_type) > 1 else res_type[0]

        filing_date_datetime = datetime.fromisoformat(filing_date)
        try:
            deadline_str = self.df_filings_deadlines.query(
                f'filing_type=="{filing_type[:3]}-{quarter}"'
            )[f"{company_type}"].values[0]
        except Exception as e:
            print(e)
            return None

        deadline_str = deadline_str.replace("year", str(filing_date_datetime.year))
        deadline_datetime = datetime.strptime(deadline_str, "%Y-%d-%m")
        # print("Filing_date", filing_date_datetime)
        # print("Deadline_date", deadline_datetime)
        if filing_date_datetime > deadline_datetime:
            return False
        return True

    def _get_price_for_filing_date(self, stock_prices_list, filing_date):
        for idx, item in enumerate(stock_prices_list):
            if item["timestamp"] == filing_date:
                return stock_prices_list[idx], stock_prices_list[idx + 1]

    def init_prepare_data(self):
        resp = json.loads(
            requests.get(f"http://localhost:8000/api/v1/company/{self.cik}").text
        )
        resp_stock_prices = json.loads(
            requests.get(
                f"http://localhost:8000/api/v1/stock_price/inflation/{self.cik}"
            ).text
        )
        if resp["code"] == 200:
            curr_company_list = resp["data"]
        else:
            print(resp["message"])
        if resp_stock_prices["code"] == 200:
            curr_stock_prices_list = resp_stock_prices["data"]
        else:
            print(resp_stock_prices["message"])

        for company_year_dict in curr_company_list:
            for quarter in company_year_dict["quarters"]:
                for metadata in quarter["metadata"]:
                    curr_input_data = {}
                    t_obj, t_1_obj = self._get_price_for_filing_date(
                        curr_stock_prices_list, metadata["filing_date"]
                    )
                    filing_on_time = self._is_filing_on_time(
                        metadata["type"],
                        quarter["q"],
                        metadata["company_type"],
                        metadata["filing_date"],
                    )
                    curr_input_data["cik"] = company_year_dict["cik"]
                    curr_input_data["year"] = company_year_dict["year"]
                    curr_input_data["type"] = metadata["type"]
                    curr_input_data["q"] = quarter["q"]
                    curr_input_data["mda_section"] = metadata["mda_section"]
                    curr_input_data["risk_section"] = metadata["risk_section"]
                    curr_input_data["company_type"] = metadata["company_type"]
                    curr_input_data["filing_date"] = metadata["filing_date"]
                    curr_input_data["period_of_report"] = metadata["period_of_report"]
                    curr_input_data["is_filing_on_time"] = filing_on_time
                    curr_input_data["close_filing_date"] = t_obj["adjusted_close"]
                    curr_input_data["volume_filing_date"] = t_obj["volume"]
                    curr_input_data["close_next_day_filing_date"] = t_1_obj[
                        "adjusted_close"
                    ]
                    curr_input_data["volume_next_day_filing_date"] = t_1_obj["volume"]
                    self.list_of_input_company.append(curr_input_data)

    def get_percentage_diff(self, first, second):
        # try:
        #     percentage = abs(current - next) / max(current, next) * 100
        # except ZeroDivisionError:
        #     percentage = float('inf')
        # return percentage
        diff = second - first
        change = 0
        try:
            if diff > 0:
                change = (diff / first) * 100
            elif diff < 0:
                diff = first - second
                change = -((diff / first) * 100)
        except ZeroDivisionError:
            return float("inf")
        return change

    def create_labels(self, perc_treshold=5):
        for idx, curr_sample_input in enumerate(self.list_of_input_company):
            if idx == len(self.list_of_input_company) - 1:
                curr_sample_input["label"] = None
                curr_sample_input["percentage_change"] = None
                break

            next_sample_input = self.list_of_input_company[idx + 1]

            perc_change = self.get_percentage_diff(
                curr_sample_input["close_next_day_filing_date"],
                next_sample_input["close_next_day_filing_date"],
            )
            if abs(perc_change) < perc_treshold:
                curr_sample_input["label"] = "hold"
            elif perc_change >= perc_treshold:
                curr_sample_input["label"] = "buy"
            else:
                curr_sample_input["label"] = "sell"

            curr_sample_input["percentage_change"] = perc_change

    def logic(self):
        self.init_prepare_data()
        self.create_labels()


df_filings_deadlines = pd.read_csv("Services/data/filing_deadlines.csv")
company_input_data_handler_obj = CompanyInputDataHandler(2178, df_filings_deadlines)
company_input_data_handler_obj.logic()

for sample_input in company_input_data_handler_obj.list_of_input_company:
    print(f"Filing date: {sample_input['filing_date']}")
    print(f"Period of report: {sample_input['period_of_report']}")
    print(f"Year: {sample_input['year']} | Q: {sample_input['q']}")
    print(f"Type of report: {sample_input['type']}")
    print(f"Is Filing on time: {sample_input['is_filing_on_time']}")
    print(f"Class label: {sample_input['label']}")
    print(f"Percentage change: {sample_input['percentage_change']}")
    print("----------------")
