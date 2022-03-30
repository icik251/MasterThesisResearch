from collections import defaultdict
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

    def separate_paragraphs(self):
        for item in self.list_of_input_company:
            try:
                mda_section_splitted = item["mda_section"].split("\n")
                dict_of_paragraphs_mda = self.reformat_section(mda_section_splitted)
                item["mda_paragraphs"] = dict_of_paragraphs_mda
            except Exception as e:
                print(e)

            try:
                risk_section_splitted = item["risk_section"].split("\n")
                dict_of_paragraphs_risk = self.reformat_section(risk_section_splitted)
                item["risk_paragraphs"] = dict_of_paragraphs_risk

            # Testing
            # with open(
            #     f'Services/data/paragraphs/mda_paragraphs_json_{item["year"]}_{item["type"]}_{item["cik"]}.json',
            #     "w",
            #     encoding="utf-8",
            # ) as f:
            #     json.dump(dict_of_paragraphs_mda, f, ensure_ascii=False)

            # with open(
            #     f'Services/data/paragraphs/risk_paragraphs_json_{item["year"]}_{item["type"]}_{item["cik"]}.json',
            #     "w",
            #     encoding="utf-8",
            # ) as f:
            #     json.dump(dict_of_paragraphs_risk, f, ensure_ascii=False)
            except Exception as e:
                print(e)

    def reformat_section(self, section_splitted, threshold_for_paragraph=20):
        """
        Things we remove:
        - string ending on ':' (because a table follows),
        - [DATA_TABLE_REMOVED],
        - MDA and QQD section names

        """
        list_of_after_table_numbers = [
            "(1)",
            "(2)",
            "(3)",
            "(4)",
            "(5)",
            "(6)",
            "(7)",
            "(8)",
            "(9)",
        ]
        dict_of_paragraphs = defaultdict(str)
        curr_paragraph_key = None
        paragraph_title = False
        len_section_spitted = len(section_splitted)
        for idx in range(len_section_spitted):
            section_splitted[idx] = section_splitted[idx].strip()
            # print(section_splitted[idx])
            # captures what we want to skip
            if (
                len(section_splitted[idx]) <= 3
                or section_splitted[idx].endswith(":")
                or section_splitted[idx] == "[DATA_TABLE_REMOVED]"
                or "_" in section_splitted[idx]
                or idx == 0
                or idx == len_section_spitted - 1
                or section_splitted[idx].split()[0] in list_of_after_table_numbers
            ):
                continue
            elif len(section_splitted[idx].split()) <= threshold_for_paragraph:
                if paragraph_title:
                    curr_paragraph_key += "|" + section_splitted[idx]
                else:
                    curr_paragraph_key = section_splitted[idx]
                paragraph_title = True
            else:
                dict_of_paragraphs[curr_paragraph_key] += (section_splitted[idx]) + "\n"
                paragraph_title = False

        return dict_of_paragraphs

    def logic(self):
        self.init_prepare_data()
        self.create_labels()
        self.separate_paragraphs()

        # Testing purposes
        # Remove last element from list as it is without label
        self.list_of_input_company.pop()
        with open(
            f"Services/data/sample_inputs/sample_input_{self.cik}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(self.list_of_input_company, f, ensure_ascii=False)


df_filings_deadlines = pd.read_csv("Services/data/filing_deadlines.csv")
company_input_data_handler_obj = CompanyInputDataHandler(2178, df_filings_deadlines)
company_input_data_handler_obj.logic()
