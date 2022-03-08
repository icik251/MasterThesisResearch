import json
from sec_api import ExtractorApi

extractorApi = ExtractorApi(
    "0ed3ffd8daf8372a1162a4b5b45ff7ba576248459fd8f1cea6d42b1ab52ca12a"
)

# Tesla 10-K filing
# filing_url = "https://www.sec.gov/Archives/edgar/data/1318605/000156459020004475\tsla-10k_20191231.htm"
# filing_prior_2003 = "https://www.sec.gov/Archives/edgar/data/1063921/000101287000000736/0001012870-00-000736.txt"
# get the standardized and cleaned text of section Management and Analysis"
# section_text = extractorApi.get_section(filing_prior_2003, "2", "text")

from sec_api import QueryApi

"""
This info is in "occ.py and searched link is: https://www.sec.gov/Archives/edgar/data/1000230/000143774918016838/occ20180731_10q.htm"
"""
# queryApi = QueryApi("0ed3ffd8daf8372a1162a4b5b45ff7ba576248459fd8f1cea6d42b1ab52ca12a")
# # OPTICAL CABLE CORP, ticker: OCC
# query = {
#   "query": { "query_string": {
#       "query": "ticker:OCC AND filedAt:{2018-01-01 TO 2018-12-31} AND formType:\"10-Q\""
#     } },
#   "sort": [{ "filedAt": { "order": "desc" } }]
# }

# filings = queryApi.get_filings(query)

# print(filings)

# OCC 10-Q
filing_url = "https://www.sec.gov/Archives/edgar/data/2178/000000217808000011/0000002178-08-000011.txt"
# get the standardized and cleaned text of section Management and Analysis"
# mda_2 = extractorApi.get_section(filing_url, "2", "text")
# mda_7 = extractorApi.get_section(filing_url, "7", "text")
# risk_21a = extractorApi.get_section(filing_url, "21A", "text")
# risk_1a = extractorApi.get_section(filing_url, "1A", "text")

# print(mda_7)
# print(mda_2)
# print(risk_21a)
# print(risk_1a)

from sec_api import XbrlApi

xbrlApi = XbrlApi("2a0063a7d9ff5548e6d1e3c4ef8332d6d53d273c60ee7d02301fad7e6b710b5e")
filing_url_nicholas = "https://www.sec.gov/Archives/edgar/data/1000045/000156459020004703/nick-10q_20191231.htm"
filing_url_no_balance_sheets = (
    "https://www.sec.gov/Archives/edgar/data/1000278/000114420411013302/v213714_10k.htm"
)
# 10-K HTM File URL example
xbrl_json = xbrlApi.xbrl_to_json(htm_url=filing_url)
print(xbrl_json['status'])
balance_sheets = xbrl_json["BalanceSheets"]
statements_of_cash_flows = xbrl_json["StatementsOfCashFlows"]
statements_of_income = xbrl_json["StatementsOfIncome"]
statements_of_shareholders_equity = xbrl_json["StatementsOfShareholdersEquity"]

with open("SEC_my_code/output/valid_filing_url_balance_sheets.json", "w") as outfile:
    json.dump(balance_sheets, outfile)

with open("SEC_my_code/output/valid_filing_url_statements_of_cash_flows.json", "w") as outfile:
    json.dump(statements_of_cash_flows, outfile)

with open("SEC_my_code/output/valid_filing_url_statements_of_income.json", "w") as outfile:
    json.dump(statements_of_income, outfile)

with open(
    "SEC_my_code/output/valid_filing_url_statements_of_shareholders_equity.json", "w"
) as outfile:
    json.dump(statements_of_shareholders_equity, outfile)
