# This file is for testing purposes on https://site.financialmodelingprep.com/

import json
import requests

# Financial Statements - both for 10-Q and 10-K
"""
Company Income Statement
Can get 10-K and 10-Q info with the paid subscription
"""
ticker = "BKEP"
limit = 5
api_key = "2bb377afbdedaa96c62af5852ea6fdd5"
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit={limit}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(f"FinancialAPIsTesting/output/income_statements_{ticker}.json", "w") as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/income-statement/')

# """
# Balance Sheet Statement
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?limit={limit}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/balance-sheet-statement_{ticker}.json", "w"
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/balance-sheet-statement/')

# """
# Cash Flow Statement
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit={limit}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/cash-flow-statement_{ticker}.json", "w"
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/cash-flow-statement/')


# # Financial Statements AS REPORTED - both for 10-Q and 10-K. If something cant be found in the past three
# # endpoints, it might be here.
# """
# Cash Flow Statement as reported
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/cash-flow-statement-as-reported/{ticker}?limit={limit}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/cash-flow-statement_as_reported_{ticker}.json",
#         "w",
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/cash-flow-statement-as-reported/')

# """
# Full Financial Statement as reported
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/financial-statement-full-as-reported/{ticker}?limit={limit}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/full-financial-statement_as_reported_{ticker}.json",
#         "w",
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/full-financial-statement-as-reported')

# """
# Quarterly Earnings Reports
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v4/financial-reports-json?symbol={ticker}&period=Q1&year2009&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/financial_reports_json_{ticker}.json", "w"
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v4/financial-reports-json')

# """
# Shares Float
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v4/shares_float?symbol={ticker}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(f"FinancialAPIsTesting/output/shares_float_json_{ticker}.json", "w") as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v4/shares_float')

# """
# Earning Call Transcript
# """
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/symbol={ticker}&year=2009&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/batch_earning_call_transcript_year_2000_{ticker}.json",
#         "w",
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v4/batch_earning_call_transcript')

# """
# Company Financial Ratios
# """
# # TTM = TTM figures can also be used to calculate financial ratios.
# # The price/earnings ratio is often referred to as P/E (TTM) and is
# # calculated as the stock's current price, divided by a company's trailing 12-month earnings per share (EPS)
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/ratios_ttm_{ticker}.json",
#         "w",
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/ratios-ttm/')

# # Not TTM
# resp = requests.get(
#     f"https://financialmodelingprep.com/api/v3/ratios/{ticker}&apikey={api_key}"
# )
# if resp.status_code == 200:
#     data = json.loads(resp.text)
#     with open(
#         f"FinancialAPIsTesting/output/ratios_{ticker}.json",
#         "w",
#     ) as f:
#         json.dump(data, f)
# else:
#     print(resp.status_code)
#     print('https://financialmodelingprep.com/api/v3/ratios/')


# Key Metrics
resp = requests.get(
    f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}&apikey={api_key}"
)
if resp.status_code == 200:
    data = json.loads(resp.text)
    with open(
        f"FinancialAPIsTesting/output/key_metrics_{ticker}.json",
        "w",
    ) as f:
        json.dump(data, f)
else:
    print(resp.status_code)
    print("https://financialmodelingprep.com/api/v3/key_metrics/")
