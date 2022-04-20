from collections import defaultdict


class FundamentalDataHandler:
    def __init__(self) -> None:
        self.list_of_periods_of_reports = None
        self.balance_sheets = None
        self.income_statements = None
        self.cash_flows = None

        self.company_ratios_dict = defaultdict(dict)

    def calculate_average(self, list_of_fundamental_data):
        pass

    def process_company_fundamental_data(
        self, fundamental_data_dict, current_stock_price
    ):
        self.list_of_periods_of_reports = list(
            fundamental_data_dict["balance_sheets"].keys()
        )

        self.balance_sheets = fundamental_data_dict["balance_sheets"]
        self.income_statements = fundamental_data_dict["income_statements"]
        self.cash_flows = fundamental_data_dict["cash_flows"]

        for period_key in self.list_of_periods_of_reports:
            """
                Profitability Ratios 
            """
            # Return Ratios
            self.company_ratios_dict[period_key]["roe"] = self._get_return_on_equity(period_key)
            self.company_ratios_dict[period_key]["roa"] = self._get_return_on_assets(period_key)
            self.company_ratios_dict[period_key]["roce"] = self._get_return_on_capital_employed(period_key)
            # Margin Ratios
            self.company_ratios_dict[period_key]["gross_margin"] = self._get_gross_margin_ratio(period_key)
            self.company_ratios_dict[period_key]["operating_profit_margin"] = self._get_operating_profit_margin(period_key)
            self.company_ratios_dict[period_key]["net_profit_margin"] = self._get_net_profit_margin(period_key)
            """
                Leverage Ratios
            """
            self.company_ratios_dict[period_key]["debt_to_equity"] = self._get_debt_to_equity_ratio(period_key)
            self.company_ratios_dict[period_key]["equity"] = self._get_equity_ratio(period_key)
            self.company_ratios_dict[period_key]["debt"] = self._get_debt_ratio(period_key)
            """
                Efficiency Ratios
            """
            pass
            """
                Liquidity Ratios
            """
            # Asset Ratios
            self.company_ratios_dict[period_key]["current"] = self._get_current_ratio(period_key)
            self.company_ratios_dict[period_key]["quick"] = self._get_quick_ratio(period_key)
            self.company_ratios_dict[period_key]["cash"] = self._get_cash_ratio(period_key)
            # Earnings Ratios
            self.company_ratios_dict[period_key]["times_interest_earned"] = self._get_times_interest_earned_ratio(period_key)
            # Cash Flow Ratios
            self.company_ratios_dict[period_key]["capex_to_operating_cash"] = self._get_capex_to_operating_cash_ratio(period_key)
            self.company_ratios_dict[period_key]["operating_cash_flow"] = self._get_operating_cash_flow_ratio(period_key)
            """
                Multiples Valuation Ratios
            """
            # Price Ratios
            self.company_ratios_dict[period_key]["price_to_earnings"] = self._get_price_to_earnings_ratio_2(period_key)
            # Enterprise Value Ratios
            self.company_ratios_dict[period_key]["ev_ebitda"] = self._get_ev_ebitda_ratio(period_key)
            self.company_ratios_dict[period_key]["ev_ebit"] = self._get_ev_ebit_ratio(period_key)
            self.company_ratios_dict[period_key]["ev_revenue"] = self._get_ev_revenue_ratio(period_key)



    """
        19 ratios are implemented at the moment.
    """

    """
    Profitability Ratios 
    """
    # Return Ratios
    def _get_return_on_equity(self, period):
        # ROE
        if (
            self.balance_sheets[period]["totalShareholderEquity"]
            and self.income_statements[period]["netIncome"]
        ):
            return (
                self.balance_sheets[period]["totalShareholderEquity"]
                / self.income_statements[period]["netIncome"]
            )
        return None

    def _get_return_on_assets(self, period):
        # ROA
        if (
            self.income_statements[period]["netIncome"]
            and self.balance_sheets[period]["totalAssets"]
        ):
            return (
                self.income_statements[period]["netIncome"]
                / self.balance_sheets[period]["totalAssets"]
            )
        return None

    def _get_return_on_capital_employed(self, period):
        # ROCE
        if (
            self.income_statements[period]["ebit"]
            and self.balance_sheets[period]["totalAssets"]
            and self.balance_sheets[period]["totalCurrentLiabilities"]
        ):
            return self.balance_sheets[period]["ebit"] / (
                self.balance_sheets[period]["totalAssets"]
                - self.balance_sheets[period]["totalCurrentLiabilities"]
            )
        return None

    # Margin Ratios
    def _get_gross_margin_ratio(self, period):
        # Gross Margin Ratio
        if (
            self.income_statements[period]["grossProfit"]
            and self.income_statements[period]["totalRevenue"]
        ):
            return (
                self.income_statements[period]["grossProfit"]
                / self.income_statements[period]["totalRevenue"]
            )

    def _get_operating_profit_margin(self, period):
        # Operating Profit Margin
        if (
            self.income_statements[period]["ebit"]
            and self.income_statements[period]["totalRevenue"]
        ):
            return (
                self.income_statements[period]["ebit"]
                / self.income_statements[period]["totalRevenue"]
            )

    def _get_net_profit_margin(self, period):
        # Net Profit Margin
        if (
            self.income_statements[period]["netIncome"]
            and self.income_statements[period]["totalRevenue"]
        ):
            return (
                self.income_statements[period]["netIncome"]
                / self.income_statements[period]["totalRevenue"]
            )
        return None

    """
    Leverage Ratios
    """

    def _get_debt_to_equity_ratio(self, period):
        # Debt to equity
        # NOTE: Implemented with totalLiabilities (version from financial-ratio cheat sheet)
        if (
            self.balance_sheets[period]["totalLiabilities"]
            and self.balance_sheets[period]["totalShareholderEquity"]
        ):
            return (
                self.balance_sheets[period]["totalLiabilities"]
                / self.balance_sheets[period]["totalShareholderEquity"]
            )
        return None

    def _get_equity_ratio(self, period):
        # Equity Ratio
        if (
            self.balance_sheets[period]["totalShareholderEquity"]
            and self.balance_sheets[period]["totalAssets"]
        ):
            return (
                self.balance_sheets[period]["totalShareholderEquity"]
                / self.balance_sheets[period]["totalAssets"]
            )
        return None

    def _get_debt_ratio(self, period):
        # Debt Ratio
        if (
            self.balance_sheets[period]["shortLongTermDebtTotal"]
            and self.balance_sheets[period]["totalAssets"]
        ):
            return (
                self.balance_sheets[period]["shortLongTermDebtTotal"]
                / self.balance_sheets[period]["totalAssets"]
            )
        return None

    """
    Efficiency Ratios
    """
    # NOTE: Skipping Efficiency ratios for now. They will be here if implemented
    # Accounts Receivable Turnover Ratio
    # Accounts Receivable Days
    # Asset Turnover Ratio
    # Inventory Turnover Ratio
    # Inventory Turnover Days

    """
    Liquidity Ratios
    """
    # Asset Ratios
    def _get_current_ratio(self, period):
        # Current Ratio
        if (
            self.balance_sheets[period]["totalCurrentAssets"]
            and self.balance_sheets[period]["totalCurrentLiabilities"]
        ):
            return (
                self.balance_sheets[period]["totalCurrentAssets"]
                / self.balance_sheets[period]["totalCurrentLiabilities"]
            )
        return None

    def _get_quick_ratio(self, period):
        # Quick Ratio
        if (
            self.balance_sheets[period]["cashAndShortTermInvestments"]
            and self.balance_sheets[period]["currentNetReceivables"]
            and self.balance_sheets[period]["totalCurrentLiabilities"]
        ):
            return (
                self.balance_sheets[period]["cashAndShortTermInvestments"]
                + self.balance_sheets[period]["currentNetReceivables"]
            ) / self.balance_sheets[period]["totalCurrentLiabilities"]
        return None

    def _get_cash_ratio(self, period):
        # Cash Ratio
        if (
            self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            and self.balance_sheets[period]["totalCurrentLiabilities"]
        ):
            return (
                self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
                / self.balance_sheets[period]["totalCurrentLiabilities"]
            )
        return None

    # NOTE: Defensive Interval Ratio skipped

    # Earnings Ratios
    def _get_times_interest_earned_ratio(self, period):
        # Times Interest Earned Ratio

        if (
            self.income_statements[period]["ebit"]
            and self.income_statements[period]["interestExpense"]
        ):
            return (
                self.income_statements[period]["ebit"]
                / self.income_statements[period]["interestExpense"]
            )
        return None

    # NOTE: Times Interest Earned (Cash-Basis) Ratio skipped

    # Cash Flow Ratios
    def _get_capex_to_operating_cash_ratio(self, period):
        # CAPEX (Capital Expenditures) to Operating cash Ratio

        if (
            self.cash_flows[period]["operatingCashflow"]
            and self.cash_flows[period]["capitalExpenditures"]
        ):
            return (
                self.cash_flows[period]["operatingCashflow"]
                / self.cash_flows[period]["capitalExpenditures"]
            )
        return None

    def _get_operating_cash_flow_ratio(self, period):
        # Operating Cash Flow Ratio

        if (
            self.cash_flows[period]["operatingCashflow"]
            and self.balance_sheets[period]["totalCurrentLiabilities"]
        ):
            return (
                self.cash_flows[period]["operatingCashflow"]
                / self.balance_sheets[period]["totalCurrentLiabilities"]
            )
        return None

    """
    Multiples Valuation Ratios
    """
    # Price Ratios
    # NOTE: Can also include:
    # 1. Earning Per Share, which is part of P/E
    # 2. Dividend Payout
    # 3. Dividend Yield
    # NOTE: Version from CFI cheat sheet
    def _get_price_to_earnings_ratio_1(self, period, current_stock_price):
        # Price to Earnings Ratio (P/E)

        if (
            self.income_statements[period]["netIncome"]
            and self.cash_flows[period]["dividendPayoutPreferredStock"]
            and self.balance_sheets[period]["commonStockSharesOutstanding"]
        ):
            return current_stock_price / (
                (
                    self.income_statements[period]["netIncome"]
                    - self.cash_flows[period]["dividendPayoutPreferredStock"]
                )
                / self.balance_sheets[period]["commonStockSharesOutstanding"]
            )
        return None

    # NOTE: Version from https://site.financialmodelingprep.com/developer/docs/formula
    def _get_price_to_earnings_ratio_2(self, period, current_stock_price):
        # Price to Earning Ratio (P/E)

        if (
            self.income_statements[period]["netIncome"]
            and self.balance_sheets[period]["commonStockSharesOutstanding"]
        ):
            return current_stock_price / (
                self.income_statements[period]["netIncome"]
                / self.balance_sheets[period]["commonStockSharesOutstanding"]
            )
        return None

    # Enterprise Value Ratios
    # NOTE: Check if market cap and net debt are calculated correctly
    def _get_ev_ebitda_ratio(self, period, current_stock_price):
        # Enterprise value to EBITDA

        if (
            self.balance_sheets[period]["commonStockSharesOutstanding"]
            and self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            and self.balance_sheets[period]["shortLongTermDebtTotal"]
            and self.income_statements["ebitda"]
        ):
            market_cap = (
                current_stock_price
                * self.balance_sheets[period]["commonStockSharesOutstanding"]
            )
            net_debt = (
                self.balance_sheets[period]["shortLongTermDebtTotal"]
                - self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            )
            enterprise_value = market_cap + net_debt

            return enterprise_value / self.income_statements["ebitda"]
        return None

    def _get_ev_ebit_ratio(self, period, current_stock_price):
        # Enterprise value to EBIT

        if (
            self.balance_sheets[period]["commonStockSharesOutstanding"]
            and self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            and self.balance_sheets[period]["shortLongTermDebtTotal"]
            and self.income_statements["ebit"]
        ):
            market_cap = (
                current_stock_price
                * self.balance_sheets[period]["commonStockSharesOutstanding"]
            )
            net_debt = (
                self.balance_sheets[period]["shortLongTermDebtTotal"]
                - self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            )
            enterprise_value = market_cap + net_debt

            return enterprise_value / self.income_statements["ebit"]
        return None

    def _get_ev_revenue_ratio(self, period, current_stock_price):
        # Enterprise value to Revenue

        if (
            self.balance_sheets[period]["commonStockSharesOutstanding"]
            and self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            and self.balance_sheets[period]["shortLongTermDebtTotal"]
            and self.income_statements["totalRevenue"]
        ):
            market_cap = (
                current_stock_price
                * self.balance_sheets[period]["commonStockSharesOutstanding"]
            )
            net_debt = (
                self.balance_sheets[period]["shortLongTermDebtTotal"]
                - self.balance_sheets[period]["cashAndCashEquivalentsAtCarryingValue"]
            )
            enterprise_value = market_cap + net_debt

            return enterprise_value / self.income_statements["totalRevenue"]
        return None
