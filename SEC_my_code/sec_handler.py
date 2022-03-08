
from datetime import datetime
import os
from bs4 import BeautifulSoup

from urllib.request import Request, urlopen


from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

# import requests


class SECScraper:
    def __init__(
        self,
        cik: int,
        name: str,
        year: int,
        quarter: int,
        type: str,
        filing_date: datetime,
        html_path: str,
        geckodriver_path: str = "D:/Selenium_drivers/geckodriver.exe",
    ) -> None:
        self.cik = cik
        self.name = name
        self.year = year
        self.quarter = quarter
        self.type = type
        self.filing_date = filing_date
        self.html_path = html_path
        self.geckodriver_path = geckodriver_path

        self.base_url = "https://www.sec.gov/Archives/"
        self.soup = None
        self.htm_url = None

    def create_soup(self) -> None:
        opts = Options()
        opts.headless = True
        ser = Service(self.geckodriver_path)
        browser = Firefox(options=opts, service=ser)

        # find another way to detect error code
        if "errorPageContainer" in [
            elem.get_attribute("id")
            for elem in browser.find_elements_by_css_selector("body > div")
        ]:
            print("Error")
            return
        else:
            browser.get(os.path.join(self.base_url, self.html_path))

        self.soup = BeautifulSoup(browser.page_source, "html.parser")

    def create_htm_url(self) -> None:
        self.create_soup()
        filing_url = self.extract_filing_url()

        self.htm_url = os.path.join(
            self.base_url,
            self.html_path.replace("index.html", "").replace("-", "")
            + "/"
            + filing_url,
        )

    def extract_date_info(self) -> dict:
        list_of_forms = self.soup.find_all("div", class_="formGrouping")

        dict_of_info = dict()

        for form in list_of_forms:
            curr_key = None
            curr_value = None
            for possible_div in form.contents:
                if possible_div == "\n":
                    continue

                text_of_div = possible_div.text
                if not self.is_num_in_string(text_of_div):
                    curr_key = text_of_div
                else:
                    curr_value = text_of_div

                if curr_key and curr_value:
                    dict_of_info[curr_key] = curr_value
                    curr_key = None
                    curr_value = None

        return dict_of_info

    def is_num_in_string(self, s):
        return any(i.isdigit() for i in s)

    def extract_filing_url(self) -> str:
        table = (
            self.soup.find("table", {"class": "tableFile"}).find("tbody").find_all("tr")
        )
        for row in table:
            cells = row.find_all("td")
            if cells:
                for idx, cell_row in enumerate(cells):
                    if cell_row.get_text() == self.type:
                        return cells[idx + 1].get_text().split()[0]

    def get_htm_url(self):
        return self.htm_url

    def get_date_info(self):
        return self.get_date_info

sec_handler = SECScraper(
    1318605,
    "Tesla, Inc.",
    2020,
    1,
    "10-K",
    datetime(2020, 2, 13),
    "edgar/data/1318605/0001564590-20-004475-index.html",
)
