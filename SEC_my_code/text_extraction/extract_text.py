import copy
import json
from msilib import sequence
import os
import re

from html_document import HtmlDocument
from text_document import TextDocument
from metadata import Metadata

from utils import requests_get
from utils import search_terms as master_search_terms


def download_filing(index_url: str, company_description: int):
    """
    Download filing, extract relevant sections.

    Download a filing (full filing submission). Find relevant <DOCUMENT>
    portions of the filing, and send the raw text for text extraction
    :param: doc_info: contains URL for the full filing submission, and
    other EDGAR index metadata
    """

    filing_metadata = Metadata(index_url)

    # check for the date
    # if re.search(date_search_string, str(filing_metadata.sec_period_of_report)):
    filing_metadata.sec_index_url = index_url
    filing_metadata.sec_url = re.sub("-index.htm.?", "", index_url) + ".txt"
    filing_metadata.company_description = company_description

    filing_url = filing_metadata.sec_url
    company_description = filing_metadata.company_description
    # log_str = "Retrieving: %s, %s, period: %s, index page: %s" % (
    #     filing_metadata.sec_company_name,
    #     filing_metadata.sec_form_header,
    #     filing_metadata.sec_period_of_report,
    #     filing_metadata.sec_index_url,
    # )
    # log_cache.append(("DEBUG", log_str))

    r = requests_get(filing_url)
    filing_text = r.text
    filing_metadata.add_data_from_filing_text(filing_text[0:10000])

    # Iterate through the DOCUMENT types that we are seeking,
    # checking for each in turn whether they are included in the current
    # filing. Note that searching for document_group '10-K' will also
    # deliberately find DOCUMENT type variants such as 10-K/A, 10-K405 etc.
    # Note we search for all DOCUMENT types that interest us, regardless of
    # whether the current filing came from a '10-K' or '10-Q' web query
    # originally. Also note that we process DOCUMENT types in no
    # fixed order.
    filtered_search_terms = {
        doc_type: master_search_terms[doc_type] for doc_type in ["10-K", "10-Q"]
    }
    for document_group in filtered_search_terms:
        doc_search = re.search(
            "<DOCUMENT>.{,20}<TYPE>" + document_group + ".*?</DOCUMENT>",
            filing_text,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if doc_search:
            doc_text = doc_search.group()
            doc_metadata = copy.copy(filing_metadata)
            # look for form type near the start of the document.
            type_search = re.search("(?i)<TYPE>.*", doc_text[0:10000])
            if type_search:
                document_type = re.sub("^(?i)<TYPE>", "", type_search.group())
                document_type = re.sub(
                    r"(-|/|\.)", "", document_type
                )  # remove hyphens etc
            else:
                document_type = "document_TYPE_not_tagged"
                # log_cache.append(
                #     ("ERROR", "form <TYPE> not given in form?: " + filing_url)
                # )

            doc_metadata.document_type = document_type
            doc_metadata.document_group = document_group

            current_match = None
            if doc_text:
                match_str = (
                    doc_text.lower()
                    .replace("<", " ")
                    .replace(">", " ")
                    .replace(";", " ")
                    .replace("&", " ")
                    .replace("_", " ")
                    .replace("[", " ")
                    .replace("]", " ")
                    .replace("nbsp", " ")
                    .replace("#160", " ")
                )
                list_of_symbols = [
                    "#9746",
                    "#9745",
                    "#x2611",
                    "#x2612",
                    "#8999",
                    "#254",
                    "#253",
                    "#x2713",
                    "#10003",
                    "q",
                    "x",
                ]
                non_numeric_symbol = "/ix:nonnumeric"
                list_to_end_seq = ["#9744"]
                list_of_splitted_match = match_str.lower().split()
                in_sequence = False
                with open("testing_extraction.json", "w") as f:
                    json.dump(list_of_splitted_match, f)

                last_substring_match = None
                curr_nonnumeric_match = False
                for idx, substring in enumerate(list_of_splitted_match):
                    if in_sequence and substring in list_to_end_seq:
                        in_sequence = False
                        curr_nonnumeric_match = False
                        current_match = None
                        

                    if in_sequence and substring in list_of_symbols:
                        break
                    
                    elif in_sequence and substring == non_numeric_symbol:
                        curr_nonnumeric_match = True

                    if (
                        substring == "accelerated"
                        and list_of_splitted_match[idx - 1] == "large"
                    ):  
                        # if there is already a valid match when we go to the next type
                        if curr_nonnumeric_match:
                            break
                        in_sequence = True
                        current_match = "large_accelerated_filer"
                        last_substring_match = substring
                    elif substring == "accelerated" and last_substring_match != "non":
                        # if there is already a valid match when we go to the next type
                        if curr_nonnumeric_match:
                            break
                        in_sequence = True
                        current_match = "accelerated_filer"
                        last_substring_match = substring
                    elif substring == "non-accelerated":
                        # if there is already a valid match when we go to the next type
                        if curr_nonnumeric_match:
                            break
                        in_sequence = True
                        current_match = "non_accelerated_filer"
                        last_substring_match = substring
                    elif substring == "non":
                        # if there is already a valid match when we go to the next type
                        if curr_nonnumeric_match:
                            break
                        in_sequence = True
                        current_match = "non_accelerated_filer"
                        last_substring_match = substring
                    elif (
                        substring == "smaller"
                        and list_of_splitted_match[idx + 1] == "reporting"
                    ):
                        # if there is already a valid match when we go to the next type
                        if curr_nonnumeric_match:
                            break
                        in_sequence = True
                        current_match = "smaller_reporting_company"
                        last_substring_match = substring

            print(current_match)

            # # search for a <html>...</html> block in the DOCUMENT
            # html_search = re.search(r"<(?i)html>.*?</(?i)html>", doc_text, re.DOTALL)
            # xbrl_search = re.search(r"<(?i)xbrl>.*?</(?i)xbrl>", doc_text, re.DOTALL)
            # # occasionally a (somewhat corrupted) filing includes a mixture
            # # of HTML-format documents, but some of them are enclosed in
            # # <TEXT>...</TEXT> tags and others in <HTML>...</HTML> tags.
            # # If the first <TEXT>-enclosed document is before the first
            # # <HTML> enclosed one, then we take that one instead of
            # # the block identified in html_search.
            # text_search = re.search(r"<(?i)text>.*?</(?i)text>", doc_text, re.DOTALL)
            # if (
            #     text_search
            #     and html_search
            #     and text_search.start() < html_search.start()
            #     and html_search.start() > 5000
            # ):
            #     html_search = text_search
            # if xbrl_search:
            #     doc_metadata.extraction_method = "xbrl"
            #     doc_text = xbrl_search.group()
            #     # main_path = local_path + ".xbrl"
            #     reader_class = HtmlDocument
            # elif html_search:
            #     # if there's an html block inside the DOCUMENT then just
            #     # take this instead of the full DOCUMENT text
            #     doc_metadata.extraction_method = "html"
            #     doc_text = html_search.group()
            #     # main_path = local_path + ".htm"
            #     reader_class = HtmlDocument
            # else:
            #     doc_metadata.extraction_method = "txt"
            #     # main_path = local_path + ".txt"
            #     reader_class = TextDocument

            # doc_metadata.original_file_size = str(len(doc_text)) + " chars"
            # reader_class(
            #     doc_metadata.original_file_name,
            #     doc_text,
            #     doc_metadata.extraction_method,
            # ).get_excerpt(
            #     doc_text, document_group, doc_metadata, skip_existing_excerpts=False
            # )

            # print("done")


download_filing(
    index_url="https://www.sec.gov/Archives/edgar/data/894627/0000894627-21-000024-index.html",
    company_description=None,
)
