import bs4
import re
import pandas as pd
import numpy as np
import httplib2
from benchmarking_variables import *
from etl_functions import clean_data, reset_index
import time

start_time = time.time()
username = SubmittedBy = 'gcadman'
# Show all columns in the dataframe
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 100)
# Establishes 'the http.' prefix as a web connector.
http = httplib2.Http()

# keys - these are the websites where the download links are located
rtt = f"https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-{financial_year()}"
cancerwt = f"https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/monthly-prov-cwt/" \
    f"{financial_year()}-monthly-provider-cancer-waiting-times-statistics/provider-based-cancer-waiting-times-for-" \
    f"{last_month_text}-{financial_year()}-provisional/"
edwta = "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/" \
    f"ae-attendances-and-emergency-admissions-{financial_year()}/"

# vals - these are the specific download links that we search for on the keys websites
admitted = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{dynamic_current_year_full}/" \
    f"{last_month_int}/Admitted-Provider-{last_month_text_short}"
nonadmitted = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{dynamic_current_year_full}/" \
    f"{last_month_int}/NonAdmitted-Provider-{last_month_text_short}"
incomplete = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{dynamic_current_year_full}/" \
    f"{last_month_int}/Incomplete-Provider-{last_month_text_short}"
cancer = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{financial_year()[:4]}/" \
    f"{last_month_int}/{last_month_text.upper()}"
# ed = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{current_year_full}/" \
#      f"{current_month}/{ed_period}-AE"
# the NHS is routinely inconsistent with naming conventions so multiple variations are required
edold = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{current_year_full}/" \
    f"{current_month}/{ed_period}-{ed_financial_year()}-monthly"
ed = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{dynamic_current_year_full}/" \
    f"{last_month_int}/{ed_period}-{ed_financial_year()}-AE"
edalt = f"^https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{dynamic_current_year_full}/" \
    f"{last_month_int}/{ed_period}-{ed_financial_year()}-by"

# dictionary - holds each key (major website for RTT, Cancer and EDWT) with their associate download links (vals)
links = {rtt: [admitted, nonadmitted, incomplete], cancerwt: [cancer], edwta: [ed, edold, edalt]}
# dictionary - for each download link we pull the data for multiple indicators ("reports")
reports = {admitted: ["18AdmBench", "ZeroRTTAPBench"],
           nonadmitted: ["18NonAdmBench", "ZeroRTTNPBench"],
           incomplete: ["18IncompBench", "ZeroRTTIPBench"],
           cancer: ["CancerUrgBench", "CanNatScr0Bench", "CancerAll0Bench", "CanSurg0Bench", "Cancanti0Bench",
                    "CancerRad0Bench", "CancUrgF0Bench", "CancBreastBench"],
           ed: ["AESitrep4Bench", "AEAttendBench"],
           edold: ["AESitrep4Bench", "AEAttendBench"],
           edalt: ["AESitrep4Bench", "AEAttendBench"], }
# as each download is different, we pass the param_select values when converting data from csv to frame
param_select = {"18AdmBench": {"sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                               "header": 0},
                "ZeroRTTAPBench": {"sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                   "header": 0},
                "18NonAdmBench": {"sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                  "header": 0},
                "ZeroRTTNPBench": {"sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                   "header": 0},
                "18IncompBench": {"sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                  "header": 0},
                "ZeroRTTIPBench": {"sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                   "header": 0},
                "CancerUrgBench": {"sheet_name": '62-DAY (ALL CANCER)', "skiprows": 9,
                                   "index_col": 0,
                                   "header": 0},
                "CanNatScr0Bench": {"sheet_name": '62-DAY (SCREENING)', "skiprows": 8,
                                    "index_col": 0, "header": 0},
                "CancerAll0Bench": {"sheet_name": '31-DAY FIRST TREAT (ALL CANCER)',
                                    "skiprows": 8, "index_col": 0, "header": 0},
                "CanSurg0Bench": {"sheet_name": '31-DAY SUB TREAT (SURGERY)', "skiprows": 8,
                                  "index_col": 0, "header": 0},
                "Cancanti0Bench": {"sheet_name": '31-DAY SUB TREAT (DRUGS)', "skiprows": 8,
                                   "index_col": 0, "header": 0},
                "CancerRad0Bench": {"sheet_name": '31-DAY SUB TREAT (RADIOTHERAPY)',
                                    "skiprows": 8, "index_col": 0, "header": 0},
                "CancUrgF0Bench": {"sheet_name": 'TWO WEEK WAIT-ALL CANCER', "skiprows": 6,
                                   "index_col": 0, "header": 0},
                "CancBreastBench": {"sheet_name": 'TWO WEEK WAIT-BREAST SYMPTOMS',
                                    "skiprows": 6, "index_col": 0, "header": 0},
                "AESitrep4Bench": {"sheet_name": 'Provider Level Data', "skiprows": 15,
                                   "index_col": 0, "header": 0},
                "AEAttendBench": {"sheet_name": 'Provider Level Data', "skiprows": 15,
                                  "index_col": 0, "header": 0},
                }
# this is a blank dataframe with preset columns which are required for the load into SQL
IPFBenchForSQL = pd.DataFrame(columns=['Append_Date', 'Indicator_ID', 'Data_Month', 'Section_Code',
                                       'Grouping_2', 'Grouping_3', 'User_Name', 'Submitted_By', 'Numerator',
                                       'Denominator', 'Numeric_Value'])


def download_bench_data(master):
    # for each webpage (key) in links (the pages for RTT, Cancer and ED)
    for k in links:
        print(k)
        # for each dynamic download link (x) in the webpages
        for x in links[k]:
            print(f"Looking for data here: {x}")
            # establish the connection to the webpage (k)
            status, response = http.request(k)
            # parse all of the html links in the background looking for href urls looking for a match to x
            for link in bs4.BeautifulSoup(response, 'html.parser', parse_only=bs4.SoupStrainer('a', href=True)) \
                    .find_all(attrs={'href': re.compile(x)}):
                # for each Indicator_ID in the reports dictionary use pandas to read the data using appropriate params
                for y in reports[x]:
                    print("FOUND: " + link.get('href') + " (" + y + ")")
                    data = pd.read_excel(link.get('href'), **param_select.get(y))
                    # custom switch is a dynamic variable that is either 1 or 2 months previous from current month
                    if y in ("AESitrep4Bench", "AEAttendBench"):
                        custom_switch = Data_Month_ED_Append
                    else:
                        custom_switch = Data_Month_Other_Append
                    # create new columns with dynamic data
                    new_columns = {'Append_Date': Append_Date, 'Indicator_ID': y, 'User_Name': username,
                                   'Submitted_By': SubmittedBy, 'Section_Code': 0,
                                   'Data_Month': custom_switch}
                    # for each column above, create the column in the dataframe - get values from the dictionary
                    for i in new_columns:
                        data[i] = new_columns.get(i)
                    # pass the defined functions to clean the data and then reset the index if required before appending
                    # to the empty master dataframe
                    data = clean_data(data, y)
                    data = reset_index(data, y)
                    master = master.append(data, ignore_index=True, sort=False)
    return master


IPFBenchForSQL = download_bench_data(IPFBenchForSQL)
print(IPFBenchForSQL)
print(f"Program took {time.time() - start_time} to run")
