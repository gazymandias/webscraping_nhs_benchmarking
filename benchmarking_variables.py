from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
from datetime import date

# new data is usually available in the third week of the month - this allows us to pull the most recent data
dynamic_month = 2 if int(date.today().strftime("%d")) < 21 else 1


# month as int MM e.g. 11 (for November)
current_month = datetime.now().strftime('%m')
# last month as int MM e.g. 10 (for October)
last_month_int = 12 if int(current_month) == 1 else int(current_month)-1
# year as int YYYY e.g. 2019
current_year_full = datetime.now().strftime('%Y')
# dynamic current year
dynamic_current_year_full = datetime.now().strftime('%Y') if current_month != '01' else int(datetime.now().strftime('%Y'))-1
# returns year as int YY e.g. 19
year_short = datetime.now().strftime('%y')
# the insert month for ED data (1 month prior to current) e.g. October (for November)
ed_period = (datetime.now() - relativedelta(months=dynamic_month)).strftime('%B')
# exactly 1 month earlier from now - full datetime format e.g. 2019-10-06 16:05:41.865752
last_month = datetime.now() - relativedelta(months=dynamic_month)
# the insert month for non-ED data (2 months prior to current) e.g. September (for November)
last_month_text = (datetime.now() - relativedelta(months=dynamic_month+1)).strftime('%B')
# shortform of the above e.g. Sep for September
last_month_text_short = (datetime.now() - relativedelta(months=dynamic_month+1)).strftime('%b')
# current time - for inserting into Perf as the Append_Date
Append_Date = datetime.now()

# the insert month for non-ED data in format MM e.g. (2 months prior to current)
other_append_month = (datetime.now() - relativedelta(months=dynamic_month+1)).strftime('%m')
# the insert month for ED data in format MM e.g. (1 month prior to current)
ed_append_month = (datetime.now() - relativedelta(months=dynamic_month)).strftime('%m')

# the last day of the insert month for non-ED data (format DD)
LastDayOther = calendar.monthrange(int(current_year_full), int(other_append_month))[1]
# the last day of the insert month for ED data (format DD)
LastDayED = calendar.monthrange(int(current_year_full), int(ed_append_month))[1]

# Gives Data_Month periods
ed_insert_year = (datetime.now() - relativedelta(months=dynamic_month)).strftime('%Y')
other_insert_year = (datetime.now() - relativedelta(months=dynamic_month+1)).strftime('%Y')
# the last day of the insert month for non-ED data (format YYYY-MM-DD - for use as Data_Month in Perf_Data)
Data_Month_Other_Append = datetime.strptime(str(LastDayOther) +
                                            str(other_append_month) + str(other_insert_year), '%d%m%Y').date()
# the last day of the insert month for ED data (format YYYY-MM-DD - for use as Data_Month in Perf_Data)
Data_Month_ED_Append = datetime.strptime(str(LastDayED) +
                                         str(ed_append_month) + str(ed_insert_year), '%d%m%Y').date()


# gives current financial year based on now() in format YYYY-YY


def financial_year():
    if current_month >= '04':
        return current_year_full + "-" + str(int(year_short) + 1)
    else:
        return str(int(current_year_full) - 1) + "-" + year_short


def ed_financial_year():
    year = int(financial_year()[:4])
    if ed_period in ('January', 'February', 'March'):
        return year + 1
    else:
        return year
