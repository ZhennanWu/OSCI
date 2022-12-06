import datetime
from multiprocessing import Pool
from osci.actions.process.generate_daily_osci_rankings import DailyOSCIRankingsAction
from osci.filter.filter_unlicensed import filter_out_unlicensed

from osci.actions.preprocess.match_company_all import process_github_daily_all_events
from osci.actions.process.repository_composition import RepositoryCompositionJob
import calendar

start_date = datetime.datetime(2020, 1, 1)
end_date = datetime.datetime(2022, 1, 1)
delta = datetime.timedelta(days=1)
day = start_date
days = (end_date - start_date).days


def func(day):
    process_github_daily_all_events(day=day)
    print(day)

list(map(func, (start_date + i * delta for i in range(days))))
# for i in range(days):
#     func(start_date + i * delta)
# with Pool(8) as p:
#     p.map(func, (start_date + i * delta for i in range(days)), 1)


# for year in [2020]:
#     for month in range(7, 13):
#         last_day_in_month = datetime.datetime(
#             year, month, calendar.monthrange(year, month)[1])
#         DailyOSCIRankingsAction._execute(None, last_day_in_month)
#         RepositoryCompositionJob(date_period_type="MTD").run(
#             to_date=last_day_in_month)
