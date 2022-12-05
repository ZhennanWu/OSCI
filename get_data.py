
import datetime
from osci.actions.load.load_all_events import get_github_daily_all_events

from osci.crawlers.github.gharchive import get_github_daily_push_events

from multiprocessing import Pool


start_date = datetime.datetime(2020,7,1)
end_date = datetime.datetime(2021,1,1)
delta = datetime.timedelta(days=1)
day = start_date
days = (end_date - start_date).days
def func(day):
    get_github_daily_all_events(day=day)
    print(day)
with Pool(8) as p:
    p.map(func, (start_date + i* delta for i in range(days)), 1)
# while start_date <= end_date:
#     get_github_daily_all_events(day=day)
#     print(day)
#     day += delta

