import datetime
from osci.actions.process.repository_composition import RepositoryCompositionJob


RepositoryCompositionJob(date_period_type="MTD").run(
            to_date=datetime.datetime(2020, 1, 1), from_date=datetime.datetime(2020, 1, 1))