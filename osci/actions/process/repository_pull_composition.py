import datetime
import logging
from typing import NamedTuple, Type
from osci.actions import Action, ActionParam
from osci.datalake import DatePeriodType
from osci.datalake.reports.general.base import GeneralReportFactory, Report
from osci.jobs.base import PushCommitsRankingJob
from osci.datalake import DataLake, DatePeriodType, GeneralReportFactory, Report, CompanyReportFactory, CompanyReport
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from osci.actions.consts import get_default_from_day, get_default_to_day
from osci.jobs.session import Session
from osci.crawlers.github.events.pull import PullEventsSchema

log = logging.getLogger(__name__)

class RepositoryPullCompositionAction(Action):
    params = (
        ActionParam(name='to_day', type=datetime, required=True, short_name='td', description='till this day',
                    default=get_default_to_day()),
        ActionParam(name='date_period', type=str, required=False,
                    short_name='dp', description='data period: day, month or year',
                    default=DatePeriodType.YTD, choices=DatePeriodType.all),
        ActionParam(name='from_day', type=datetime, required=False, short_name='fd',
                    description=f'Optional parameter will be ignored for no `DTD` time period.'),
    )

    @classmethod
    def help_text(cls) -> str:
        return "Transform, filter and save data for the subsequent creating report"

    @classmethod
    def name(cls):
        return 'repository-composition'

    def _execute(self, to_day: datetime, date_period: str, from_day: datetime):
        RepositoryPullCompositionJob(date_period_type=date_period).run(
            to_date=to_day, from_date=from_day)


class RepositoryPullCompositionSchema:
    repository = "Repository"
    company = "Company"
    commits = "Commits"


class RepositoryPullCompositionFactory(GeneralReportFactory):
    report_base_cls = type('_Report', (Report,),
                           dict(base_name='Repository_Composition_Ranking',
                                              schema=RepositoryPullCompositionSchema))
class RepositoryPullCompositionMTD(RepositoryPullCompositionFactory.report_base_cls):
    date_period = DatePeriodType.MTD


class RepositoryPullCompositionYTD(RepositoryPullCompositionFactory.report_base_cls):
    date_period = DatePeriodType.YTD



class PullRankingJob:
    """Base push commits ranking spark job"""
    REPORT_NAME = 'unnamed_report'
    REPORT_FACTORY: Type[GeneralReportFactory] = None
    report_cls: Type[Report]

    def __init__(self, date_period_type: str = DatePeriodType.YTD):
        self.data_lake = DataLake()
        self.pull_schema = PullEventsSchema()
        self.date_period_type = date_period_type
        self.report_cls: Type[Report] = self.REPORT_FACTORY().get_cls(date_period=self.date_period_type)

    def extract(self, to_date: datetime, from_date: datetime = None) -> DataFrame:
        return Session().load_dataframe(paths=self._get_dataset_paths(to_date, from_date))


    def _get_dataset_paths(self, to_date: datetime, from_date: datetime = None):
        paths = self.data_lake.staging.get_pull_events_spark_paths(from_date=from_date,
                                                                           to_date=to_date,
                                                                           date_period_type=self.date_period_type)
        log.debug(f'Loaded paths for ({from_date} {to_date}) {paths}')
        return paths

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError()

    def load(self, df: DataFrame, date: datetime):
        self.report_cls(date=date).save(df=df.toPandas())

    def run(self, to_date: datetime, from_date: datetime = None):
        df = self.extract(to_date, from_date)
        df = self.transform(df)
        self.load(df, to_date)


class RepositoryPullCompositionJob(PullRankingJob):
    REPORT_FACTORY = RepositoryPullCompositionFactory

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        report_schema = self.report_cls.schema

        repo_name_field = self.pull_schema.repo_name
        event_id_field = self.pull_schema.event_id
        company_field = self.pull_schema.company
        result_field = report_schema.commits
        return df\
            .select(f.col(repo_name_field), f.col(event_id_field), f.col(company_field))\
        .groupBy(repo_name_field, company_field)\
        .agg(f.count(event_id_field).alias(result_field))\
        .sort(repo_name_field, company_field)


