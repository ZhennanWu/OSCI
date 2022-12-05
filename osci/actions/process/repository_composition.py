import datetime
from osci.actions import Action, ActionParam
from osci.datalake import DatePeriodType
from osci.datalake.reports.general.base import GeneralReportFactory, Report
from osci.jobs.base import PushCommitsRankingJob

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from osci.actions.consts import get_default_from_day, get_default_to_day

class RepositoryCompositionAction(Action):
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
        RepositoryCompositionJob(date_period_type=date_period).run(
            to_date=to_day, from_date=from_day)


class RepositoryCompositionSchema:
    repository = "Repository"
    company = "Company"
    commits = "Commits"


class RepositoryCompositionFactory(GeneralReportFactory):
    report_base_cls = type('_Report', (Report,),
                           dict(base_name='Repository_Composition_Ranking',
                                              schema=RepositoryCompositionSchema))
class RepositoryCompositionMTD(RepositoryCompositionFactory.report_base_cls):
    date_period = DatePeriodType.MTD


class RepositoryCompositionYTD(RepositoryCompositionFactory.report_base_cls):
    date_period = DatePeriodType.YTD


class RepositoryCompositionJob(PushCommitsRankingJob):
    REPORT_FACTORY = RepositoryCompositionFactory

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        report_schema = self.report_cls.schema

        repo_name_field = self.commits_schema.repo_name
        commits_id_field = self.commits_schema.sha
        company_field = self.commits_schema.company
        result_field = report_schema.commits
        return df\
            .select(f.col(repo_name_field), f.col(commits_id_field), f.col(company_field))\
        .groupBy(repo_name_field, company_field)\
        .agg(f.count(commits_id_field).alias(result_field))\
        .sort(repo_name_field, company_field)


