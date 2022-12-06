from osci.preprocess.match_company.process import process_github_daily_push_events
from osci.actions import Action
from datetime import datetime


class MatchCompanyAllAction(Action):
    """Enrich and filter by company"""

    @classmethod
    def help_text(cls) -> str:
        return "Command for processing daily github push events (filtering, enriching data etc)"

    @classmethod
    def name(cls):
        return 'process-github-daily-all-events'

    def _execute(self, day: datetime):
        return process_github_daily_all_events(day=day)

import datetime
import logging


from osci.datalake import DataLake

from osci.preprocess.match_company.push_commits import process_push_commits

log = logging.getLogger(__name__)


def process_github_daily_all_events(day: datetime.datetime):
    pull_events_commits = DataLake().landing.get_daily_pull_events(date=day)
    if pull_events_commits is not None and not pull_events_commits.empty:
        companies_events = process_push_commits(pull_events_commits,
                                                email_field=DataLake().landing.schemas.push_commits.author_email,
                                                company_field=DataLake().staging.schemas.push_commits.company,
                                                datetime_field=DataLake().landing.schemas.push_commits.event_created_at)
        for company, pull in companies_events:
            log.debug(f'Save company {company}')
            DataLake().staging.save_pull_events(push_event_commits=pull, date=day, company_name=company)
        
    issues_events_commits = DataLake().landing.get_daily_issues_events(date=day)
    if issues_events_commits is not None and not issues_events_commits.empty:
        companies_events = process_push_commits(issues_events_commits,
                                                email_field=DataLake().landing.schemas.push_commits.author_email,
                                                company_field=DataLake().staging.schemas.push_commits.company,
                                                datetime_field=DataLake().landing.schemas.push_commits.event_created_at)
        for company, issues in companies_events:
            log.debug(f'Save company {company}')
            DataLake().staging.save_issues_events(issues_event_commits=issues, date=day, company_name=company)
    
    issue_comment_events_commits = DataLake().landing.get_daily_issue_comment_events(date=day)
    if issue_comment_events_commits is not None and not issue_comment_events_commits.empty:
        companies_events = process_push_commits(issue_comment_events_commits,
                                                email_field=DataLake().landing.schemas.push_commits.author_email,
                                                company_field=DataLake().staging.schemas.push_commits.company,
                                                datetime_field=DataLake().landing.schemas.push_commits.event_created_at)
        for company, issue_comment in companies_events:
            log.debug(f'Save company {company}')
            DataLake().staging.save_issue_comment_events(issue_comment_event_commits=issue_comment, date=day, company_name=company)
    
    review_events_commits = DataLake().landing.get_daily_review_events(date=day)
    if review_events_commits is not None and not review_events_commits.empty:
        companies_events = process_push_commits(review_events_commits,
                                                email_field=DataLake().landing.schemas.push_commits.author_email,
                                                company_field=DataLake().staging.schemas.push_commits.company,
                                                datetime_field=DataLake().landing.schemas.push_commits.event_created_at)
        for company, review in companies_events:
            log.debug(f'Save company {company}')
            DataLake().staging.save_review_events(review_event_commits=review, date=day, company_name=company)