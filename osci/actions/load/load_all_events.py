from osci.actions import Action
from datetime import datetime
from osci.crawlers.github.events.crawler import get_hour_events
from osci.crawlers.github.events.issue import IssueCommentEvent, IssuesEvent
from osci.crawlers.github.events.parser import get_push_events, get_push_events_commits
from osci.crawlers.github.events.pull import PullEvent, ReviewEvent
from osci.crawlers.github.events.push import PushEvent
from osci.crawlers.github.gharchive import get_github_daily_push_events
from osci.crawlers.github.rest import GithubArchiveRest
from osci.datalake import DataLake


class GithubDailyAllEventAction(Action):
    """Load push event commits from Github"""

    @classmethod
    def help_text(cls) -> str:
        return "Command for downloading daily push event commits from Github"

    @classmethod
    def name(cls):
        return 'get-github-daily-all-events'

    def _execute(self, day: datetime):
        return get_github_daily_all_events(day=day)


def get_github_daily_all_events(day: datetime):
    with GithubArchiveRest() as rest:
        for hour in range(24):
            # log.info(f'Crawl events for {day}')
            day = day.replace(hour=hour)
            [push_events, issues_events, issue_comment_events, pull_events, review_events] = get_hour_all_events(
                day=day, rest=rest)
            DataLake().landing.save_push_events_commits(
                push_event_commits=get_push_events_commits(push_events=push_events), date=day)
            DataLake().landing.save_issues_events(
                push_event_commits=(x.get_issues() for x in issues_events), date=day)
            DataLake().landing.save_issue_comment_events(
                push_event_commits=(x for x in (x.get_issue_comment() for x in issue_comment_events) if x is not None), date=day)
            DataLake().landing.save_pull_events(
                push_event_commits=(x.get_pull() for x in pull_events), date=day)
            DataLake().landing.save_review_events(
                push_event_commits=(x.get_review() for x in review_events), date=day)


def get_hour_all_events(day: datetime, rest: GithubArchiveRest):
    events = list(get_hour_events(day=day, rest=rest))
    return [filter(lambda event: isinstance(event, PushEvent), events),
            filter(lambda event: isinstance(event, IssuesEvent), events),
            filter(lambda event: isinstance(event, IssueCommentEvent), events),
            filter(lambda event: isinstance(event, PullEvent), events),
            filter(lambda event: isinstance(event, ReviewEvent), events),
            ]
