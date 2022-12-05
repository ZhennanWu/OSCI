

from datetime import datetime
from logging import log
import logging
from typing import Iterator, NamedTuple

from .base import Event

log = logging.getLogger(__name__)

class IssuesEventEntry(NamedTuple):
    event_id: int
    event_created_at: datetime

    repo_name: str
    org_name: str
    actor_login: str

    author_name: str
    author_email: str

    comments: int


class IssuesEvent(Event):
    def get_issues(self) -> IssuesEventEntry:
        issue = self.payload.get("issue")
        user = issue.get("user")
        return IssuesEventEntry(
            event_id=self.id,
            event_created_at=self.created_at,
            repo_name=self.repository.name,
            org_name=self.organization.login,
            actor_login=self.actor.login,

            author_name=user.get("name"),
            author_email=user.get("email"),

            comments = issue.get("comments"),
        )

class IssueCommentEventEntry(NamedTuple):
    event_id: int
    event_created_at: datetime

    repo_name: str
    org_name: str
    actor_login: str

    author_name: str
    author_email: str

class IssueCommentEvent(Event):
    def get_issue_comment(self) -> IssueCommentEventEntry:
        if self.payload == None:
            return None
        comment = self.payload.get("comment")
        if comment == None:
            log.warning(f"Broken comment event {self}")
            return None
        user = comment.get("user")
        return IssueCommentEventEntry(
            event_id=self.id,
            event_created_at=self.created_at,
            repo_name=self.repository.name,
            org_name=self.organization.login,
            actor_login=self.actor.login,

            author_name=user.get("name"),
            author_email=user.get("email"),
        )
