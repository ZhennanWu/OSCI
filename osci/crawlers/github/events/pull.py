

from datetime import datetime
from typing import Iterator, NamedTuple

from .base import Event


class PullEventEntry(NamedTuple):
    event_id: int
    event_created_at: datetime

    repo_name: str
    org_name: str
    actor_login: str

    author_name: str
    author_email: str

    merged: bool
    closed: bool


class PullEvent(Event):
    def get_pull(self) -> PullEventEntry:
        pull = self.payload.get("pull_request")
        user = pull.get("user")
        return PullEventEntry(
            event_id=self.id,
            event_created_at=self.created_at,
            repo_name=self.repository.name,
            org_name=self.organization.login,
            actor_login=self.actor.login,

            author_name=user.get("name"),
            author_email=user.get("email"),

            merged=pull.get("merged_at") != None,
            closed=pull.get("state") == "closed"
        )


class PullEventsSchema(NamedTuple):
    event_id = "event_id"
    event_created_at = "event_created_at"

    repo_name = "repo_name"
    org_name = "org_name"
    actor_login = "actor_login"

    author_name = "author_name"
    author_email = "author_email"

    merged = "merged"
    closed = "closed"

    required = [event_id,
                event_created_at,
                repo_name,
                org_name,
                actor_login,
                author_name,
                author_email,
                merged,
                closed]


class ReviewEventEntry(NamedTuple):
    event_id: int
    event_created_at: datetime

    repo_name: str
    org_name: str
    actor_login: str

    author_name: str
    author_email: str


class ReviewEvent(Event):
    def get_review(self) -> PullEventEntry:
        pull = self.payload.get("review")
        user = pull.get("user")
        return ReviewEventEntry(
            event_id=self.id,
            event_created_at=self.created_at,
            repo_name=self.repository.name,
            org_name=self.organization.login,
            actor_login=self.actor.login,

            author_name=user.get("name"),
            author_email=user.get("email"),
        )
