"""Copyright since 2020, EPAM Systems

   This file is part of OSCI.

   OSCI is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   OSCI is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with OSCI.  If not, see <http://www.gnu.org/licenses/>."""
import yaml

from .base import LocalSystemArea
from osci.datalake.base import BaseStagingArea, DatePeriodType
from osci.utils import get_pandas_data_frame_info, normalize_company

from datetime import datetime
from typing import Union, List, Iterator
from pathlib import Path

import logging
import pandas as pd

log = logging.getLogger(__name__)


class LocalStagingArea(BaseStagingArea, LocalSystemArea):
    BASE_AREA_DIR = 'staging'

    def get_push_events_commits_file_path(self, date: datetime, company: str):
        return self.get_push_events_commits_parent_dir(date) / self.get_push_events_commits_filename(date, company)

    def get_private_push_events_commits_file_path(self, date: datetime, company: str):
        path = self.BASE_PATH / self.BASE_AREA_DIR / \
            normalize_company(name=company).upper()
        path = path / 'github' / 'events' / 'push' / \
            date.strftime("%Y") / date.strftime("%m")
        path.mkdir(parents=True, exist_ok=True)
        return path / date.strftime('%Y-%m-%d.parquet')

    def save_private_push_events_commits(self, push_event_commits: pd.DataFrame, company_name: str, date: datetime):
        file_path = self.get_private_push_events_commits_file_path(
            date=date, company=company_name)

        log.info(f'Save private push events commits for {date} into file {file_path}\n'
                 f'DF INFO: {get_pandas_data_frame_info(push_event_commits)}')

        push_event_commits.to_parquet(
            str(file_path), engine='pyarrow', index=False)

    @staticmethod
    def _get_file_name(path: Path) -> str:
        return path.name

    def save_push_events_commits(self, push_event_commits: pd.DataFrame, company_name: str, date: datetime):
        file_path = self.get_push_events_commits_file_path(
            date=date, company=company_name)

        log.info(f'Save push events commits for {date} into file {file_path}\n'
                 f'DF INFO: {get_pandas_data_frame_info(push_event_commits)}')

        push_event_commits.to_parquet(
            str(file_path), engine='pyarrow', index=False)

    def get_push_events_commits_spark_paths(self, to_date: datetime, date_period_type: str = DatePeriodType.YTD,
                                            from_date: datetime = None, company=None) -> List[str]:
        return self.get_spark_paths(self.get_push_events_commits_paths(to_date=to_date,
                                                                       date_period_type=date_period_type,
                                                                       from_date=from_date,
                                                                       company=company))

    def get_push_events_commits(self, to_date: datetime, date_period_type: str = DatePeriodType.YTD,
                                from_date: datetime = None, company=None) -> pd.DataFrame:
        paths = self.get_push_events_commits_paths(to_date=to_date, date_period_type=date_period_type,
                                                   from_date=from_date, company=company)
        return pd.concat([pd.read_parquet(path) for path in paths]) if paths else pd.DataFrame()

    def get_push_events_commits_parent_dir(self, date: datetime) -> Path:
        path = self._github_events_commits_base \
            / date.strftime("%Y") / date.strftime("%m") / date.strftime("%d")
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def get_push_events_commits_filename(date: datetime, company: str) -> str:
        return date.strftime(f'{normalize_company(name=company)}-%Y-%m-%d.parquet')

    def _get_date_partitioned_paths(self, dir_path: Union[str, Path], year: Union[str, int] = None,
                                    month: Union[str, int] = None, day: Union[str, int] = None,
                                    company: str = None,
                                    file_format='parquet'):

        if year is not None:
            dir_path = dir_path / str(year)
            if month is not None:
                dir_path = dir_path / str(month).zfill(2)
                if day is not None:
                    dir_path = dir_path / str(day).zfill(2)

        file_pattern = f'{normalize_company(name=company) + "-" if company else ""}*.{file_format}'
        return self._get_paths(dir_path=dir_path, file_pattern=file_pattern)

    def _get_configuration_file_path(self, file: str) -> Path:
        return self.BASE_PATH / self.BASE_AREA_DIR / self.CONF_AREA_DIR / file

    def load_projects_filter(self):
        with self._get_configuration_file_path(file='projects_filter.yaml').open('r') as stream:
            return yaml.load(stream, Loader=yaml.FullLoader)

    def get_repositories_path(self, date: datetime) -> Path:
        # path = self._github_repositories_base / date.strftime("%Y") / date.strftime("%m")
        path = self._github_repositories_base / "2020" / "01"
        path.mkdir(parents=True, exist_ok=True)
        return path / "repository-2020-01-01.parquet"

    def get_repositories(self, date: datetime) -> pd.DataFrame:
        return pd.read_parquet(self.get_repositories_path(date=date), engine='pyarrow')

    def save_repositories(self, df: pd.DataFrame, date: datetime):
        df.to_parquet(str(self.get_repositories_path(date=date)),
                      engine='pyarrow', index=False)

    def get_raw_push_events_commits_parent_dir_path(self, date: datetime):
        return self._github_raw_events_commits_base \
            / date.strftime("%Y") / date.strftime("%m") / date.strftime("%d")

    def get_raw_push_events_commits_path(self, date: datetime, company: str) -> Path:
        path = self.get_raw_push_events_commits_parent_dir_path(date)
        path.mkdir(parents=True, exist_ok=True)
        return path / self._get_raw_push_events_commits_file_name(date, company)

    def get_raw_push_events_commits(self, path: str) -> pd.DataFrame:
        path = Path(path)
        return pd.read_parquet(path, engine='pyarrow') if path.is_file() else pd.DataFrame()

    def save_raw_push_events_commits(self, push_event_commits: pd.DataFrame, company_name: str, date: datetime):
        return push_event_commits.to_parquet(self.get_raw_push_events_commits_path(date=date, company=company_name),
                                             engine='pyarrow', index=False)

    def get_daily_raw_push_events_commits_paths(self, date: datetime) -> Iterator[Union[str, Path]]:
        return self._get_paths(dir_path=self.get_raw_push_events_commits_parent_dir_path(date))

    def get_pull_events_spark_paths(self, to_date: datetime, date_period_type: str = DatePeriodType.YTD,
                                    from_date: datetime = None, company=None) -> List[str]:
        year, month, day = None, None, None
        if date_period_type == DatePeriodType.DTD:
            if from_date is None:
                raise ValueError(
                    f'`from_date` must be non None for {DatePeriodType.DTD} passed: {from_date}')
            if to_date < from_date:
                raise ValueError(
                    f'`from_date` {from_date} must be less than `to_date` {to_date}')
            if to_date.year == from_date.year:
                year = to_date.year
                if to_date.month == from_date.month:
                    month = to_date.month
                    if to_date.day == from_date.day:
                        day = to_date.day
        elif date_period_type == DatePeriodType.MTD:
            year, month = to_date.year, to_date.month
        elif date_period_type == DatePeriodType.YTD:
            year = to_date.year
        else:
            raise ValueError(
                f'Unsupported date period type: {date_period_type} not in {DatePeriodType.all}')

        results = self._get_date_partitioned_paths(dir_path=self._github_events_pull_base,
                                                   year=year, month=month, day=day, company=company)
        return self.get_spark_paths(results)
    
    def get_issues_events_spark_paths(self, to_date: datetime, date_period_type: str = DatePeriodType.YTD,
                                    from_date: datetime = None, company=None) -> List[str]:
        year, month, day = None, None, None
        if date_period_type == DatePeriodType.DTD:
            if from_date is None:
                raise ValueError(
                    f'`from_date` must be non None for {DatePeriodType.DTD} passed: {from_date}')
            if to_date < from_date:
                raise ValueError(
                    f'`from_date` {from_date} must be less than `to_date` {to_date}')
            if to_date.year == from_date.year:
                year = to_date.year
                if to_date.month == from_date.month:
                    month = to_date.month
                    if to_date.day == from_date.day:
                        day = to_date.day
        elif date_period_type == DatePeriodType.MTD:
            year, month = to_date.year, to_date.month
        elif date_period_type == DatePeriodType.YTD:
            year = to_date.year
        else:
            raise ValueError(
                f'Unsupported date period type: {date_period_type} not in {DatePeriodType.all}')

        results = self._get_date_partitioned_paths(dir_path=self._github_events_issues_base,
                                                   year=year, month=month, day=day, company=company)
        return self.get_spark_paths(results)
    
    def get_issue_comment_events_spark_paths(self, to_date: datetime, date_period_type: str = DatePeriodType.YTD,
                                    from_date: datetime = None, company=None) -> List[str]:
        year, month, day = None, None, None
        if date_period_type == DatePeriodType.DTD:
            if from_date is None:
                raise ValueError(
                    f'`from_date` must be non None for {DatePeriodType.DTD} passed: {from_date}')
            if to_date < from_date:
                raise ValueError(
                    f'`from_date` {from_date} must be less than `to_date` {to_date}')
            if to_date.year == from_date.year:
                year = to_date.year
                if to_date.month == from_date.month:
                    month = to_date.month
                    if to_date.day == from_date.day:
                        day = to_date.day
        elif date_period_type == DatePeriodType.MTD:
            year, month = to_date.year, to_date.month
        elif date_period_type == DatePeriodType.YTD:
            year = to_date.year
        else:
            raise ValueError(
                f'Unsupported date period type: {date_period_type} not in {DatePeriodType.all}')

        results = self._get_date_partitioned_paths(dir_path=self._github_events_issue_comment_base,
                                                   year=year, month=month, day=day, company=company)
        return self.get_spark_paths(results)
    
    def get_review_events_spark_paths(self, to_date: datetime, date_period_type: str = DatePeriodType.YTD,
                                    from_date: datetime = None, company=None) -> List[str]:
        year, month, day = None, None, None
        if date_period_type == DatePeriodType.DTD:
            if from_date is None:
                raise ValueError(
                    f'`from_date` must be non None for {DatePeriodType.DTD} passed: {from_date}')
            if to_date < from_date:
                raise ValueError(
                    f'`from_date` {from_date} must be less than `to_date` {to_date}')
            if to_date.year == from_date.year:
                year = to_date.year
                if to_date.month == from_date.month:
                    month = to_date.month
                    if to_date.day == from_date.day:
                        day = to_date.day
        elif date_period_type == DatePeriodType.MTD:
            year, month = to_date.year, to_date.month
        elif date_period_type == DatePeriodType.YTD:
            year = to_date.year
        else:
            raise ValueError(
                f'Unsupported date period type: {date_period_type} not in {DatePeriodType.all}')

        results = self._get_date_partitioned_paths(dir_path=self._github_events_review_base,
                                                   year=year, month=month, day=day, company=company)
        return self.get_spark_paths(results)

    def save_pull_events(self, pull_event: pd.DataFrame, company_name: str, date: datetime):
        path = self._get_paths(dir_path=self._github_events_pull_base /
                               date.strftime("%Y") / date.strftime("%m") / date.strftime("%d"))
        path.mkdir(parents=True, exist_ok=True)
        return pull_event.to_parquet(path / self._get_raw_push_events_commits_file_name(date, company_name), engine='pyarrow', index=False)
    
    def save_issues_events(self, issues_event: pd.DataFrame, company_name: str, date: datetime):
        path = self._get_paths(dir_path=self._github_events_issues_base /
                               date.strftime("%Y") / date.strftime("%m") / date.strftime("%d"))
        path.mkdir(parents=True, exist_ok=True)
        return issues_event.to_parquet(path / self._get_raw_push_events_commits_file_name(date, company_name), engine='pyarrow', index=False)
    
    def save_issue_comment_events(self, issue_comment_event: pd.DataFrame, company_name: str, date: datetime):
        path = self._get_paths(dir_path=self._github_events_issue_comment_base /
                               date.strftime("%Y") / date.strftime("%m") / date.strftime("%d"))
        path.mkdir(parents=True, exist_ok=True)
        return issue_comment_event.to_parquet(path / self._get_raw_push_events_commits_file_name(date, company_name), engine='pyarrow', index=False)
    
    def save_review_events(self, review_event: pd.DataFrame, company_name: str, date: datetime):
        path = self._get_paths(dir_path=self._github_events_review_base /
                               date.strftime("%Y") / date.strftime("%m") / date.strftime("%d"))
        path.mkdir(parents=True, exist_ok=True)
        return review_event.to_parquet(path / self._get_raw_push_events_commits_file_name(date, company_name), engine='pyarrow', index=False)
