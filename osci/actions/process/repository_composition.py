import datetime
from osci.actions import Action, ActionParam
from osci.datalake import DatePeriodType

class RepositoryCompositionAction(Action):
    params = (
        ActionParam(name='to_day', type=datetime, required=True, short_name='td'),
    )

    @classmethod
    def help_text(cls) -> str:
        return "Transform, filter and save data for the subsequent creating report"

    @classmethod
    def name(cls):
        return 'daily-osci-rankings'
    
    def _execute(self, to_day: datetime):
        pass


class RepositoryCompositionJob()