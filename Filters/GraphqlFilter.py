from datetime import datetime
from multipledispatch import dispatch

class GraphqlFilter:
    def __init__(self, p_filter: dict):
        self.filter = p_filter

    @dispatch(dict, str, str, bool)
    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        pass

    @dispatch(dict, str, datetime)
    def updateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        pass