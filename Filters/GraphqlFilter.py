from datetime import datetime
import pandas as pd

class GraphqlFilter:
    def __init__(self, p_filter: dict):
        self.filter = p_filter

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        pass

    def xiaUpdateFrame(self, repository: pd.DataFrame, path: str, date: datetime) -> bool:
        pass