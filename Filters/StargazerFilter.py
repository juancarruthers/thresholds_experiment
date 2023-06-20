from Filters.GraphqlFilter import GraphqlFilter
import pandas as pd
import datetime

class StargazerFilter(GraphqlFilter):
    
    def updateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        starData = pd.read_csv(f'{path}_stargazer.csv')
        starData['starred_at'] = pd.to_datetime(starData['starred_at'])
        rows = starData[starData['starred_at'] <= date]
        total = rows.shape[0]

        if total < self.filter['stargazerCount']:
            return True
        else:
            repository["stargazerCount"] = total
            return False