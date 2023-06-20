from Filters.GraphqlFilter import GraphqlFilter
import pandas as pd
import datetime

class ForkFilter(GraphqlFilter):

    def updateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        forkData = pd.read_csv(f'{path}_forks.csv')
        forkData['created_at'] = pd.to_datetime(forkData['created_at'])
        rows = forkData[forkData['created_at'] <= date]
        total = rows.shape[0]

        if total < self.filter['forkCount']:
            return True
        else:
            repository["forkCount"] = total
            return False