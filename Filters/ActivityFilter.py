from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities


class ActivityFilter(GraphqlFilter):

    # UPDATE: to check if had 1 commit each month for the last 12 months

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        filter = self.filter['activity']
        util = Utilities()
        today = datetime.today()
        until = datetime(today.year, today.month, 1)
        since = filter['since']
        commitThreshold = filter['commits']
        months = pd.date_range(since, until, freq='MS')
        query = util.readFile("APIQueries/activityExploration")

        for i in range(1, len(months)):
            since = months[i-1].isoformat()
            until = months[i].isoformat()
            variables = {'owner': owner, 'name': repositoryName, 'since': since, 'until': until}
            activityQuery = {'query': query, 'variables': variables}
            jsonResponse = util.makeRequest(activityQuery)
            commits = jsonResponse['data']['repository']['defaultBranchRef']['target']['history']['edges']
            if len(commits) < commitThreshold:
                return True

        return False
