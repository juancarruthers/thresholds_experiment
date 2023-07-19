from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities
import pandas as pd
from datetime import datetime

class ContributorsFilter(GraphqlFilter):


    def __init__(self, p_filter: dict, p_itemsPageContrQuery=100):
        super().__init__(p_filter)
        self._elementPerPageContribQuery = str(p_itemsPageContrQuery)

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        util = Utilities()

        url = "https://api.github.com/repos/" + owner + "/" + repositoryName + "/contributors?per_page="+ self._elementPerPageContribQuery +"&page="
        i = 1
        response = util.makeRequest("", "GET", url + str(i))
        acum = 0
        contribAcum = 0

        if type(response) is list:

            while len(response) > 0:
                acum += len(response)
                i += 1
                for contributor in response:
                    contribAcum += contributor['contributions']
                response = util.makeRequest("", "GET", url + str(i))

        if acum < self.filter['contributors']:
            return True
        else:
            json.update({"contributors": acum, "contributions": contribAcum})
            return False

    def xiaUpdateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        fmt = '%Y-%m-%d %H:%M:%S'
        commData = pd.read_csv(f'{path}_commits_and_comments.csv')
        commData['committed_at'] = pd.to_datetime(commData['committed_at'], format=fmt)
        rowsBeforeDate = commData[commData['committed_at'] < date]
        totalContrib = rowsBeforeDate['committer_id'].nunique()
        
        if (totalContrib < self.filter['contributors']):
            return True
        else:
            repository['contributors'] = totalContrib
            return False