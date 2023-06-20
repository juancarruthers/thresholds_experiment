from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities
from datetime import datetime
import pandas as pd
from multipledispatch import dispatch

class PullReqFilter(GraphqlFilter):

    def __init__(self, p_filter: dict):
        super().__init__(p_filter)

    @dispatch(dict, str, str)
    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        util = Utilities()
        closePullReqQuery = util.readFile("APIQueries/PullReq/closedPullReq")
        mergedPullReqQuery = util.readFile("APIQueries/PullReq/mergedPullReq")
        openPullReqQuery = util.readFile("APIQueries/PullReq/openPullReq")

        if (json['pullRequests']['totalCount'] > 0):
            states = {'closed': closePullReqQuery, 'merged': mergedPullReqQuery, 'open': openPullReqQuery}
            newJson = {}
            for state, query in states.items():
                variables = {'owner': owner, 'repoName': repositoryName}
                lastPullReqQuery = {'query': query, 'variables': variables}
                jsonResponse = util.makeRequest(lastPullReqQuery)
                pullReqCount = jsonResponse['data']['repository']['pullRequests']['totalCount']
                if pullReqCount > 0:
                    newJson.update({state+"PullReqCount": pullReqCount, state+"PullReqLastDate": jsonResponse['data']['repository']['pullRequests']['nodes'][0]['createdAt']})
                else:
                    newJson.update({state + "PullReqCount": pullReqCount, state + "PullReqLastDate": '-'})
        else:
            newJson = {"closedPullReqCount": 0, "closedPullReqLastDate": "-", "mergedPullReqCount": 0, "mergedPullReqLastDate": "-", "openPullReqCount": 0, "openPullReqLastDate": "-"}

        pullReqCount = newJson['closedPullReqCount'] + newJson['mergedPullReqCount']

        if pullReqCount < self.filter['pullReqCount']:
            return True
        else:

            json.pop('pullRequests', None)
            json.update(newJson)
            return False

    @dispatch(dict, str, datetime)
    def updateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        prData = pd.read_csv(f'{path}_pr.csv')
        prData['closed_at'] = pd.to_datetime(prData['closed_at'])
        rows = prData[(prData['state']=='closed') & (prData['closed_at'] <= date)]
        closedRows = rows[~rows['merged']]
        mergedRows = rows[rows['merged']]
        closedLastDate = closedRows['closed_at'].max()
        mergedLastDate = mergedRows['closed_at'].max()
        merged = mergedRows.shape[0]
        closed = closedRows.shape[0]

        pullReqCount = closed + merged
        if (pullReqCount < self.filter['pullReqCount']):
            return True
        else:
            repository['closedPullReqCount'] = closed
            repository['mergedPullReqCount'] = merged
            repository['closedPullReqLastDate'] = closedLastDate
            repository['mergedPullReqLastDate'] = mergedLastDate
            return False