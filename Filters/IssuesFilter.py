from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities
import pandas as pd
from datetime import datetime

class IssuesFilter(GraphqlFilter):

    def __init__(self, p_filter: dict):
        super().__init__(p_filter)

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:

        util = Utilities()
        closeIssuesQuery = util.readFile("APIQueries/Issues/closedIssues")
        openIssuesQuery = util.readFile("APIQueries/Issues/openIssues")

        if (json['issues']['totalCount'] > 0):
            states = {'closed': closeIssuesQuery, 'open': openIssuesQuery}
            newJson = {}
            for state, query in states.items():
                variables = {'owner': owner, 'repoName': repositoryName}
                lastPullReqQuery = {'query': query, 'variables': variables}
                jsonResponse = util.makeRequest(lastPullReqQuery)
                pullReqCount = jsonResponse['data']['repository']['issues']['totalCount']
                if pullReqCount > 0:
                    newJson.update({state+"IssuesCount": pullReqCount, state+"IssueLastDate": jsonResponse['data']['repository']['issues']['nodes'][0]['createdAt']})
                else:
                    newJson.update({state + "IssuesCount": pullReqCount, state + "IssueLastDate": '-'})
        else:
            newJson = {"closedIssuesCount": 0, "closedIssueLastDate": "-", "openIssuesCount": 0, "openIssueLastDate": "-"}

        if newJson['closedIssuesCount'] < self.filter['closedIssuesCount']:
            return True
        else:

            json.pop('issues', None)
            json.update(newJson)
            return False

    def xiaUpdateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        issueData = pd.read_csv(f'{path}_issues.csv')
        date1 = pd.to_datetime(issueData['closed_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
        date2 = pd.to_datetime(issueData['closed_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        issueData['closed_at'] = date1.fillna(date2)
        rows = issueData[(issueData['state'] == "closed") & (issueData['closed_at'] < date)]
        total = (rows.shape[0] - repository['closedPullReqCount'] - repository['mergedPullReqCount'])[0]
        
        if (total < self.filter['closedIssuesCount']):
            return True
        else:
            repository['closedIssuesCount'] = total
            return False