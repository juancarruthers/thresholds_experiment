from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities

class IssuesFilter(GraphqlFilter):

    def __init__(self, p_filter: dict):
        super().__init__(p_filter)
        util = Utilities()
        self._closeIssuesQuery = util.readFile("APIQueries/Issues/closedIssues")
        self._openIssuesQuery = util.readFile("APIQueries/Issues/openIssues")

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        if filtersFlag:
            return True
        util = Utilities()

        if (json['issues']['totalCount'] > 0):
            states = {'closed': self._closeIssuesQuery, 'open': self._openIssuesQuery}
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