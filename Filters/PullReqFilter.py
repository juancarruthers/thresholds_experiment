from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities


class PullReqFilter(GraphqlFilter):

    def __init__(self, p_filter: dict):
        super().__init__(p_filter)
        util = Utilities()
        self._closePullReqQuery = util.readFile("APIQueries/PullReq/closedPullReq")
        self._mergedPullReqQuery = util.readFile("APIQueries/PullReq/mergedPullReq")
        self._openPullReqQuery = util.readFile("APIQueries/PullReq/openPullReq")
        self._tokens = util.readFile("token").split(",\n")

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        if filtersFlag:
            return True
        util = Utilities()

        if (json['pullRequests']['totalCount'] > 0):
            states = {'closed': self._closePullReqQuery, 'merged': self._mergedPullReqQuery, 'open': self._openPullReqQuery}
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