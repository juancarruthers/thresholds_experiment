from Filters.GraphqlFilter import GraphqlFilter
import datetime
from dateutil import relativedelta
import math
from Utilities import Utilities


class MunaiahFilter(GraphqlFilter):

    def __init__(self, p_filter: dict, p_itemsPageContrQuery = 100):

        super().__init__(p_filter)
        self._elementPerPageContribQuery = str(p_itemsPageContrQuery)

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:

        dateOfCreation = json['createdAt']
        dateLastCommit = json['dateLastCommit']
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        tstamp1 = datetime.datetime.strptime(dateOfCreation, fmt)
        tstamp2 = datetime.datetime.strptime(dateLastCommit, fmt)

        delta = relativedelta.relativedelta(tstamp2, tstamp1).months + relativedelta.relativedelta(tstamp2, tstamp1).years * 12

        history = math.trunc(json['commits'] / delta)
        issueFreq = math.trunc((json['closedIssuesCount'] + json['openIssuesCount']) / delta)
        coreContributors = self.coreContributors(json, owner, repositoryName)

        filtersFlag = history <= self.filter['history']
        filtersFlag = filtersFlag | (issueFreq <= self.filter['issueFrequency'])
        filtersFlag = filtersFlag | (coreContributors <= self.filter['coreContributors'])

        if filtersFlag:
            return True
        else:
            json.update({"history": history, 'issueFrequency': issueFreq, 'coreContributors': coreContributors})
            return False

    def coreContributors(self, json: dict, owner: str, repoName: str) -> int:
        url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page=" + self._elementPerPageContribQuery + "&page="
        i = 1
        util = Utilities()
        response = util.makeRequest("", "GET", url + str(i))
        coreContributors = 0
        contributed = 0
        j = 0

        if type(response) is list:

            while contributed < 0.8:
                i += 1
                while j < len(response) and contributed < 0.8:
                    coreContributors += 1
                    contributed += response[j]['contributions'] / json['contributions']
                    j += 1
                j = 0
                response = util.makeRequest("", "GET", url + str(i))

        json.pop('contributions', None)
        return coreContributors