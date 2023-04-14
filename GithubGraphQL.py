import math
import random
import shutil
import requests
import pandas as pd
import datetime
import time
import concurrent.futures
import Utilities as util
from dateutil import relativedelta

class GithubGraphQL:

    def __init__(self, queryFilter: str, filters: dict, folderPath: str, p_saveThreshold = 5000, p_itemsPageMainQuery = 30, p_itemsPageContrQuery = 100):
        self._tokens = util.readFile("token").split(",\n")
        self._startSize, self._sizeInc, self._df_data = util.restoreCheckPoint()
        self._saveThreshold = p_saveThreshold
        self._queryVar = queryFilter + ", size:"
        self._filters = filters
        self._queryFile = util.readFile("APIQueries/repositoryMetadata")
        self._repoCountQueryFile = util.readFile("APIQueries/repositoryCount")
        self._closeIssuesQuery = util.readFile("APIQueries/Issues/closedIssues")
        self._openIssuesQuery = util.readFile("APIQueries/Issues/openIssues")
        self._closePullReqQuery = util.readFile("APIQueries/PullReq/closedPullReq")
        self._mergedPullReqQuery = util.readFile("APIQueries/PullReq/mergedPullReq")
        self._openPullReqQuery = util.readFile("APIQueries/PullReq/openPullReq")
        self._elementPerPageMainQuery = p_itemsPageMainQuery
        self._elementPerPageContribQuery = str(p_itemsPageContrQuery)
        self._reqSleepTime = [50, 100, 150, 200, 250, 300]
        self._folderPath = folderPath
        self._quit = False

    def main(self):
        try:
            start = datetime.datetime.now()
            repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + ">=" + str(self._startSize)}}
            repoCount = self.makeRequest(repoCountQuery)['data']['search']['repositoryCount']

            if repoCount > 0:
                projectsSaved = 0

                while repoCount > 0:

                    repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                    repoCountSubQuery = self.makeRequest(repoCountQuery)['data']['search']['repositoryCount']

                    j = 1
                    while (repoCountSubQuery >= 1000) | (repoCountSubQuery == 0):
                        if repoCountSubQuery >= 1000:
                            self._sizeInc -= j
                        else:
                            self._sizeInc += j
                        repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                        repoCountSubQuery = self.makeRequest(repoCountQuery)['data']['search']['repositoryCount']
                        j += j

                    repoCount -= repoCountSubQuery
                    cursor = None
                    hasNextPage = True

                    while hasNextPage:
                        variables = {'first': self._elementPerPageMainQuery, 'cursor': cursor, 'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}
                        repoQuery = {'query': self._queryFile, 'variables': variables}
                        jsonResponse = self.makeRequest(repoQuery)
                        repositories = jsonResponse['data']['search']

                        #PARALELISM
                        executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
                        futures = {executor.submit(self._replaceNestedPropertiesValues, repoProperties) for repoProperties in repositories['edges']}
                        for future in concurrent.futures.as_completed(futures):
                            repositoryProperties, filtersFlag = future.result()
                            if not filtersFlag:
                                self._df_data.append(repositoryProperties)

                        hasNextPage = repositories['pageInfo']['hasNextPage']
                        if hasNextPage:
                            cursor = repositories['pageInfo']['endCursor']

                    projectsSaved += repoCountSubQuery

                    if projectsSaved > self._saveThreshold:
                        util.saveCheckPoint(self._startSize, self._sizeInc, self._df_data)
                        projectsSaved = 0

                    self._startSize += self._sizeInc

                finish = datetime.datetime.now()
                difference = finish - start
                print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)

                dataset = pd.DataFrame(self._df_data)
                dataset = dataset.drop_duplicates(subset=['id'])
                dataset.to_csv(self._folderPath + "/frame.csv", index=False)
                shutil.rmtree('./.backup')
        except KeyboardInterrupt:
            self.quit = True

    def _replaceNestedPropertiesValues(self, repoProperties: dict) -> tuple[dict, bool]:
        if not self._quit:

            properties = repoProperties['node']
            owner = properties['owner']['login']
            repositoryName = properties['name']

            filtersFlag = False
            properties["owner"] = owner

            #NORMALIZE OUTPUT
            filtersFlag = self.checkKeywords(repositoryName, filtersFlag)
            filtersFlag = self.updateLanguage(properties, filtersFlag)
            filtersFlag = self.updateCommits(properties, filtersFlag)
            filtersFlag = self.updateIssues(properties, owner, repositoryName, filtersFlag)
            filtersFlag = self.updatePullRequests(properties, owner, repositoryName, filtersFlag)
            filtersFlag = self.updateContributors(properties, owner, repositoryName, filtersFlag)
            filtersFlag = self.updateMunaiahMetrics(properties, owner, repositoryName, filtersFlag)
            filtersFlag = self._evaluateMaintainability(properties, filtersFlag)

            print(datetime.datetime.now(), "- Owner:", owner, "- Repository:", repositoryName)

            return properties, filtersFlag

    def updateLanguage(self, json: dict, filtersFlag: bool) -> bool:
        totalSize: int = json['languages']['totalSize']

        if filtersFlag or totalSize < self._filters['totalSize']:
            return True

        if totalSize > 0:
            newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'], "totalSize": json['languages']['totalSize']}
        else:
            newJson = {"primaryLanguage": "-", "totalSize": 0}

        json.pop('languages', None)
        json.update(newJson)
        return False

    def updateCommits(self, json: dict, filtersFlag: bool) -> bool:
        commits = json['defaultBranchRef']['target']['history']['totalCount']
        if filtersFlag or commits < self._filters['commits']:
            return True

        newJson = {"commits": commits, "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
        json.pop('defaultBranchRef', None)
        json.update(newJson)
        return False

    def updateIssues(self, json: dict, owner: str, repoName: str, filtersFlag: bool):
        if filtersFlag:
            return True

        if (json['issues']['totalCount'] > 0):
            states = {'closed': self._closeIssuesQuery, 'open': self._openIssuesQuery}
            newJson = {}
            for state, query in states.items():
                variables = {'owner': owner, 'repoName': repoName}
                lastPullReqQuery = {'query': query, 'variables': variables}
                jsonResponse = self.makeRequest(lastPullReqQuery)
                pullReqCount = jsonResponse['data']['repository']['issues']['totalCount']
                if pullReqCount > 0:
                    newJson.update({state+"IssuesCount": pullReqCount, state+"IssueLastDate": jsonResponse['data']['repository']['issues']['nodes'][0]['createdAt']})
                else:
                    newJson.update({state + "IssuesCount": pullReqCount, state + "IssueLastDate": '-'})
        else:
            newJson = {"closedIssuesCount": 0, "closedIssueLastDate": "-", "openIssuesCount": 0, "openIssueLastDate": "-"}

        if newJson['closedIssuesCount'] < self._filters['closedIssuesCount']:
            return True
        else:

            json.pop('issues', None)
            json.update(newJson)
            return False

    '''
    1)OBTENER CANTIDAD PULL REQUESTS CERRADAS DEL REPO NO MERGEADAS DE OTRO REPO
    2)FECHA ULTIMO PULL REQUEST
    '''
    def updatePullRequests(self, json: dict, owner: str, repoName: str, filtersFlag: bool):
        if filtersFlag:
            return True

        if (json['pullRequests']['totalCount'] > 0):
            states = {'closed': self._closePullReqQuery, 'merged': self._mergedPullReqQuery, 'open': self._openPullReqQuery}
            newJson = {}
            for state, query in states.items():
                variables = {'owner': owner, 'repoName': repoName}
                lastPullReqQuery = {'query': query, 'variables': variables}
                jsonResponse = self.makeRequest(lastPullReqQuery)
                pullReqCount = jsonResponse['data']['repository']['pullRequests']['totalCount']
                if pullReqCount > 0:
                    newJson.update({state+"PullReqCount": pullReqCount, state+"PullReqLastDate": jsonResponse['data']['repository']['pullRequests']['nodes'][0]['createdAt']})
                else:
                    newJson.update({state + "PullReqCount": pullReqCount, state + "PullReqLastDate": '-'})
        else:
            newJson = {"closedPullReqCount": 0, "closedPullReqLastDate": "-", "mergedPullReqCount": 0, "mergedPullReqLastDate": "-", "openPullReqCount": 0, "openPullReqLastDate": "-"}

        pullReqCount = newJson['closedPullReqCount'] + newJson['mergedPullReqCount']

        if pullReqCount < self._filters['pullReqCount']:
            return True
        else:

            json.pop('pullRequests', None)
            json.update(newJson)
            return False

    def updateContributors(self, json: dict, owner: str, repoName: str, filtersFlag: bool):

        if filtersFlag:
            return True

        url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page="+ self._elementPerPageContribQuery +"&page="
        i = 1
        response = self.makeRequest("", "GET", url + str(i))
        acum = 0
        contribAcum = 0

        if type(response) is list:

            while len(response) > 0:
                acum += len(response)
                i += 1
                for contributor in response:
                    contribAcum += contributor['contributions']
                response = self.makeRequest("", "GET", url + str(i))

        if acum < self._filters['contributors']:
            return True
        else:
            json.update({"contributors": acum, "contributions": contribAcum})
            return False

    def coreContributors (self, json: dict, owner: str, repoName: str) -> int:
        url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page=" + self._elementPerPageContribQuery + "&page="
        i = 1
        response = self.makeRequest("", "GET", url + str(i))
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
                response = self.makeRequest("", "GET", url + str(i))

        json.pop('contributions', None)
        return coreContributors



    def updateMunaiahMetrics(self, json: dict, owner: str, repoName: str, filtersFlag: bool) -> bool:
        if filtersFlag:
            return True

        dateOfCreation = json['createdAt']
        dateLastCommit = json['dateLastCommit']
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        tstamp1 = datetime.datetime.strptime(dateOfCreation, fmt)
        tstamp2 = datetime.datetime.strptime(dateLastCommit, fmt)

        delta = relativedelta.relativedelta(tstamp2, tstamp1).months + relativedelta.relativedelta(tstamp2, tstamp1).years * 12

        history = math.trunc(json['commits'] / delta)
        issueFreq = math.trunc((json['closedIssuesCount'] + json['openIssuesCount']) / delta)
        coreContributors = self.coreContributors(json, owner, repoName)

        filtersFlag = history <= self._filters['history']
        filtersFlag = filtersFlag | (issueFreq <= self._filters['issueFrequency'])
        filtersFlag = filtersFlag | (coreContributors <= self._filters['coreContributors'])

        if filtersFlag:
            return True
        else:
            json.update({"history": history, 'issueFrequency': issueFreq, 'coreContributors': coreContributors})
            return False

    def _evaluateMaintainability(self, json: dict, filtersFlag: bool) -> bool:
        if filtersFlag:
            return True

        dateLastCommitCond = json['dateLastCommit'] < self._filters['dateLastCommit']
        dateLastPullReq = json['closedPullReqLastDate'] < self._filters['dateLastPullReq'] and json['closedPullReqLastDate'] < self._filters['dateLastPullReq']

        return dateLastCommitCond and dateLastPullReq

    def checkKeywords(self, repoName: str, filtersFlag: bool) -> bool:

        if filtersFlag or any(keyword in repoName for keyword in self._filters['keywords']):
            return True

        return False


    def makeRequest (self, query: str | dict, reqType="POST", url='https://api.github.com/graphql') -> dict:
        headers = {'Authorization': 'Bearer ' + random.choice(self._tokens)}
        response: dict
        response, condition = self._requestCondition(query, reqType, url, headers)

        while condition:
            print(response)
            if "or it could be a GitHub bug" in str(condition):
                variables : dict = query['variables']
                first = round(variables['first']/2)
                query['variables']['first'] = first

            time.sleep(random.choice(self._reqSleepTime))
            headers = {'Authorization': 'Bearer ' + random.choice(self._tokens)}
            response, condition = self._requestCondition(query, reqType, url, headers)

        return response


    def _requestCondition (self, query: str | dict, reqType: str, url: str, headers: dict) -> tuple[dict, bool]:
        condition: bool
        try:
            if reqType == "POST":
                response = requests.post(url, json=query, headers=headers, timeout=120).json()
                condition = response.get("errors", False)
            else:
                response = requests.get(url, headers=headers, timeout=120).json()
                if not (type(response) is list):
                    message: str = response['message']
                    condition = message != 'The history or contributor list is too large to list contributors for this repository via the API.'
                else:
                   condition = not (type(response) is list)


            return response, condition
        except BaseException as err:
            self.quit = True
            print(err)
            exit()

