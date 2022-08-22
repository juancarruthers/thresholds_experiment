import os.path
import random
import shutil
import requests
import pandas as pd
import datetime
import time
import concurrent.futures

class GithubGraphQL:

    def __init__(self, p_saveThreshold: int, p_itemsPageMainQuery = 30, p_itemsPageContrQuery = 100):
        self._tokens = self._readFile("token").split(",\n")
        self._startSize, self._sizeInc, self._df_data = self._restoreCheckPoint()
        self._saveThreshold = p_saveThreshold
        self._queryVar = "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:"
        self._queryFile = self._readFile("APIQueries/repositoryMetadata")
        self._repoCountQueryFile = self._readFile("APIQueries/repositoryCount")
        self._closeIssuesQuery = self._readFile("APIQueries/Issues/closedIssues")
        self._openIssuesQuery = self._readFile("APIQueries/Issues/openIssues")
        self._closePullReqQuery = self._readFile("APIQueries/PullReq/closedPullReq")
        self._mergedPullReqQuery = self._readFile("APIQueries/PullReq/mergedPullReq")
        self._openPullReqQuery = self._readFile("APIQueries/PullReq/openPullReq")
        self._elementPerPageMainQuery = p_itemsPageMainQuery
        self._elementPerPageContribQuery = str(p_itemsPageContrQuery)
        self._reqSleepTime = [50, 100, 150, 200, 250, 300]

    def main(self):
        start = datetime.datetime.now()
        repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + ">=" + str(self._startSize)}}
        repoCount = self._makeRequest(repoCountQuery)['data']['search']['repositoryCount']

        if repoCount > 0:
            projectsSaved = 0

            while repoCount > 0:

                repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                repoCountSubQuery = self._makeRequest(repoCountQuery)['data']['search']['repositoryCount']

                j = 1
                while (repoCountSubQuery >= 990) | (repoCountSubQuery == 0):
                    if repoCountSubQuery >= 990:
                        self._sizeInc -= j
                    else:
                        self._sizeInc += j
                    repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                    repoCountSubQuery = self._makeRequest(repoCountQuery)['data']['search']['repositoryCount']
                    j += j

                repoCount -= repoCountSubQuery
                cursor = None
                hasNextPage = True

                while hasNextPage:
                    variables = {'first': self._elementPerPageMainQuery, 'cursor': cursor, 'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}
                    repoQuery = {'query': self._queryFile, 'variables': variables}
                    jsonResponse = self._makeRequest(repoQuery)
                    repositories = jsonResponse['data']['search']

                    #CONCURRENCY
                    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
                    futures = {executor.submit(self._replaceNestedPropertiesValues, repoProperties) for repoProperties in repositories['edges']}
                    for future in concurrent.futures.as_completed(futures):
                        repositoryProperties = future.result()
                        self._df_data.append(repositoryProperties)

                    hasNextPage = repositories['pageInfo']['hasNextPage']
                    if hasNextPage:
                        cursor = repositories['pageInfo']['endCursor']

                projectsSaved += repoCountSubQuery

                if projectsSaved > self._saveThreshold:
                    self._saveCheckPoint(self._startSize, self._sizeInc, self._df_data)
                    projectsSaved = 0

                self._startSize += self._sizeInc

            finish = datetime.datetime.now()
            difference = finish - start
            print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)
            df = pd.DataFrame(self._df_data)
            df.to_csv("./datasets/githubquery" + str(finish.month) + str(finish.year) + ".csv")
            shutil.rmtree('./.backup')


    def _replaceNestedPropertiesValues(self, repoProperties: dict) -> dict:
            properties = repoProperties['node']
            owner = properties['owner']['login']
            repositoryName = properties['name']

            #NORMALIZE OUTPUT
            self._updateLanguage(properties)
            self._updateIssues(properties, owner, repositoryName)
            self._updatePullRequests(properties, owner, repositoryName)
            self._updateCommits(properties)
            self._updateContributors(properties, owner, repositoryName)

            print(datetime.datetime.now(), "- Owner:", owner, "- Repository:", repositoryName)

            return properties


    def _updateLanguage(self, json):
        if json['languages']['totalSize'] > 0:
            newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'], "totalSize": json['languages']['totalSize']}
        else:
            newJson = {"primaryLanguage": "-", "totalSize": 0}
        json.pop('languages', None)
        json.update(newJson)

    def _updateCommits(self, json):
        newJson = {"commits": json['defaultBranchRef']['target']['history']['totalCount'], "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
        json.pop('defaultBranchRef', None)
        json.update(newJson)

    def _updateIssues(self, json, owner, repoName):
        if (json['issues']['totalCount'] > 0):
            states = {'closed': self._closeIssuesQuery, 'open': self._openIssuesQuery}
            newJson = {}
            for state, query in states.items():
                variables = {'owner': owner, 'repoName': repoName}
                lastPullReqQuery = {'query': query, 'variables': variables}
                jsonResponse = self._makeRequest(lastPullReqQuery)
                pullReqCount = jsonResponse['data']['repository']['issues']['totalCount']
                if pullReqCount > 0:
                    newJson.update({state+"IssuesCount": pullReqCount, state+"IssueLastDate": jsonResponse['data']['repository']['issues']['nodes'][0]['createdAt']})
                else:
                    newJson.update({state + "IssuesCount": pullReqCount, state + "IssueLastDate": '-'})
        else:
            newJson = {"closedIssuesCount": 0, "closedIssueLastDate": "-", "openIssuesCount": 0, "openIssueLastDate": "-"}
        json.pop('issues', None)
        json.update(newJson)

    '''
    1)OBTENER CANTIDAD PULL REQUESTS CERRADAS DEL REPO NO MERGEADAS DE OTRO REPO
    2)FECHA ULTIMO PULL REQUEST
    '''
    def _updatePullRequests(self, json, owner, repoName):
        if (json['pullRequests']['totalCount'] > 0):
            states = {'closed': self._closePullReqQuery, 'merged': self._mergedPullReqQuery, 'open': self._openPullReqQuery}
            newJson = {}
            for state, query in states.items():
                variables = {'owner': owner, 'repoName': repoName}
                lastPullReqQuery = {'query': query, 'variables': variables}
                jsonResponse = self._makeRequest(lastPullReqQuery)
                pullReqCount = jsonResponse['data']['repository']['pullRequests']['totalCount']
                if pullReqCount > 0:
                    newJson.update({state+"PullReqCount": pullReqCount, state+"PullReqLastDate": jsonResponse['data']['repository']['pullRequests']['nodes'][0]['createdAt']})
                else:
                    newJson.update({state + "PullReqCount": pullReqCount, state + "PullReqLastDate": '-'})
        else:
            newJson = {"closedPullReqCount": 0, "closedPullReqLastDate": "-", "mergedPullReqCount": 0, "mergedPullReqLastDate": "-", "openPullReqCount": 0, "openPullReqLastDate": "-"}
        json.pop('pullRequests', None)
        json.update(newJson)

    def _updateContributors(self, json, owner, repoName):
        url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page="+ self._elementPerPageContribQuery +"&page="
        i = 1
        response = self._makeRequest("", "GET", url + str(i))

        if not(type(response) is list):
            acum = 1000
        else:
            acum = 0

            while len(response) > 0:
                acum += len(response)
                i += 1
                response = self._makeRequest("", "GET", url + str(i))

        json.pop('owner', None)
        json.update({"contributors": acum})


    def _makeRequest (self, query: str | dict, reqType="POST", url='https://api.github.com/graphql') -> dict:
        headers = {'Authorization': 'Bearer ' + random.choice(self._tokens)}
        response: dict
        response, condition = self._requestCondition(query, reqType, url, headers)

        while condition:
            time.sleep(random.choice(self._reqSleepTime))
            response, condition = self._requestCondition(query, reqType, url, headers)

        return response


    def _requestCondition (self, query: str | dict, reqType: str, url: str, headers: dict) -> tuple[dict, bool]:
        condition: bool

        if reqType == "POST":
            response = requests.post(url, json=query, headers=headers).json()
            condition = response.get("errors", False)
        else:
            response = requests.get(url, headers=headers).json()
            if not (type(response) is list):
                message: str = response['message']
                condition = message != 'The history or contributor list is too large to list contributors for this repository via the API.'
            else:
               condition = not (type(response) is list)


        return response, condition


    def _readFile(self, filePath: str) -> str:
        file = open(filePath).readlines()
        query = ""

        for lines in file:
            query += lines

        return query

    def _restoreCheckPoint(self) -> tuple[int, int, list]:
        startSize = 10000
        sizeInc = 2000
        path = './.backup'
        if os.path.isdir(path):
            backupData = pd.read_csv(path + '/largerFrame.csv', encoding='unicode_escape').to_dict('records')
            queryState = pd.read_csv(path + '/queryState.csv')
            return int(queryState.iloc[0, 0]), int(queryState.iloc[0, 1]), backupData
        else:
            return startSize, sizeInc, []

    def _saveCheckPoint(self, startSize, sizeInc, dataset):
        path = "./.backup"
        if not (os.path.isdir(path)):
            os.mkdir(path)

        df = pd.DataFrame(dataset)
        df.to_csv(path + '/largerFrame.csv', index=False)
        queryState = pd.DataFrame([{'startSize': startSize, 'sizeInc': sizeInc}])
        queryState.to_csv(path + "/queryState.csv", index=False)

if __name__ == '__main__':
    test = GithubGraphQL(5000)
    test.main()