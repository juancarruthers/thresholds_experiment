import os.path
import random
import shutil
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
import numpy as np
import requests
import pandas as pd
import datetime
import time
import concurrent.futures
import SampleBuilder as SB
from DiversityScore import DiversityScore

class GithubGraphQL:

    def __init__(self, queryFilter: str, filters: dict, folderPath: str, p_saveThreshold = 5000, p_itemsPageMainQuery = 30, p_itemsPageContrQuery = 100):
        self._tokens = self.readFile("token").split(",\n")
        self._startSize, self._sizeInc, self._df_data = self._restoreCheckPoint()
        self._saveThreshold = p_saveThreshold
        self._queryVar = queryFilter + ", size:"
        self._filters = filters
        self._queryFile = self.readFile("APIQueries/repositoryMetadata")
        self._repoCountQueryFile = self.readFile("APIQueries/repositoryCount")
        self._closeIssuesQuery = self.readFile("APIQueries/Issues/closedIssues")
        self._openIssuesQuery = self.readFile("APIQueries/Issues/openIssues")
        self._closePullReqQuery = self.readFile("APIQueries/PullReq/closedPullReq")
        self._mergedPullReqQuery = self.readFile("APIQueries/PullReq/mergedPullReq")
        self._openPullReqQuery = self.readFile("APIQueries/PullReq/openPullReq")
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
                    while (repoCountSubQuery >= 990) | (repoCountSubQuery == 0):
                        if repoCountSubQuery >= 990:
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

                        #CONCURRENCY
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
                        self._saveCheckPoint(self._startSize, self._sizeInc, self._df_data)
                        projectsSaved = 0

                    self._startSize += self._sizeInc

                finish = datetime.datetime.now()
                difference = finish - start
                print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)

                self._filterProjects()
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
            filtersFlag = self.updateLanguage(properties, filtersFlag)
            filtersFlag = self.updateCommits(properties, filtersFlag)
            filtersFlag = self.updateIssues(properties, owner, repositoryName, filtersFlag)
            filtersFlag = self.updatePullRequests(properties, owner, repositoryName, filtersFlag)
            filtersFlag = self.updateContributors(properties, owner, repositoryName, filtersFlag)

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
        dateLastCommit = json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']
        if filtersFlag or dateLastCommit < self._filters['dateLastCommit']:
            return True

        newJson = {"commits": json['defaultBranchRef']['target']['history']['totalCount'], "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
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

        json.pop('issues', None)
        json.update(newJson)

        if json['closedIssuesCount'] < self._filters['closedIssuesCount']:
            return True
        else:
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

        json.pop('pullRequests', None)
        json.update(newJson)

        if json['closedPullReqCount'] < self._filters['closedPullReqCount']:
            return True
        else:
            return False

    def updateContributors(self, json: dict, owner: str, repoName: str, filtersFlag: bool):

        if filtersFlag:
            return True

        url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page="+ self._elementPerPageContribQuery +"&page="
        i = 1
        response = self.makeRequest("", "GET", url + str(i))

        if not(type(response) is list):
            acum = 1000
        else:
            acum = 0

            while len(response) > 0:
                acum += len(response)
                i += 1
                response = self.makeRequest("", "GET", url + str(i))

        #json.pop('owner', None)
        json.update({"contributors": acum})

        if json['contributors'] < self._filters['contributors']:
            return True
        else:
            return False


    def makeRequest (self, query: str | dict, reqType="POST", url='https://api.github.com/graphql') -> dict:
        headers = {'Authorization': 'Bearer ' + random.choice(self._tokens)}
        response: dict
        response, condition = self._requestCondition(query, reqType, url, headers)

        while condition:
            time.sleep(random.choice(self._reqSleepTime))
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


    def readFile(self, filePath: str) -> str:
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

    def _saveCheckPoint(self, startSize: int, sizeInc: int, dataset: list[dict]):
        path = "./.backup"
        if not (os.path.isdir(path)):
            os.mkdir(path)

        df = pd.DataFrame(dataset)
        df.to_csv(path + '/largerFrame.csv', index=False)
        queryState = pd.DataFrame([{'startSize': startSize, 'sizeInc': sizeInc}])
        queryState.to_csv(path + "/queryState.csv", index=False)

    def _updateProject(self, project, repoDataQuery)-> bool | dict:
        variables = {'repoName': project['name'], 'owner': project['owner']}
        repoUpdate = {'query': repoDataQuery, 'variables': variables}
        jsonResponse = self.makeRequest(repoUpdate)['data']['repository']
        self.updateLanguage(jsonResponse, False)
        result = self.updateCommits(jsonResponse, False)

        if result:
            return False

        else:
            jsonResponse["owner"] = project['owner']
            self.updateIssues(jsonResponse, project['owner'], project['name'], False)
            self.updatePullRequests(jsonResponse, project['owner'], project['name'], False)
            self.updateContributors(jsonResponse, project['owner'], project['name'], False)
            return jsonResponse


    def updateSample(self, frameUpdated: pd.DataFrame, sample: pd.DataFrame, groups: pd.DataFrame, dimensions: list[str]):
        frameUpdated = frameUpdated[~frameUpdated['id'].isin(sample['id'])]
        repoDataQuery = self._readFile("./APIQueries/repositoryUpdate")
        sampleFiltered = sample[sample['dateLastCommit'] < self._filters['dateLastCommit']]
        for id, project in sampleFiltered.iterrows():
            group = project['groupId']
            jsonResponse = self._updateProject(project, repoDataQuery)
            if jsonResponse:
                groupUpdate = SB.getProjectGroup(jsonResponse, groups, dimensions).values.tolist()
                if group in groupUpdate:
                    jsonResponse['groupId'] = group
                    newProject = pd.Series(jsonResponse)

                else:
                    newProject = self._replace(frameUpdated, groups[group == groups['groupId']], dimensions)
            else:
                newProject = self._replace(frameUpdated, groups[group == groups['groupId']], dimensions)

            if newProject.empty:
                sample.drop(id, inplace=True)
            else:
                sample.loc[id] = newProject

        sample.to_csv(self._folderPath + "/sampleUpdated.csv", index=False)

    def _replace(self, frame: pd.DataFrame, group: pd.DataFrame, dimensions: list[str]):
        frameAux = frame.copy()
        for dimension in dimensions:
            frameAux = frameAux[group[dimension + 'Min'].item() <= frameAux[dimension]]
            frameAux = frameAux[group[dimension + 'Max'].item() >= frameAux[dimension]]

        project = pd.DataFrame()
        if frameAux.shape[0] > 0:
            frameAux['groupId'] = group['groupId'].item()
            randomNumber = random.randint(0, frameAux.shape[0] - 1)
            project = frameAux.iloc[randomNumber]
        return project


    def updateFrame(self, frame: pd.DataFrame, dimensions: list[str]):
        repoDataQuery = self.readFile("./APIQueries/repositoryUpdate")
        frameFiltered = frame[frame['dateLastCommit'] < self._filters['dateLastCommit']]
        for id, project in frameFiltered.iterrows():
            jsonResponse = self._updateProject(project, repoDataQuery)
            if jsonResponse:
                newRow = pd.Series(jsonResponse)
                frame.loc[id] = newRow

            else:
                frame.drop(id, inplace=True)

        frame.to_csv(self._folderPath + "/frameUpdated.csv", index=False)
        frame = pd.read_csv(self._folderPath + "/frameUpdated.csv")
        self.sampleRecategorization2(dimensions, frame)

    def sampleRecategorization(self, dimensions, frame: pd.DataFrame):


        clusterizer = DiversityScore(frame, dimensions)
        diverseSample = SB.createDiverseSample(frame, dimensions)
        sampleArray = diverseSample.to_numpy()
        populationArray = frame.to_numpy()

        groups, outliers = clusterizer.clusterizePopulation(sampleArray, populationArray)
        groups = SB.generateGroupsOutput(groups)
        groupsDF = pd.DataFrame(groups)

        #open sample
        sample = pd.read_csv('./datasets/202291/stratified.csv')
        #self.updateSample(frame, sample, groupsDF, dimensions)
        sample = pd.read_csv(self._folderPath + '/sampleUpdated.csv')


        for id, group in groupsDF.iterrows():
            elementsInTheGroup = pd.DataFrame(group['similarProjects'], columns=frame.columns)
            sampleFiltered = sample[sample['id'].isin(elementsInTheGroup['id'])].copy()
            frameFiltered = frame[frame['id'].isin(elementsInTheGroup['id'])].copy()
            frameFiltered = frameFiltered[~frameFiltered['id'].isin(sampleFiltered['id'])]

            quantity = sampleFiltered.shape[0]
            difference = int(group['sampleQty'] - quantity)
            if difference > 0:
                randElem = frameFiltered.sample(difference)
                sample = pd.concat([sample, randElem], ignore_index=True)

                #add
            elif difference < 0:
                #delete
                randElem = sampleFiltered.sample(difference * -1)
                sample = sampleFiltered[~sampleFiltered['id'].isin(randElem['id'])]


        sample.to_csv(self._folderPath + "/sampleUpdated2.csv", index=False)


    def sampleRecategorization2(self, dimensions, frame: pd.DataFrame, ksScore = 0.2):

        sample = pd.read_csv('./datasets/202291/stratified.csv')
        sampleUpdated = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        DS = DiversityScore(frame, dimensions)
        filledSample = SB.createDiverseSample(frame, dimensions, sample=sampleUpdated)
        representative = self.testRepresentativeness(filledSample, frame, dimensions, ksScore)

        while not (representative):
            sampleAux = sampleUpdated.copy()

            diverseSample = SB.createDiverseSample(frame, dimensions)
            sampleArray = diverseSample.to_numpy()
            populationArray = frame.to_numpy()

            groups, outliers = DS.clusterizePopulation(sampleArray, populationArray)
            groups = SB.generateGroupsOutput(groups)
            groupsDF = pd.DataFrame(groups)


            for id, group in groupsDF.iterrows():
                elementsInTheGroup = pd.DataFrame(group['similarProjects'], columns=frame.columns)
                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frame[frame['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameFiltered[~frameFiltered['id'].isin(sampleFiltered['id'])]

                quantity = sampleFiltered.shape[0]
                difference = int(group['sampleQty'] - quantity)
                projDiv = diverseSample.iloc[[id]]

                if difference > 0:
                    if projDiv['id'].item() in sampleAux['id'].values:
                        randElem = frameFiltered.sample(difference)
                    else:
                        randElem = pd.concat([frameFiltered.sample(difference - 1), projDiv], ignore_index=True)
                    sampleAux = pd.concat([sampleAux, randElem], ignore_index=True)
                elif difference < 0:
                    randElem = sampleFiltered.sample(difference * -1)
                    randElem = randElem[projDiv['id'].item() != randElem['id']]
                    sampleAux = sampleAux[~sampleAux['id'].isin(randElem['id'])]

            filledSample = SB.createDiverseSample(frame, dimensions, sample=sampleAux)
            representative = self.testRepresentativeness(filledSample, frame, dimensions, ksScore)

        filledSample.to_csv(self._folderPath + "/sampleUpdated2.csv", index=False)

    def testRepresentativeness (self, sample, population, dimensions, ksScore = 0.2) -> bool:

        for variable in dimensions:
            cdfFrame = stMod.ECDF(population[variable].to_numpy())
            ks = sp.ks_1samp(sample[variable], cdfFrame)
            if ks[1] < ksScore:
                return False

        return True

    def _filterProjects(self):
        dataset = pd.DataFrame(self._df_data)
        dataset = dataset.drop_duplicates(subset=['url'])

        for key, value in self._filters.items():
            dataset = dataset[dataset[key] >= value]
        dataset.to_csv(self._folderPath + "/frame.csv", index=False)




