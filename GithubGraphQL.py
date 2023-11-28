import os
import shutil
import pandas as pd
import datetime
import concurrent.futures

from scipy.spatial.distance import euclidean

from Filters.__init__ import *
from Utilities import Utilities

class GithubGraphQL:

    def __init__(self, queryFilter: str, secondFilter: dict, folderPath: str, p_itemsPageMainQuery = 30):
        util = Utilities()
        self._startSize, self._sizeInc, self._df_data = util.restoreCheckPoint()
        if queryFilter != "": self._queryVar = queryFilter + ", size:"
        else: self._queryVar = "size:"
        self._filters = self._setFilters(secondFilter)
        self._queryFile = util.readFile("APIQueries/repositoryMetadata")
        self._repoCountQueryFile = util.readFile("APIQueries/repositoryCount")
        self._elementPerPageMainQuery = p_itemsPageMainQuery
        self._folderPath = folderPath
        self._quit = False

    def _exploreRepos(self):
        try:
            util = Utilities()
            start = datetime.datetime.now()
            repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + ">=" + str(self._startSize)}}
            repoCount = util.makeRequest(repoCountQuery)['data']['search']['repositoryCount']

            if repoCount > 0:

                while repoCount > 0:

                    repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                    repoCountSubQuery = util.makeRequest(repoCountQuery)['data']['search']['repositoryCount']

                    j = 1
                    k = 0
                    while (repoCountSubQuery >= 1000) | (repoCountSubQuery == 0):
                        if repoCountSubQuery >= 1000:
                            self._sizeInc -= j
                        else:
                            self._sizeInc += j
                        repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                        repoCountSubQuery = util.makeRequest(repoCountQuery)['data']['search']['repositoryCount']
                        j += j
                        k += 1
                        if k > 100:
                            repoCount = 0
                            break

                    util.saveCheckPoint(self._startSize, self._sizeInc, self._df_data)
                    repoCount -= repoCountSubQuery
                    hasNextPage = repoCount > 0
                    cursor = None

                    while hasNextPage:
                        variables = {'first': self._elementPerPageMainQuery, 'cursor': cursor, 'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}
                        repoQuery = {'query': self._queryFile, 'variables': variables}
                        jsonResponse = util.makeRequest(repoQuery)
                        repositories = jsonResponse['data']['search']

                        #PARALELISM
                        executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
                        futures = {executor.submit(self._replaceNestedPropertiesValues, repoProperties['node']) for repoProperties in repositories['edges']}
                        for future in concurrent.futures.as_completed(futures):
                            repositoryProperties, filtersFlag = future.result()
                            if not filtersFlag:
                                self._df_data.append(repositoryProperties)
                                print(f'{datetime.datetime.now()} - Added: {repositoryProperties["url"]}')

                        hasNextPage = repositories['pageInfo']['hasNextPage']
                        if hasNextPage:
                            cursor = repositories['pageInfo']['endCursor']

                    self._startSize += self._sizeInc

                finish = datetime.datetime.now()
                difference = finish - start
                print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)

                dataset = pd.DataFrame(self._df_data)
                dataset = dataset.drop_duplicates(subset=['id'])
                dataset.to_csv(self._folderPath + "/frame.csv", index=False)
                if (os.path.isdir('./.backup')):
                    shutil.rmtree('./.backup')
                return dataset
        except KeyboardInterrupt:
            self.quit = True

    def extractFrame(self, listRepo:pd.DataFrame, language='Java'):
        try:
            frame = self._exploreRepos()
            listRepo = listRepo[~listRepo['url'].isin(frame['url'])]

            newDataset = self._getRepoDataByURL(listRepo, language)

            '''
            newDataset = []
            chunkSize = 30

            for chunk_start in range(0, listRepo.shape[0], chunkSize):
                chunk_end = chunk_start + chunkSize
                chunk = listRepo.iloc[chunk_start:chunk_end]

                # PARALELISM
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
                futures = {executor.submit(self._getRepoDataByName, _, repo) for _, repo in chunk.iterrows()}
                for future in concurrent.futures.as_completed(futures):
                    repositoryProperties, filtersFlag = future.result()
                    if not filtersFlag and repositoryProperties['primaryLanguage'] == language:
                        newDataset.append(repositoryProperties)
                        print(f'{datetime.datetime.now()} - Added: {repositoryProperties["url"]}')
            '''

            updatedRepos = pd.DataFrame(newDataset)
            frame = pd.concat([frame, updatedRepos])
            print(f"Number of projects in the dataset: {frame.shape[0]}")
            frame = frame.drop_duplicates(subset=['id'])
            print(f"Without duplicates: {frame.shape[0]}")
            frame.to_csv(self._folderPath + "/frameUpdated.csv", index=False)
        except KeyboardInterrupt:
            self.quit = True


    def updateSample(self, sample: pd.DataFrame, language='Java'):

        updatedDataset = pd.DataFrame(self._getRepoDataByURL(sample, language))

        if sample.shape[0] > updatedDataset.shape[0]:
            toUpdate = sample[~sample['id'].isin(updatedDataset['id'])]
            for id, project in toUpdate.iterrows():
                similar = self.getsimilarProjects(project['totalSize']/1024, sample)
                updatedDataset = pd.concat([updatedDataset, similar])

        return updatedDataset

    def _getRepoDataByURL(self, listRepo:pd.DataFrame, language='Java'):

        newDataset = []
        chunkSize = 30

        for chunk_start in range(0, listRepo.shape[0], chunkSize):
            chunk_end = chunk_start + chunkSize
            chunk = listRepo.iloc[chunk_start:chunk_end]

            # PARALELISM
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
            futures = {executor.submit(self._getRepoDataByName, _, repo) for _, repo in chunk.iterrows()}
            for future in concurrent.futures.as_completed(futures):
                repositoryProperties, filtersFlag = future.result()
                if language == "*": languageFlag = True
                else: languageFlag = repositoryProperties['primaryLanguage'] == language
                if not filtersFlag and languageFlag:
                    newDataset.append(repositoryProperties)
                    print(f'{datetime.datetime.now()} - Added: {repositoryProperties["url"]}')

        return newDataset


    def getsimilarProjects(self, totalSize:int, sample):
        util = Utilities()
        candidates = pd.DataFrame()
        delta = 5000
        jsonResponse = {}

        while len(jsonResponse) == 0:
            variables = {'first': self._elementPerPageMainQuery, 'query': f"{self._queryVar}{totalSize - delta}..{totalSize + delta}"}
            repoQuery = {'query': self._queryFile, 'variables': variables}
            jsonResponse = util.makeRequest(repoQuery)
            delta = delta + delta

            if len(jsonResponse) > 0:
                repositories = jsonResponse['data']['search']
                for repoProperties in repositories['edges']:

                    repositoryProperties, filtersFlag = self._replaceNestedPropertiesValues(repoProperties['node'])
                    if not filtersFlag:
                        repositoryProperties['distance'] = euclidean(totalSize, repositoryProperties['totalSize'])
                        candidates = pd.concat([candidates, repositoryProperties])

                candidates = candidates[~candidates['id'].isin(sample['id'])]

        candidates = candidates.sort_values(by='distance', ascending=True)
        return candidates.iloc[0]

    def _getRepoDataByName(self, _, repo: pd.DataFrame):
        util = Utilities()
        variables = {'name': repo['name'], 'owner': repo['owner']}
        query = util.readFile("APIQueries/repositoryMetadataWID")
        repoQuery = {'query': query, 'variables': variables}
        jsonResponse = util.makeRequest(repoQuery)['data']['repository']
        if jsonResponse != None:
            jsonResponse, filtersFlag = self._replaceNestedPropertiesValues(jsonResponse)
        else:
            return jsonResponse, True

        return jsonResponse, filtersFlag


    def _replaceNestedPropertiesValues(self, repoProperties: dict) -> tuple[dict, bool]:
        if not self._quit:

            owner = repoProperties['owner']['login']
            repositoryName = repoProperties['name']

            filtersFlag = False
            repoProperties["owner"] = owner

            #NORMALIZE OUTPUT
            for filter in self._filters:
                filtersFlag = filter.updateFrame(repoProperties, owner, repositoryName)
                if filtersFlag:
                    break

            return repoProperties, filtersFlag


    def _setFilters(self, thresholds: dict) -> list[GraphqlFilter]:
        filters = ['keywords', 'totalSize', 'commits', 'closedIssuesCount', 'pullReqCount', 'dateLastActivity', 'contributors']
        filtersConfig = []
        for filter in filters:
            filterConfig = GraphqlFilter
            if filter in thresholds:
                if filter == 'keywords':
                    filterConfig = KeywordsFilter({filter: thresholds[filter]})
                if filter == 'totalSize':
                    filterConfig = SizeFilter({filter: thresholds[filter]})
                if filter == 'commits':
                    filterConfig = CommitFilter({filter: thresholds[filter]})
                if filter == 'closedIssuesCount':
                    filterConfig = IssuesFilter({filter: thresholds[filter]})
                if filter == 'pullReqCount':
                    filterConfig = PullReqFilter({filter: thresholds[filter]})
                if filter == 'dateLastActivity':
                    filterConfig = RecentActivityFilter({filter: thresholds[filter]})
                if filter == 'contributors':
                    filterConfig = ContributorsFilter({filter: thresholds[filter]})

                filtersConfig.append(filterConfig)
            else:
                print(f"{filter} filter not set")
                exit()

        return filtersConfig