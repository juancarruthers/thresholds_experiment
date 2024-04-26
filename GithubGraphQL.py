import os
import shutil
import pandas as pd
import datetime
import concurrent.futures
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
            totalRepos = repoCount

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
                    util.saveCheckPoint(self._startSize, self._sizeInc, self._df_data)

                finish = datetime.datetime.now()
                difference = finish - start
                print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)

                dataset = pd.DataFrame(self._df_data)
                dataset = dataset.drop_duplicates(subset=['id'])
                dataset.to_csv(self._folderPath + "/frame.csv", index=False)
                if (os.path.isdir('./.backup')):
                    shutil.rmtree('./.backup')
                print(f"\nTotal number of repositories retrieved in the: {totalRepos}")
                return dataset, totalRepos, difference
        except KeyboardInterrupt:
            self.quit = True

    def extractFrame(self, listRepo:pd.DataFrame = pd.DataFrame(), language='Java'):
        try:
            frame, totalReposWithoutFiltering, duration = self._exploreRepos()

            if listRepo.shape[0] > 0:
                listRepo = listRepo[~listRepo['url'].isin(frame['url'])]
                newDataset = self._getRepoDataByURL(listRepo, language)
                updatedRepos = pd.DataFrame(newDataset)
                frame = pd.concat([frame, updatedRepos])

            frame = frame.drop_duplicates(subset=['id'])
            print(f"Number of projects after filtering: {frame.shape[0]}")
            frame.to_csv(self._folderPath + "/frameUpdated.csv", index=False)

            return frame, totalReposWithoutFiltering, duration

        except KeyboardInterrupt:
            self.quit = True



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

                if not filtersFlag:
                    if language == "*" or repositoryProperties['primaryLanguage'] == language:
                        newDataset.append(repositoryProperties)
                        print(f'{datetime.datetime.now()} - Added: {repositoryProperties["url"]}')

        return newDataset


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
        filters = ['keywords', 'totalSize', 'commits', 'closedIssuesCount', 'pullReqCount', 'dateLastActivity', 'contributors', 'activity']
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
                if filter == 'activity':
                    filterConfig = ActivityFilter({filter: thresholds[filter]})

                filtersConfig.append(filterConfig)
            else:
                print(f"{filter} filter not set")
                exit()

        return filtersConfig