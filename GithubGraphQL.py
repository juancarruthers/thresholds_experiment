import math
import random
import shutil
import requests
import pandas as pd
import datetime
import time
import concurrent.futures


from Filters.__init__ import *
from Utilities import Utilities
from dateutil import relativedelta
from github import Github

class GithubGraphQL:

    def __init__(self, queryFilter: str, secondFilter: dict, folderPath: str, p_saveThreshold = 5000, p_itemsPageMainQuery = 30):
        util = Utilities()
        self._startSize, self._sizeInc, self._df_data = util.restoreCheckPoint()
        self._saveThreshold = p_saveThreshold
        self._queryVar = queryFilter + ", size:"
        self._filters = self._setFilters(secondFilter)
        self._queryFile = util.readFile("APIQueries/repositoryMetadata")
        self._repoCountQueryFile = util.readFile("APIQueries/repositoryCount")
        self._elementPerPageMainQuery = p_itemsPageMainQuery
        self._folderPath = folderPath
        self._quit = False

    def main(self):
        try:
            util = Utilities()
            start = datetime.datetime.now()
            repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + ">=" + str(self._startSize)}}
            repoCount = util.makeRequest(repoCountQuery)['data']['search']['repositoryCount']

            if repoCount > 0:
                projectsSaved = 0

                while repoCount > 0:

                    repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                    repoCountSubQuery = util.makeRequest(repoCountQuery)['data']['search']['repositoryCount']

                    j = 1
                    while (repoCountSubQuery >= 1000) | (repoCountSubQuery == 0):
                        if repoCountSubQuery >= 1000:
                            self._sizeInc -= j
                        else:
                            self._sizeInc += j
                        repoCountQuery = {'query': self._repoCountQueryFile, 'variables': {'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}}
                        repoCountSubQuery = util.makeRequest(repoCountQuery)['data']['search']['repositoryCount']
                        j += j

                    repoCount -= repoCountSubQuery
                    cursor = None
                    hasNextPage = True

                    while hasNextPage:
                        variables = {'first': self._elementPerPageMainQuery, 'cursor': cursor, 'query': self._queryVar + str(self._startSize) + ".." + str(self._startSize + self._sizeInc)}
                        repoQuery = {'query': self._queryFile, 'variables': variables}
                        jsonResponse = util.makeRequest(repoQuery)
                        repositories = jsonResponse['data']['search']

                        #PARALELISM
                        executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
                        futures = {executor.submit(self._replaceNestedPropertiesValues, repoProperties) for repoProperties in repositories['edges']}
                        for future in concurrent.futures.as_completed(futures):
                            repositoryProperties, filtersFlag = future.result()
                            if not filtersFlag:
                                self._df_data.append(repositoryProperties)
                                print(f'{datetime.datetime.now()} - Added: {repositoryProperties["url"]}')

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

    def updateFrame(self, listRepo:pd.DataFrame):
        util = Utilities()
        query = util.readFile("APIQueries/repositoryMetadataWID")
        newDataset = excluded = pd.DataFrame(columns=listRepo.columns)

        for id, repo in listRepo.iterrows():
            variables = {'name': repo['name'], 'owner': repo['owner']}
            repoQuery = {'query': query, 'variables': variables}
            jsonResponse = util.makeRequest(repoQuery)['data']['repository']
            self._filters[2].updateFrame(jsonResponse, repo['owner'], repo['name'])
            self._filters[4].updateFrame(jsonResponse, repo['owner'], repo['name'])
            updateFlag = not self._filters[5].updateFrame(jsonResponse, repo['owner'], repo['name'])
            if updateFlag:
                self._filters[1].updateFrame(jsonResponse, repo['owner'], repo['name'])
                self._filters[3].updateFrame(jsonResponse, repo['owner'], repo['name'])
                DFResponse = pd.DataFrame([jsonResponse])
                row = repo.copy().to_frame().T
                row[DFResponse.columns] = DFResponse.values
                newDataset = pd.concat([newDataset, row])
            else:
                excluded = pd.concat([excluded, repo.copy().to_frame().T])







    def _replaceNestedPropertiesValues(self, repoProperties: dict) -> tuple[dict, bool]:
        if not self._quit:

            properties = repoProperties['node']
            owner = properties['owner']['login']
            repositoryName = properties['name']

            filtersFlag = False
            properties["owner"] = owner

            #NORMALIZE OUTPUT
            for filter in self._filters:
                filtersFlag = filter.updateFrame(properties, owner, repositoryName)
                if filtersFlag:
                    break

            return properties, filtersFlag


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