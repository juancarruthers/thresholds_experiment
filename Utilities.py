import concurrent.futures
import os.path
import shutil
import stat
from pathlib import Path

import pandas as pd
import random
import requests
import time
from git import Repo
from SourceMeter.SourceMeter import SourceMeter

class Utilities:

    def __init__(self):
        self._reqSleepTime = [50, 100, 150, 200, 250, 300]
        #self._tokens = self.readFile("token").split(",\n")

    def readFile(self, filePath: str) -> str:
        file = open(filePath).readlines()
        query = ""

        for lines in file:
            query += lines

        return query

    def restoreCheckPoint(self) -> tuple[int, int, list]:
        startSize = 10000
        sizeInc = 2000
        path = './.backup'
        if os.path.isdir(path):
            backupData = pd.read_csv(path + '/largerFrame.csv', encoding='unicode_escape').to_dict('records')
            queryState = pd.read_csv(path + '/queryState.csv')
            return int(queryState.iloc[0, 0]), int(queryState.iloc[0, 1]), backupData
        else:
            return startSize, sizeInc, []

    def saveCheckPoint(self, startSize: int, sizeInc: int, dataset: list[dict]):
        path = "./.backup"
        if not (os.path.isdir(path)):
            os.mkdir(path)

        df = pd.DataFrame(dataset)
        df.to_csv(path + '/largerFrame.csv', index=False)
        queryState = pd.DataFrame([{'startSize': startSize, 'sizeInc': sizeInc}])
        queryState.to_csv(path + "/queryState.csv", index=False)

    def makeRequest (self, query: str | dict, reqType="POST", url='https://api.github.com/graphql') -> dict:
        tokens = self.readFile("token").split("\n")
        headers = {'Authorization': 'Bearer ' + random.choice(tokens)}
        response: dict
        response, condition = self._requestCondition(query, reqType, url, headers)
        iterations = 0

        while condition:
            iterations += 1
            query2 = query
            print(response)
            if "or it could be a GitHub bug" in str(condition):
                variables : dict = query['variables']
                first = round(variables['first']/iterations)
                query2['variables']['first'] = first

            time.sleep(random.choice(self._reqSleepTime))
            headers = {'Authorization': 'Bearer ' + random.choice(tokens)}
            response, condition = self._requestCondition(query2, reqType, url, headers)

        return response

    def _requestCondition (self, query: str | dict, reqType: str, url: str, headers: dict) -> tuple[dict, bool]:
        condition: bool
        try:
            if reqType == "POST":
                response = requests.post(url, json=query, headers=headers, timeout=120).json()
                condition: bool | list = response.get("errors", False)
                if condition and condition[0]['type'] == 'NOT_FOUND':
                    return response, False

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

    def generateDataset(self, dataset: pd.DataFrame, path: str):
        data = pd.DataFrame()
        scanTool = SourceMeter('./SourceMeter/SMResults','./SourceMeter/tool/Java/AnalyzerJava.exe', path)
        repoQuantity = dataset.shape[0]
        step = 10
        for i in range(0, repoQuantity, step):
            set = dataset[dataset.index.isin(range(i, i + step))]
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
            futures = {executor.submit(self._generateRepositoryData, value, path, scanTool) for key, value in set.iterrows()}
            for future in concurrent.futures.as_completed(futures):
                classInfo = future.result()
                if classInfo.shape[0] > 0:
                    data = pd.concat([data, classInfo[['Repository', 'Path', 'CBO', 'DIT', 'LCOM5', 'NOC', 'RFC', 'WMC']]], axis=0)

        return data


    def _generateRepositoryData(self, project, path, scanTool):
        self._cloneRepository(project, path)
        print(f'Downloaded Repository {project["url"]}')
        try:
            scanTool.analyze(project["name"])
            print(f'Scanned Repository {project["url"]}')
            self._deleteFolder(os.path.abspath(f'{path}/{project["name"]}'))

            classMetricsPath = f'{scanTool.getResultsDir()}/{project["name"]}/java/'
            p = Path(classMetricsPath)
            sub = [x for x in p.iterdir() if x.is_dir()]
            metricsData = pd.read_csv(f'{sub[len(sub) - 1]}/{project["name"]}-Class.csv')

            self._deleteFolder(f'{scanTool.getResultsDir()}/{project["name"]}')

            metricsData['Repository'] = project['url']
            return metricsData

        except Exception as error:
            print (error)
            return pd.DataFrame()

    def _deleteFolder(self, folder):
        try:
            shutil.rmtree(folder)

        except Exception as error:
            os.chmod(error.filename, stat.S_IWUSR | stat.S_IREAD)
            self._deleteFolder(folder)


    def _cloneRepository(self, project: pd.DataFrame, clonePath: str):
        #filePath = f'{clonePath}/{project["name"]}.zip'
        #url = f'https://api.github.com/repos/{project["owner"]}/{project["name"]}/zipball'
        filePath = f'{clonePath}/{project["name"]}'
        url = f'https://github.com/{project["owner"]}/{project["name"]}.git'
        tokens = self.readFile("token").split(",\n")
        headers = {'Authorization': 'Bearer ' + random.choice(tokens)}
        try:
            repo = Repo.clone_from(url, os.path.abspath(filePath))
            #r = requests.get(url, headers=headers)

            '''if r.status_code == 200:
                with open(filePath, 'wb') as fh:
                    fh.write(r.content)
            else:
                print(r.text)'''

        except Exception as error:
            print(error)







