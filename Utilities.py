import concurrent.futures
import os.path
import pandas as pd
import random
import requests
import time
from git import Repo, Git


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
        tokens = self.readFile("token").split(",\n")
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
                first = round(variables['first']/(2*iterations))
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

    def downloadDataset(self, dataset: pd.DataFrame, path: str):
        repoQuantity = dataset.shape[0]
        step = 10
        for i in range(0, repoQuantity, step):
            set = dataset[dataset.index.isin(range(i, i + step))]
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
            futures = {executor.submit(self._cloneRepositories, value, path) for key, value in set.iterrows()}
            for future in concurrent.futures.as_completed(futures):
                repo = future.result()
                print(f'Downloaded Repository {repo}')

    def _cloneRepositories(self, project: pd.DataFrame, clonePath: str):
        filePath = f'{clonePath}/{project["name"]}.zip'
        url = f'https://api.github.com/repos/{project["owner"]}/{project["name"]}/zipball'
        tokens = self.readFile("token").split(",\n")
        headers = {'Authorization': 'Bearer ' + random.choice(tokens)}
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            with open(filePath, 'wb') as fh:
                fh.write(r.content)
        else:
            print(r.text)

        return project['url']

