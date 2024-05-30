import shutil
import stat
import pandas as pd
import random
import requests
import time
import os


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

    def writeTxtFile(self, filePath: str, text: str, new=False):
        if new:
            with open(filePath, "w") as file:
                file.write(f'{text}\n')
        else:
            with open(filePath, "a") as file:
                file.write(f'{text}\n')


    def restoreCheckPoint(self, p_sizeInc= 2000, p_startSize=10000) -> tuple[int, int, list]:
        startSize = p_startSize
        sizeInc = p_sizeInc
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
        if df.shape[0] > 0:
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
            elif "Something went wrong while executing your query. Please include" in str(condition):
                if iterations > 2:
                    return response
            time.sleep(random.choice(self._reqSleepTime))
            headers = {'Authorization': 'Bearer ' + random.choice(tokens)}
            response, condition = self._requestCondition(query2, reqType, url, headers)

        return response

    def _requestCondition (self, query: str | dict, reqType: str, url: str, headers: dict) -> tuple[dict, bool]:
        condition: bool
        try:
            if reqType == "POST":
                response = requests.post(url, json=query, headers=headers, timeout=120)
                if response.status_code >= 400:
                    return {'errors': {'message': 'GITHUB: We had issues producing the response to your request!'}}, False
                else:
                    response = response.json()

                condition: bool | list = response.get("errors", False)
                if condition:
                    if 'type' in condition[0]:
                        if condition[0]['type'] == 'NOT_FOUND': return response, False

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

    def deleteFolder(self, folder):
        try:
            if os.path.exists(folder):
                shutil.rmtree(folder)

        except PermissionError as error:
            if error.errno == 13:
                if (os.path.isfile(error.filename)):
                    os.chmod(error.filename, stat.S_IWUSR | stat.S_IREAD)
                self.deleteFolder(folder)
            else:
                print(error)

    def excludeTestFilesMeasures(self, dataset: pd.DataFrame):
        dataset = dataset[dataset['LOC'] > 0]
        dataset = dataset[~dataset['Path'].str.match(r'.*src\\test.*')]
        dataset = dataset[~dataset['Path'].str.match(r'.*src\\tests.*')]

        return dataset










                







