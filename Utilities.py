import os.path
import pandas as pd

def readFile(filePath: str) -> str:
    file = open(filePath).readlines()
    query = ""

    for lines in file:
        query += lines

    return query

def restoreCheckPoint() -> tuple[int, int, list]:
    startSize = 10000
    sizeInc = 2000
    path = './.backup'
    if os.path.isdir(path):
        backupData = pd.read_csv(path + '/largerFrame.csv', encoding='unicode_escape').to_dict('records')
        queryState = pd.read_csv(path + '/queryState.csv')
        return int(queryState.iloc[0, 0]), int(queryState.iloc[0, 1]), backupData
    else:
        return startSize, sizeInc, []

def saveCheckPoint(startSize: int, sizeInc: int, dataset: list[dict]):
    path = "./.backup"
    if not (os.path.isdir(path)):
        os.mkdir(path)

    df = pd.DataFrame(dataset)
    df.to_csv(path + '/largerFrame.csv', index=False)
    queryState = pd.DataFrame([{'startSize': startSize, 'sizeInc': sizeInc}])
    queryState.to_csv(path + "/queryState.csv", index=False)