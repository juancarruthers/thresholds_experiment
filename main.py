import os
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import SampleBuilder as SB
import time
from GithubGraphQL import GithubGraphQL as GQL
import statsmodels.distributions.empirical_distribution as stMod
import scipy.stats as sp

def createFrame(queryFilter: str, secondFilter: dict):
    today = datetime.date.today()

    folderPath = "./datasets/" + str(today.year) + str(today.month) + str(today.day)
    if not (os.path.isdir(folderPath)):
        os.mkdir(folderPath)

    projectRetriever = GQL(queryFilter, secondFilter, folderPath)
    projectRetriever.main()

def updateFrame(folderPath: str, queryFilter: str, secondFilter: dict):

    projectUpdater = GQL(queryFilter, secondFilter, folderPath)
    frame = pd.read_csv(folderPath + "/frame.csv")

    projectUpdater.updateFrame(frame)

def createStratifiedSample(folderPath: str, dimensions: list[str]):
    start_time = time.time()
    frame = pd.read_csv(folderPath + "/frame.csv")
    stratified = pd.DataFrame()

    hypRejected = True
    while hypRejected:
        stratified, groups = SB.createStratifiedSample(frame, dimensions)
        for variable in dimensions:
            cdfFrame = stMod.ECDF(frame[variable].to_numpy())
            ks = sp.ks_1samp(stratified[variable], cdfFrame)
            print(variable, ' -Pvalues:', ks[1])
            if ks[1] > 0.25:
                hypRejected = False
            else:
                hypRejected = True
                break

    print("--- %s seconds ---" % (time.time() - start_time))
    stratified.to_csv(folderPath + "/stratified.csv", index=False)

def createSimpleRandomSample(folderPath: str):
    frame = pd.read_csv(folderPath + "/frame.csv")
    simpleRandom = SB.createSimpleRandomSample(frame)
    simpleRandom.to_csv(folderPath + "/simpleRandom.csv", index=False)

def createDiverseSample(folderPath: str, dimensions: list[str]):
    frame = pd.read_csv(folderPath + "/frame.csv")
    diverse = SB.createDiverseSample(frame, dimensions)
    diverse.to_csv(folderPath + "/diverse.csv", index=False)


def createSamples(folderPath: str, dimensions: list[str]):
    createSimpleRandomSample(folderPath)
    createDiverseSample(folderPath, dimensions)
    createStratifiedSample(folderPath, dimensions)

def updateSample(folderPath: str, dimensions: list[str], queryFilter: str, secondFilter: dict):

    frame = pd.read_csv(folderPath + "/frameUpdated.csv")
    sample = pd.read_csv(folderPath + "/stratified.csv")
    groups = pd.read_csv(folderPath + "/groups.csv")
    projectUpdater = GQL(queryFilter, secondFilter, folderPath)

    projectUpdater.updateSample(frame, sample, groups, dimensions)


def scoreSample(samplePath: str, framePath: str, dimensions: list[str]):
    sample = pd.read_csv(samplePath)
    frame = pd.read_csv(framePath)
    dScore, rScore = SB.testSampleDiversityRepresentativeness(sample, frame, dimensions)
    nullHypRej = len(rScore[1])
    print('Diversity Score:', dScore, ' -Null Hypotesis Rejected:', nullHypRej)


def compareWaves():
    dimensions = ['stargazerCount', 'forkCount', 'closedIssuesCount', 'totalSize', 'closedPullReqCount', 'commits']

    frame1 = pd.read_csv("./datasets/2022715/filtered.csv")
    frame2 = pd.read_csv("./datasets/2022823/filtered.csv")
    frame3 = pd.read_csv("./datasets/2022829/filtered.csv")

    for variable in dimensions:
        res1 = sp.ks_2samp(frame1[variable], frame2[variable])
        res2 = sp.ks_2samp(frame2[variable], frame3[variable])
        res3 = sp.ks_2samp(frame1[variable], frame3[variable])
        print('Comparison 1 & 2 -- dimension: ' + variable + ' - test-score:' + str(res1[0]) + ' - p-value:' + str(
            res1[1]))
        print('Comparison 2 & 3 -- dimension: ' + variable + ' - test-score:' + str(res2[0]) + ' - p-value:' + str(
            res2[1]))
        print('Comparison 1 & 3 -- dimension: ' + variable + ' - test-score:' + str(res3[0]) + ' - p-value:' + str(
            res3[1]))

if __name__ == '__main__':
    today = datetime.date.today()
    oneYearAgo = today - relativedelta(years=1)
    oneMonthAgo = today - relativedelta(months=1)

    queryFilter = "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=" + str(oneYearAgo)
    secondFilter = {'totalSize': 10000, 'dateLastCommit': str(oneMonthAgo), 'contributors': 3, 'closedIssuesCount': 50,
                    'closedPullReqCount': 50}

    folderPath = "./datasets/2022913"
    dimensions = ['stargazerCount', 'forkCount', 'closedIssuesCount', 'totalSize', 'closedPullReqCount', 'commits']

    scoreSample(folderPath + "/sampleUpdated.csv", folderPath + "/frame.csv", dimensions)