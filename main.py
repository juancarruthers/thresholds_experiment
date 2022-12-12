import os
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import SampleBuilder as SB
import time
from GithubGraphQL import GithubGraphQL as GQL
import statsmodels.distributions.empirical_distribution as stMod
import scipy.stats as sp
from Maintenance import Maintenance

def createFrame(queryFilter: str, secondFilter: dict, savethreshold = 5000, p_itemsPageMainQuery = 30):
    today = datetime.date.today()

    folderPath = "./datasets/" + str(today.year) + str(today.month) + str(today.day)
    if not (os.path.isdir(folderPath)):
        os.mkdir(folderPath)

    projectRetriever = GQL(queryFilter, secondFilter, folderPath, p_saveThreshold=savethreshold, p_itemsPageMainQuery=p_itemsPageMainQuery)
    projectRetriever.main()

def updateFrame(folderPath: str, queryFilter: str, secondFilter: dict, dimensions: list[str]):

    projectUpdater = GQL(queryFilter, secondFilter, folderPath)
    maintenance = Maintenance(projectUpdater, dimensions, secondFilter['dateLastCommit'])
    frame = pd.read_csv(folderPath + "/frame.csv")

    frame = maintenance.updateFrame(frame)

    frame.to_csv(folderPath + "/frameUpdated.csv", index=False)
    return frame

def createStratifiedSample(folderPath: str, dimensions: list[str], sampleSize: int, ksScore = 0.05):
    start_time = time.time()
    frame = pd.read_csv(folderPath + "/frame.csv")
    stratified = pd.DataFrame()

    hypRejected = True
    while hypRejected:
        stratified, groups = SB.createStratifiedSample(frame, dimensions, sampleSize)
        for variable in dimensions:
            cdfFrame = stMod.ECDF(frame[variable].to_numpy())
            ks = sp.ks_1samp(stratified[variable], cdfFrame)
            print(variable, ' -Pvalues:', ks[1])
            if ks[1] > ksScore:
                hypRejected = False
            else:
                hypRejected = True
                break

    print("--- %s seconds ---" % (time.time() - start_time))
    stratified.to_csv(folderPath + "/stratified.csv", index=False)
    proportion = stratified.shape[0] / frame.shape[0]
    groupDF = pd.DataFrame(SB.generateGroupsOutput(groups, proportion))
    groupDF.to_csv(folderPath + "/groups.csv", index=False)

def createSimpleRandomSample(folderPath: str, sampleSize: int):
    frame = pd.read_csv(folderPath + "/frame.csv")
    simpleRandom = SB.createSimpleRandomSample(frame, sampleSize)
    simpleRandom.to_csv(folderPath + "/simpleRandom.csv", index=False)

def createDiverseSample(folderPath: str, dimensions: list[str]):
    frame = pd.read_csv(folderPath + "/frame.csv")
    diverse = SB.createDiverseSample(frame, dimensions)
    diverse.to_csv(folderPath + "/diverse.csv", index=False)


def createSamples(folderPath: str, dimensions: list[str]):
    createSimpleRandomSample(folderPath)
    createDiverseSample(folderPath, dimensions)
    createStratifiedSample(folderPath, dimensions)

def updateSampleDTDQ(frame: pd.DataFrame, sample: pd.DataFrame, folderPath: str, dimensions: list[str], queryFilter: str, secondFilter: dict, ksScore= 0.2):
    projectUpdater = GQL(queryFilter, secondFilter, folderPath)
    maintenance = Maintenance(projectUpdater, dimensions, secondFilter['dateLastCommit'])
    sampleDTDQ = maintenance.updateSampleDTDQ(frame, sample, ksScore)
    sampleDTDQ.to_csv(folderPath + "/sampleUpdatedDTDQ.csv", index=False)

def updateSampleSTDQ(frame: pd.DataFrame, sample: pd.DataFrame, groups: pd.DataFrame, folderPath: str, dimensions: list[str], queryFilter: str, secondFilter: dict, ksScore= 0.2):
    projectUpdater = GQL(queryFilter, secondFilter, folderPath)
    maintenance = Maintenance(projectUpdater, dimensions, secondFilter['dateLastCommit'])
    sampleSTDQ = maintenance.updateSampleST(frame, sample, groups, ksScore, 'dynamic')
    sampleSTDQ.to_csv(folderPath + "/sampleUpdatedSTDQ.csv", index=False)

def updateSampleSTSQ(frame: pd.DataFrame, sample: pd.DataFrame, groups: pd.DataFrame, folderPath: str, dimensions: list[str], queryFilter: str, secondFilter: dict, ksScore=0.2):
    projectUpdater = GQL(queryFilter, secondFilter, folderPath)
    maintenance = Maintenance(projectUpdater, dimensions, secondFilter['dateLastCommit'])
    sampleSTSQ = maintenance.updateSampleST(frame, sample, groups, ksScore, 'static')
    sampleSTSQ.to_csv(folderPath + "/sampleUpdatedSTSQ.csv", index=False)


def scoreSample(samplePath: str, framePath: str, dimensions: list[str]):
    sample = pd.read_csv(samplePath)
    frame = pd.read_csv(framePath)
    dScore, rScore = SB.testSampleDiversityRepresentativeness(sample, frame, dimensions)
    nullHypRej = len(rScore[1])

    print('Diversity Score:', dScore)
    for variable in rScore[0]:
        print(variable['dimension'] + ' - p-value: ' + str(variable['p-value']))


def compareWaves():
    dimensions = ['stargazerCount', 'forkCount', 'closedIssuesCount', 'totalSize', 'closedPullReqCount', 'commits']

    frame1 = pd.read_csv("./datasets/20220715/stratified.csv")
    frame2 = pd.read_csv("./datasets/20220901/sampleUpdated.csv")
    frame3 = pd.read_csv("./datasets/20220913/sampleUpdated.csv")

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

    queryFilter = "is:public, language:java, archived:false, mirror:false, forks:>=10, stars:>=10, created:<=" + str(oneYearAgo)
    secondFilter = {'totalSize': 10000, 'closedIssuesCount': 50, 'pullReqCount': 50, 'commits': 1000,
                    'contributors': 0,  'coreContributors': 0, 'history': 0, 'issueFrequency': 0,
                    'dateLastPullReq': '2010-01-01', 'dateLastCommit': '2010-01-01'}

    folderPath = "./datasets/prueba"
    dimensions = ['stargazerCount', 'forkCount', 'totalSize', 'commits', 'closedIssuesCount', 'contributors']

    frame = pd.read_csv(folderPath + '/frame.csv')
    #sampleFolder = './datasets/20220913'
    #groups = pd.read_csv('./datasets/20220715/groups.csv')

    createStratifiedSample(folderPath, dimensions, 293)
    #createSimpleRandomSample(folderPath, 293)
    #createDiverseSample(folderPath, dimensions)
    #scoreSample(folderPath + '/simpleRandom.csv', folderPath + '/frame.csv', dimensions)
    #scoreSample(folderPath + '/diverse.csv', folderPath + '/frame.csv', dimensions)
    #scoreSample(folderPath + '/stratified.csv', folderPath + '/frame.csv', dimensions)

    #createFrame(queryFilter, secondFilter)

    '''
    createFrame(queryFilter, secondFilter)

    
    #updateSamples(frame, sampleFolder, groups, folderPath, dimensions, queryFilter, secondFilter, 0.05)
    updateSampleDTDQ(frame, pd.read_csv(sampleFolder + '/sampleUpdatedDTDQ.csv'), folderPath, dimensions, queryFilter, secondFilter, 0.3)
    updateSampleSTDQ(frame, pd.read_csv(sampleFolder + '/sampleUpdatedSTDQ.csv'), groups, folderPath, dimensions, queryFilter, secondFilter, 0.05)
    updateSampleSTSQ(frame, pd.read_csv(sampleFolder + '/sampleUpdatedSTSQ.csv'), groups, folderPath, dimensions, queryFilter, secondFilter, 0.05)
    

    folderPath2 = "./datasets/longStudy"
    sample = folderPath + '/simpleRandom.csv'

    scoreSample(sample, folderPath + '/frame.csv', dimensions)
    scoreSample(sample, folderPath2 + '/20221004/frame.csv', dimensions)
    scoreSample(sample, folderPath2 + '/20221017/frame.csv', dimensions)
    scoreSample(sample, folderPath2 + '/20221101/frame.csv', dimensions)
    scoreSample(sample, folderPath2 + '/20221115/frame.csv', dimensions)
    scoreSample(sample, folderPath2 + '/20221206/frame.csv', dimensions)
    '''




