import os
import datetime
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import SampleBuilder as SB
from GithubGraphQL import GithubGraphQL as GQL
import scipy.stats as sp
from Maintenance import Maintenance



def createFrame(queryFilter: str, secondFilter: dict, savethreshold = 5000, p_itemsPageMainQuery = 30):
    today = datetime.date.today()

    folderPath = "./datasets/" + str(today.year) + str(today.month) + str(today.day)
    if not (os.path.isdir(folderPath)):
        os.mkdir(folderPath)

    projectRetriever = GQL(queryFilter, secondFilter, folderPath, p_saveThreshold=savethreshold, p_itemsPageMainQuery=p_itemsPageMainQuery)
    projectRetriever.main()

'''
    stochastic equality is readily seen to be equivalent to the p÷ = .5

    In the continuous case p+ > .5 clearly implies that population 1 is
    stochastically larger than population 2, and p+ < .5 implies that population 1 is
    stochastically smaller than population 2

    Small: 0.56
    Medium: 0.64
    Large: 0.71

    Vargha, A., & Delaney, H. D. (2000). A critique and improvement of the CL common language effect size statistics of McGraw and Wong. Journal of Educational and Behavioral Statistics, 25(2), 101-132.
'''
def varghaDelaney(a, b):
    m = len(a)
    n = len(b)
    U = sp.rankdata(np.concatenate([a, b]))
    R1 = U[:m].sum()
    R2 = U[m:].sum()
    v = (R1/m - (m+1)/2)/n
    return v

def representativenessCheck(frame, sample, variables, pvalue) -> bool:
    dScore, mwScore, ksScore = SB.testSampleDiversityRepresentativeness(frame, sample, variables, pvalue)
    notRepresentative = False
    if (len(mwScore[1]) > 0) or (len(ksScore[1]) > 0):
        notRepresentative = True

    return notRepresentative

def samplingExperiment(frame: pd.DataFrame, sampleStrategy: str, dimensions: list[str], samplesPath: str, sampleSize: int, sigLevel:float) -> pd.DataFrame:

    sample = pd.DataFrame()
    notRepresentative = True
    if sampleStrategy == 'protodash':
        sample = SB.createProtodashSample(frame, dimensions, sampleSize)
    if sampleStrategy == 'diverse':
        sample = SB.createDiverseSample(frame, dimensions)
    if sampleStrategy == 'stratified':
        sample = SB.createStratifiedSample(frame, dimensions, sampleSize)[0]
    if sampleStrategy == 'stratifiedKMeans':
        while notRepresentative:
            sample = SB.createKMeansStratifiedSample(frame, dimensions, sampleSize)
            notRepresentative = representativenessCheck(frame, sample, dimensions, sigLevel)
    if sampleStrategy == 'simpleRandom':
        while notRepresentative:
            sample = SB.createSimpleRandomSample(frame, sampleSize)
            notRepresentative = representativenessCheck(frame, sample, dimensions, sigLevel)

    return sample


def maintenanceExperiment(newFrame: pd.DataFrame, sample: pd.DataFrame, maintenanceStrategy: str, dimensions: list[str], samplesPath: str, sampleSize: int, sigLevel: float) -> pd.DataFrame:
    maintainer = Maintenance(dimensions)
    sampleMaintained = pd.DataFrame()

    if maintenanceStrategy == 'DirectReplacementKM':
        notRepresentative = True
        while notRepresentative:
            sampleMaintained = maintainer.updateSample(newFrame, sample, 'DR', 'kmeans', sampleSize, **{'nClusters': 6})[0]
            notRepresentative = representativenessCheck(newFrame, sampleMaintained, dimensions, sigLevel)
    elif maintenanceStrategy == 'DynamicThresholdsKM':
        notRepresentative = True
        while notRepresentative:
            sampleMaintained = maintainer.updateSample(newFrame, sample, 'DT', 'kmeans', sampleSize, **{'nClusters': 6})[0]
            notRepresentative = representativenessCheck(newFrame, sampleMaintained, dimensions, sigLevel)
    elif maintenanceStrategy == 'UpdateIfAvailable':
        projectsUpdated = newFrame[newFrame['id'].isin(sample['id'])]
        projectsNotUpdated = sample[~sample['id'].isin(newFrame['id'])]
        sampleMaintained = pd.concat([projectsUpdated, projectsNotUpdated])
    elif maintenanceStrategy == 'Resample':
        notRepresentative = True
        while notRepresentative:
            sampleMaintained = SB.createKMeansStratifiedSample(newFrame, dimensions, sampleSize)
            notRepresentative = representativenessCheck(newFrame, sampleMaintained, dimensions, sigLevel)

    if maintenanceStrategy == 'No':
        sampleMaintained = sample.copy()

    return sampleMaintained
def experiment(frame: pd.DataFrame, experimentType: str, strategies: list[str], dimensions: list[str], samplesPath: str, sampleSize = 300, repetitions = 360, sigLevel = 0.05):

    emdDataFrame = pd.DataFrame(columns=dimensions)
    for strategy in strategies:
        for i in range(repetitions):
            sample = pd.DataFrame()
            if experimentType == 'sampling':
                sample = samplingExperiment(frame, strategy, dimensions, samplesPath, sampleSize, sigLevel)
                sample.to_csv(f"{samplesPath}{strategy}/s{i}.csv", index=False)
            elif experimentType == 'maintenance':
                oldSample = pd.read_csv(f'./datasets/samMainStudy/samples/stratifiedKMeans/s{i}.csv')
                sample = maintenanceExperiment(frame, oldSample, strategy, dimensions, samplesPath, sampleSize, sigLevel)
                sample.to_csv(f"{samplesPath}{strategy}/s{i}.csv", index=False)

            print(f'{strategy} Muestra Nº {i}')
            # compute the Earth Mover's Distance (EMD) between the two distributions
            emd = []
            for dimension in dimensions:
                wassDistance = sp.wasserstein_distance(frame[dimension], sample[dimension])
                emd.append(wassDistance)

            emdDataFrameAux = pd.DataFrame(emd).T
            emdDataFrameAux.columns = dimensions
            emdDataFrame = pd.concat([emdDataFrame, emdDataFrameAux])

        emdDataFrame.to_csv(f'{samplesPath}{strategy}.csv', index=False)
        emdDataFrame = pd.DataFrame()


if __name__ == '__main__':
    today = datetime.date.today()
    oneYearAgo = today - relativedelta(years=1)
    oneMonthAgo = today - relativedelta(months=1)

    queryFilter = "is:public, language:java, archived:false, mirror:false, forks:>=10, stars:>=10, created:<=" + str(oneYearAgo)
    secondFilter = {'totalSize': 10000, 'closedIssuesCount': 50, 'pullReqCount': 50, 'commits': 1000,
                    'contributors': 0,  'coreContributors': 0, 'history': 0, 'issueFrequency': 0,
                    'dateLastPullReq': '2010-01-01', 'dateLastCommit': '2010-01-01', 'keywords': ['sample', 'tutorial', 'demo', 'conf', 'exam']}

    #createFrame(queryFilter, secondFilter)

    frame1 = pd.read_csv("./datasets/longStudy/20221004/frame.csv")
    dimensions = ['forkCount', 'stargazerCount', 'totalSize', 'commits', "closedIssuesCount", 'contributors',
                  "mergedPullReqCount", "closedPullReqCount"]
    path = "./datasets/samMainStudy/samples/"
    sampling = ['stratified', 'stratifiedKMeans', 'simpleRandom']
    experiment(frame1, 'sampling', sampling, dimensions, path)

    frame2 = pd.read_csv("./datasets/longStudy/20230301/frame.csv")
    path2 = "./datasets/samMainStudy/maintenance/"
    maintenance = ['DirectReplacementKM', 'DynamicThresholdsKM', 'UpdateIfAvailable', 'Resample', 'No']
    experiment(frame2, 'maintenance', maintenance, dimensions, path2)


    for dimension in dimensions:
        sampleStratified = pd.read_csv("./datasets/samMainStudy/samples/stratified.csv")
        sampleStratifiedKM = pd.read_csv("./datasets/samMainStudy/samples/stratifiedKMeans.csv")
        sampleSimpleRandom = pd.read_csv("./datasets/samMainStudy/samples/simpleRandom.csv")
        mainDRKM = pd.read_csv("./datasets/samMainStudy/maintenance/DirectReplacementKM.csv")
        mainDTKM = pd.read_csv("./datasets/samMainStudy/maintenance/DynamicThresholdsKM.csv")
        mainUpdateIfAv = pd.read_csv("./datasets/samMainStudy/maintenance/UpdateIfAvailable.csv")
        mainResample = pd.read_csv("./datasets/samMainStudy/maintenance/Resample.csv")
        mainNoUpdate = pd.read_csv("./datasets/samMainStudy/maintenance/No.csv")
        print(dimension)
        print(f"Dynamic Thresholds: {sp.mannwhitneyu(mainResample[dimension], mainDTKM[dimension], alternative='less', method='auto')}")
        print(f"Direct Replacement: {sp.mannwhitneyu(mainResample[dimension], mainDRKM[dimension], alternative='less', method='auto')}")
        print(f"Diferencia: {sp.mannwhitneyu(mainDTKM[dimension], mainDRKM[dimension], alternative='less', method='auto')}\n")

















