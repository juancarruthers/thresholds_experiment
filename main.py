import os
import datetime
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
#import SampleBuilder as SB
from GithubGraphQL import GithubGraphQL as GQL
import scipy.stats as sp

from Utilities import Utilities


#from Maintenance import Maintenance



def createFrame(queryFilter: str, secondFilter: dict, p_itemsPageMainQuery = 30):
    today = datetime.date.today()

    folderPath = "./datasets/" + str(today.year) + str(today.month) + str(today.day)
    if not (os.path.isdir(folderPath)):
        os.mkdir(folderPath)

    projectRetriever = GQL(queryFilter, secondFilter, folderPath, p_itemsPageMainQuery=p_itemsPageMainQuery)
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

def samplingExperiment(frame: pd.DataFrame, sampleStrategy: str, dimensions: list[str], sampleSize: int) -> pd.DataFrame:

    sample = pd.DataFrame()
    if sampleStrategy == 'protodash':
        sample = SB.createProtodashSample(frame, dimensions, sampleSize)
    if sampleStrategy == 'diverse':
        sample = SB.createDiverseSample(frame, dimensions)
    if sampleStrategy == 'stratified':
        sample = SB.createStratifiedSample(frame, dimensions, sampleSize)[0]
    if sampleStrategy == 'stratifiedKMeans':
        sample = SB.createKMeansStratifiedSample(frame, dimensions, sampleSize) #7 clusters star experiment
    if sampleStrategy == 'simpleRandom':
        sample = SB.createSimpleRandomSample(frame, sampleSize)


    return sample


def maintenanceExperiment(newFrame: pd.DataFrame, sample: pd.DataFrame, maintenanceStrategy: str, dimensions: list[str], sampleSize: int) -> pd.DataFrame:
    maintainer = Maintenance(dimensions)
    sampleMaintained = pd.DataFrame()

    if maintenanceStrategy == 'DirectReplacementKM':
        sampleMaintained = maintainer.updateSample(newFrame, sample, 'DR', 'kmeans', sampleSize, **{'nClusters': 6})[0]

    elif maintenanceStrategy == 'DynamicThresholdsKM':
        sampleMaintained = maintainer.updateSample(newFrame, sample, 'DT', 'kmeans', sampleSize, **{'nClusters': 6})[0]
    elif maintenanceStrategy == 'UpdateIfAvailable':
        projectsUpdated = newFrame[newFrame['id'].isin(sample['id'])]
        projectsNotUpdated = sample[~sample['id'].isin(newFrame['id'])]
        sampleMaintained = pd.concat([projectsUpdated, projectsNotUpdated])
    elif maintenanceStrategy == 'Resample':
        sampleMaintained = SB.createKMeansStratifiedSample(newFrame, dimensions, sampleSize)


    if maintenanceStrategy == 'No':
        sampleMaintained = sample.copy()

    return sampleMaintained
def experiment(frame: pd.DataFrame, experimentType: str, strategies: list[str], dimensions: list[str], samplesPath: str, sampleSize = 300, repetitions = 360):

    emdDataFrame = pd.DataFrame(columns=dimensions)
    for strategy in strategies:
        for i in range(repetitions):
            sample = pd.DataFrame()
            if experimentType == 'sampling':
                sample = samplingExperiment(frame, strategy, dimensions, sampleSize)
                sample.to_csv(f"{samplesPath}{strategy}/s{i}.csv", index=False)
            elif experimentType == 'maintenance':
                oldSample = pd.read_csv(f'./datasets/samMainStudy/samples/stratifiedKMeans/s{i}.csv')
                sample = maintenanceExperiment(frame, oldSample, strategy, dimensions, sampleSize)
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


    util = Utilities()
    data = pd.read_csv('./datasets/samMainStudy/samples/stratifiedKMeans/s0.csv')
    dataset = util.generateDataset(data, "../sm/SM/repos")

    dataset.to_csv('./datasets/caseStudy/downloadTestSet/metrics.csv', index=False)





















