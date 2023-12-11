import os
import datetime
import numpy as np
import pandas as pd
import psutil
from dateutil.relativedelta import relativedelta
import SampleBuilder as SB
from GithubGraphQL import GithubGraphQL as GQL
import scipy.stats as sp
from scipy.spatial.distance import euclidean, cosine
from Maintenance import Maintenance
import SizeThresholds as ST
from SourceMeter.DatasetGenerator import DatasetGenerator
from SourceMeter.SourceMeter import SourceMeter
from Utilities import Utilities


def createFrame():
    today = datetime.date.today()
    aYearAgo = today - relativedelta(years=1)
    aMonthAgo = today - relativedelta(months=1)

    QUERY_FILTER = f"is:public, language:java, mirror:false, forks:>=10, stars:>=10, created:<={str(aYearAgo)}"
    SECOND_FILTER = {'keywords': ['sample', 'tutorial', 'demo', 'conf', 'exam', 'docs', 'benchmark', 'wiki', 'guide'],
                     'totalSize': 10000, 'commits': 1000,
                     'closedIssuesCount': 50, 'pullReqCount': 50, 'dateLastActivity': str(aMonthAgo), 'contributors': 3,
                     'activity': {'since': str(aYearAgo), 'commits': 1}}


    folderPath = "./datasets/" + str(today.year) + str(today.month) + str(today.day)
    if not (os.path.isdir(folderPath)):
        os.mkdir(folderPath)

    graphql = GQL(QUERY_FILTER, SECOND_FILTER, folderPath)
    graphql.extractFrame(pd.read_csv('./datasets/longStudy/listProj.csv'))


def representativenessCheck(frame, sample, variables, pvalue) -> bool:
    dScore, mwScore, ksScore = SB.testSampleDiversityRepresentativeness(frame, sample, variables, pvalue)
    notRepresentative = False
    if (len(mwScore[1]) > 0) or (len(ksScore[1]) > 0):
        notRepresentative = True

    return notRepresentative

def samplingExperiment(frame: pd.DataFrame, sampleStrategy: str, dimensions: list[str], sampleSize: int) -> pd.DataFrame:

    sample = pd.DataFrame()
    '''
    if sampleStrategy == 'diverse':
        sample = SB.createDiverseSample(frame, dimensions)
    if sampleStrategy == 'stratified':
        sample = SB.createStratifiedSample(frame, dimensions, sampleSize)[0]
    '''
    if sampleStrategy == 'stratifiedKMeans':
        sample = SB.createKMeansSample(frame, sampleSize, dimensions[0])#sample = SB.createKMeansStratifiedSample(frame, dimensions, sampleSize) #7 clusters star experiment
    elif sampleStrategy == 'simpleRandom':
        sample = SB.createSimpleRandomSample(frame, sampleSize)

    return sample


def maintenanceExperiment(newFrame: pd.DataFrame, sample: pd.DataFrame, maintenanceStrategy: str, dimensions: list[str], sampleSize: int) -> pd.DataFrame:
    maintainer = Maintenance(dimensions)
    sampleMaintained = pd.DataFrame()

    if maintenanceStrategy == 'DirectReplacement':
        sampleMaintained = maintainer.updateSample(newFrame, sample, 'DR', sampleSize, **{'nClusters': 6})[0]
    elif maintenanceStrategy == 'DynamicThresholdsKM':
        sampleMaintained = maintainer.updateSample(newFrame, sample, 'DT', sampleSize, **{'nClusters': 6})[0]
    elif maintenanceStrategy == 'Resample':
        sampleMaintained = SB.createKMeansSample(newFrame, sampleSize, dimensions[0])

    return sampleMaintained
def experiment(frame: pd.DataFrame, experimentType: str, strategies: list[str], samplesPath: str, dimensions=['totalSize'], sampleSize = 112, repetitions = 360):
    measureData = pd.DataFrame(columns=['COS', 'EUC', 'EMD', 'VD'])

    for strategy in strategies:
        if not os.path.exists(f"{samplesPath}/{strategy}"):
            os.mkdir(f"{samplesPath}/{strategy}")
        for i in range(repetitions):
            sample = pd.DataFrame()
            if experimentType == 'sampling':
                sample = samplingExperiment(frame, strategy, dimensions, sampleSize)
                sample.to_csv(f"{samplesPath}/{strategy}/s{i}.csv", index=False)
            elif experimentType == 'maintenance':
                oldSample = pd.read_csv(f'./datasets/caseStudy/qualitas/sample.csv')
                sample = maintenanceExperiment(frame, oldSample, strategy, dimensions, sampleSize)
                sample.to_csv(f"{samplesPath}/{strategy}/s{i}.csv", index=False)

            print(f'{strategy} Muestra Nº {i}')

            sampleCharacteristics = SB.characterizeSample(sample, dimensions)
            frameCharacteristics = SB.characterizeSample(frame, dimensions)

            cosineDistance = cosine(sampleCharacteristics, frameCharacteristics)
            euclideanDistance = euclidean(sampleCharacteristics, frameCharacteristics)
            wassDistance = sp.wasserstein_distance(frame[dimensions[0]], sample[dimensions[0]])
            VGEffectSize = SB.varghaDelaney(frame[dimensions[0]], sample[dimensions[0]])

            row = pd.DataFrame({'COS': [cosineDistance], 'EUC': [euclideanDistance], 'EMD': [wassDistance], 'VD': [VGEffectSize]})
            measureData = pd.concat([measureData, row])

        measureData.to_csv(f'{samplesPath}/{strategy}.csv', index=False)
        measureData = pd.DataFrame()

def generateDataset(frame: pd.DataFrame, targetPath: str, dimensions=['totalSize'], type='download'):

    start = datetime.datetime.now()

    sample = pd.read_csv(f'{targetPath}/sample.csv')
    maintainer = Maintenance(dimensions)

    analyzer = SourceMeter('./SourceMeter/SMResults', './SourceMeter/tool/Java/AnalyzerJava.exe', './SourceMeter/repos')
    generator = DatasetGenerator(analyzer, './SourceMeter/repos')
    if type == "download":
        classData, methodData, packageData, remainingOr = generator.generateDataset(sample)
    else:
        classData, methodData, packageData, remainingOr = generator.generateQualitasMetrics(sample)

    classData.to_csv(f'{targetPath}/class.csv', index=False)
    methodData.to_csv(f'{targetPath}/method.csv', index=False)
    packageData.to_csv(f'{targetPath}/package.csv', index=False)

    classData, methodData, packageData, remainingOr = generateDatasetRemainings(remainingOr, classData, methodData, packageData, targetPath)

    if type == 'download':
        i = 0
        remainingOr = remainingOr.reset_index(drop=True)
        remaining = remainingOr.copy()
        newSample = sample[~sample['id'].isin(remaining['id'])]

        while remaining.shape[0] > 0:
            print(f'\n\n-------> Iteration Number {i+1} - Updating {remaining.shape[0]} projects\n\n')
            updates = maintainer.directReplacement(sample, frame, remainingOr, 0)
            updates = updates.reset_index(drop=True)
            updates = updates[updates.index.isin(remaining.index)]

            classData, methodData, packageData, remaining = generateDatasetRemainings(updates, classData, methodData, packageData, targetPath)
            frame = frame[~frame['id'].isin(remaining['id'])]

            replacements = updates[~updates['id'].isin(remaining['id'])]
            newSample = pd.concat([newSample, replacements])
            i += 1

            if remaining.shape[0] == 0:
                util = Utilities()
                classData = util.excludeTestFilesMeasures(classData)
                methodData = util.excludeTestFilesMeasures(methodData)
                outliers = ST.getOutliers(methodData, ['McCC', 'LOC'])
                classData = classData[~classData['Repository'].isin(outliers)]
                methodData = methodData[~methodData['Repository'].isin(outliers)]
                packageData = packageData[~packageData['Repository'].isin(outliers)]

                frame = frame[~frame['id'].isin(remainingOr['id'])]

                remainingOr = newSample[newSample['url'].isin(outliers)]
                sample = newSample.copy()

                i = 0
                remainingOr = remainingOr.reset_index(drop=True)
                remaining = remainingOr.copy()
                newSample = sample[~sample['id'].isin(remaining['id'])]


        classData.to_csv(f'{targetPath}/class.csv', index=False)
        methodData.to_csv(f'{targetPath}/method.csv', index=False)
        packageData.to_csv(f'{targetPath}/package.csv', index=False)
        newSample.to_csv(f'{targetPath}/sample2.csv', index=False)
        frame.to_csv(f'{targetPath}/frame.csv', index=False)

        finish = datetime.datetime.now()
        print('Start:', start, '- Finish:', finish, " -  Time:", finish - start)


def generateDatasetRemainings(remainings, classData, methodData, packageData, targetPath):

    analyzer = SourceMeter('./SourceMeter/SMResults', './SourceMeter/tool/Java/AnalyzerJava.exe', './SourceMeter/repos', 6)
    generator = DatasetGenerator(analyzer, './SourceMeter/repos')
    classRem, methodRem, packageRem, remaining = generator.generateDataset(remainings, 1)


    if classRem.shape[0] > 0:
        classData = pd.concat([classData, classRem])
        methodData = pd.concat([methodData, methodRem])
        packageData = pd.concat([packageData, packageRem])
        classData.to_csv(f'{targetPath}/class.csv', index=False)
        methodData.to_csv(f'{targetPath}/method.csv', index=False)
        packageData.to_csv(f'{targetPath}/package.csv', index=False)

    return classData, methodData, packageData, remaining


if __name__ == '__main__':

    generateDataset(pd.read_csv('./datasets/samMainStudy/20231201.csv'), './datasets/caseStudy/sampleU3')




















