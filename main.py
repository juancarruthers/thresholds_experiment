import os
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import SampleBuilder as SB
from GithubGraphQL import GithubGraphQL as GQL
from Maintenance import Maintenance
from SourceMeter.DatasetGenerator import DatasetGenerator
from SourceMeter.SourceMeter import SourceMeter
from Utilities import Utilities


def createFrame():
    today = datetime.date.today()
    aYearAgo = today - relativedelta(years=1)
    aMonthAgo = today - relativedelta(months=1)

    QUERY_FILTER = f"is:public, language:java, mirror:false, forks:>=10, stars:>=10, created:<={str(aYearAgo)}"
    SECOND_FILTER = {'keywords': ['sample', 'tutorial', 'demo', 'conf', 'exam', 'docs', 'benchmark', 'wiki', 'guide', 'template'],
                     'totalSize': 10000, 'commits': 1000,
                     'closedIssuesCount': 50, 'pullReqCount': 50, 'dateLastActivity': str(aMonthAgo), 'contributors': 3,
                     'activity': {'since': str(aYearAgo), 'commits': 1}}


    folderPath = "./datasets/" + str(today.year) + str(today.month) + str(today.day)
    if not (os.path.isdir(folderPath)):
        os.mkdir(folderPath)

    graphql = GQL(QUERY_FILTER, SECOND_FILTER, folderPath)
    frame, totalReposWithoutFiltering, duration = graphql.extractFrame()

    return frame, totalReposWithoutFiltering, duration


def generateDataset(frame: pd.DataFrame, targetPath: str, toolPath: str, reposPath: str, dimensions=['totalSize'], type='download'):

    start = datetime.datetime.now()

    analyzer = SourceMeter(f'{toolPath}/../SMResults', f'{toolPath}/Java/AnalyzerJava.exe', reposPath)
    generator = DatasetGenerator(analyzer, reposPath)
    if type == "download":
        sample = pd.read_csv(f'{targetPath}/sample.csv')
        classData, methodData, packageData, remainingOr = generator.generateDataset(sample)
    else:
        classData, methodData, packageData, remainingOr = generator.generateQualitasMetrics()

    classData.to_csv(f'{targetPath}/class.csv', index=False)
    methodData.to_csv(f'{targetPath}/method.csv', index=False)
    packageData.to_csv(f'{targetPath}/package.csv', index=False)

    if type == 'download':
        classData, methodData, packageData, remainingOr = generateDatasetRemainings(remainingOr, classData, methodData,
                                                                                    packageData, targetPath, toolPath,
                                                                                    reposPath)
        frame = frame[~frame['id'].isin(remainingOr['id'])]
        maintainer = Maintenance(dimensions)
        i = 0
        remainingOr = remainingOr.reset_index(drop=True)
        remaining = remainingOr.copy()
        newSample = sample[~sample['id'].isin(remaining['id'])]

        while remaining.shape[0] > 0:
            print(f'\n\n-------> Iteration Number {i+1} - Updating {remaining.shape[0]} projects\n\n')
            updates = maintainer.directReplacement(sample, frame, remainingOr)
            updates = updates.reset_index(drop=True)
            updates = updates[updates.index.isin(remaining.index)]

            classData, methodData, packageData, remaining = generateDatasetRemainings(updates, classData, methodData, packageData, targetPath, toolPath, reposPath)
            frame = frame[~frame['id'].isin(remaining['id'])]

            replacements = updates[~updates['id'].isin(remaining['id'])]
            newSample = pd.concat([newSample, replacements])
            i += 1

        classData.to_csv(f'{targetPath}/class.csv', index=False)
        methodData.to_csv(f'{targetPath}/method.csv', index=False)
        packageData.to_csv(f'{targetPath}/package.csv', index=False)
        newSample.to_csv(f'{targetPath}/sample.csv', index=False)
        frame.to_csv(f'{targetPath}/../frame.csv', index=False)

        finish = datetime.datetime.now()
        print('Start:', start, '- Finish:', finish, " -  Time:", finish - start)

        return (finish - start)


def generateDatasetRemainings(remainings, classData, methodData, packageData, targetPath, toolPath: str, reposPath: str):

    analyzer = SourceMeter(f'{toolPath}/../SMResults', f'{toolPath}/Java/AnalyzerJava.exe', reposPath, 6)
    generator = DatasetGenerator(analyzer, reposPath)
    classRem, methodRem, packageRem, remaining = generator.generateDataset(remainings, 1)

    if classRem.shape[0] > 0:
        classData = pd.concat([classData, classRem])
        methodData = pd.concat([methodData, methodRem])
        packageData = pd.concat([packageData, packageRem])
        classData.to_csv(f'{targetPath}/class.csv', index=False)
        methodData.to_csv(f'{targetPath}/method.csv', index=False)
        packageData.to_csv(f'{targetPath}/package.csv', index=False)

    return classData, methodData, packageData, remaining

def replaceOutliers(frame: pd.DataFrame, targetPath: str, toReplace: list[str], toolPath: str, reposPath: str, dimensions=['totalSize']):
    if len(toReplace) == 0:
        print(f'Define the repositories to replace')
        return

    start = datetime.datetime.now()

    sample = pd.read_csv(f'{targetPath}/sample.csv')
    maintainer = Maintenance(dimensions)

    classData = pd.read_csv(f'{targetPath}/class.csv')
    methodData = pd.read_csv(f'{targetPath}/method.csv')
    packageData = pd.read_csv(f'{targetPath}/package.csv')
    remainingOr = sample[sample['url'].isin(toReplace)]
    if remainingOr.shape[0] == 0:
        print(f'The outliers are not in the sample')
        return

    frame = frame[~frame['url'].isin(toReplace)]

    classData = classData[~classData['Repository'].isin(remainingOr['url'])]
    methodData = methodData[~methodData['Repository'].isin(remainingOr['url'])]
    packageData = packageData[~packageData['Repository'].isin(remainingOr['url'])]

    i = 0
    remainingOr = remainingOr.reset_index(drop=True)
    remaining = remainingOr.copy()
    newSample = sample[~sample['id'].isin(remaining['id'])]

    while remaining.shape[0] > 0:
        print(f'\n\n-------> Iteration Number {i + 1} - Updating {remaining.shape[0]} projects\n\n')
        updates = maintainer.directReplacement(sample, frame, remainingOr)
        updates = updates.reset_index(drop=True)
        updates = updates[updates.index.isin(remaining.index)]

        classData, methodData, packageData, remaining = generateDatasetRemainings(updates, classData, methodData,
                                                                                  packageData, targetPath, toolPath, reposPath)
        frame = frame[~frame['id'].isin(remaining['id'])]

        replacements = updates[~updates['id'].isin(remaining['id'])]
        newSample = pd.concat([newSample, replacements])
        i += 1

    classData.to_csv(f'{targetPath}/class.csv', index=False)
    methodData.to_csv(f'{targetPath}/method.csv', index=False)
    packageData.to_csv(f'{targetPath}/package.csv', index=False)
    newSample.to_csv(f'{targetPath}/sample.csv', index=False)
    frame.to_csv(f'{targetPath}/../frame.csv', index=False)

    finish = datetime.datetime.now()
    print(f'Start: {start} - Finish: {finish} -  Time:{finish - start}\n\n')


if __name__ == '__main__':

    DATASETS_ROOT_PATH = './datasets/test'

    CURRENT_SAMPLE_PATH = f'{DATASETS_ROOT_PATH}/currentSample'
    QUALITAS_PATH = f'{DATASETS_ROOT_PATH}/qualitas'
    QUALITAS_UPDATED_PATH = f'{DATASETS_ROOT_PATH}/qualitasUpdated'

    SAMPLING_FRAME_PATH = f'{DATASETS_ROOT_PATH}/frame.csv'
    EXECUTION_REPORT_PATH = f'{DATASETS_ROOT_PATH}/executionReport.txt'

    SOURCEMETER_PATH = './SourceMeter/tool'
    REPO_DOWNLOAD_PATH = f'{SOURCEMETER_PATH}/../repos'

    paths = [DATASETS_ROOT_PATH, CURRENT_SAMPLE_PATH, QUALITAS_PATH, QUALITAS_UPDATED_PATH, REPO_DOWNLOAD_PATH]

    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)

    util = Utilities()
    util.writeTxtFile(EXECUTION_REPORT_PATH, f'Execution date: {datetime.datetime.now()}', True)

    # Retrieve a sampling frame according to a specific quality criteria
    frame, totalReposWithoutFiltering, duration = createFrame()

    frame.to_csv(SAMPLING_FRAME_PATH, index=False)
    util.writeTxtFile(EXECUTION_REPORT_PATH, f'Obtained projects without filtering: {totalReposWithoutFiltering}')
    util.writeTxtFile(EXECUTION_REPORT_PATH, f'Obtained projects with filtering: {frame.shape[0]}')
    util.writeTxtFile(EXECUTION_REPORT_PATH, f'Execution duration - Project extraction: {duration}')

    '''
    # Generate metrics for the Qualitas Corpus (QC) dataset
    generateDataset(frame, QUALITAS_PATH, TOOL_PATH, f'{QUALITAS_PATH}/dataset', type="qualitas")
    '''

    # Obtain a current sample from Github
    sampleUpdated = SB.createKMeansSample(frame, 112)

    sampleUpdated.to_csv(f'{CURRENT_SAMPLE_PATH}/sample.csv', index=False)

    # Generate metrics for the current sample
    duration = generateDataset(frame, CURRENT_SAMPLE_PATH, SOURCEMETER_PATH, REPO_DOWNLOAD_PATH)
    util.writeTxtFile(EXECUTION_REPORT_PATH, f'Execution duration - Current sample metric generation: {duration}')

    # Instantiate the dataset maintainer

    maintainer = Maintenance(['totalSize'])

    qualitas = pd.read_csv(f'{QUALITAS_PATH}/sample.csv')

    # Update projects in the Qualitas Corpus

    qualitasUpdated, _, _ = maintainer.updateSample(frame, qualitas, 'DT', 112, **{'nClusters': 5})

    qualitasUpdated.to_csv(f'{QUALITAS_UPDATED_PATH}/sample.csv', index=False)

    # Generate metrics for the updated version of the Qualitas
    duration = generateDataset(frame, QUALITAS_UPDATED_PATH, SOURCEMETER_PATH, REPO_DOWNLOAD_PATH)
    util.writeTxtFile(EXECUTION_REPORT_PATH, f'Execution duration - Qualitas Corpus update metric generation: {duration}')


