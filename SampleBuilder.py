import pandas as pd
import numpy as np
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
import random as rd
from DiversityScore import DiversityScore

def createSimpleRandomSample(dataset: pd.DataFrame, numberElements: int) -> pd.DataFrame:
    sample = dataset.sample(numberElements)
    return sample

def createStratifiedSample(dataset: pd.DataFrame, dimensions: list[str], numberElements: int, configuration=[]) -> tuple[pd.DataFrame, list]:
    clusterizer = DiversityScore(dataset, dimensions, configuration)
    diverseSample = createDiverseSample(dataset, dimensions)

    cont = 0
    for column in diverseSample.columns.to_list():
        if column == 'id' or column == 'url':
            break
        cont += 1

    dataset = dataset[~dataset['id'].isin(diverseSample['id'])]

    sampleArray = diverseSample.to_numpy()
    populationArray = dataset.to_numpy()

    groups, outliers = clusterizer.clusterizePopulation(sampleArray, populationArray)
    sample: list = sampleArray.tolist()

    proportion = (numberElements - len(groups)) / dataset.shape[0]
    for group in groups:
        qty = round(group['groupQty'] * proportion)

        if qty > 0:
            groupSample = rd.sample(group['similarProjects'], qty)
            sample += groupSample
        group.pop('similarProjects')

    col_headers = dataset.columns.to_list()
    df = pd.DataFrame(sample)
    df.columns = col_headers
    return df, groups

def sampleSize(populationSize: int, zscore=1.96, error= 0.05, sd = 0.5) -> int:
    a = pow(zscore, 2) * sd * (1-sd)
    numerator = a / pow(error, 2)
    denominator = (a / pow(error, 2) * populationSize) + 1
    return round(numerator / denominator)

def createDiverseSample(dataset: pd.DataFrame, dimensions: list[str], configuration=[], sample=pd.DataFrame()) -> pd.DataFrame:
    diversityScore = DiversityScore(dataset, dimensions, configuration)
    projectScores = diversityScore.scoreProjectsSorted()
    diversityScoreCol = projectScores.columns.get_loc('diversityScore')
    similarityMatrixCol = projectScores.columns.get_loc('similarityMatrix')
    populationArray = projectScores.to_numpy()
    projCount = populationArray[:, 0].size

    populationCovered = populationArray[0, similarityMatrixCol]
    sampleScore = populationArray[0, diversityScoreCol]
    sampleArray = [populationArray[0, :]]

    if sample.shape[0] > 0:
        sampleScore = diversityScore.scoreSample(sample)
        populationCovered = sampleScore[2]
        sampleScore = sampleScore[0]
        sampleArray = []
        for index, project in diversityScore.scoreProjectsSorted(sample).iterrows():
            sampleArray.append(project.to_numpy())


    while sampleScore < 1:
        for project in populationArray:
            project[similarityMatrixCol] = project[similarityMatrixCol] | populationCovered
            project[diversityScoreCol] = np.bincount(project[similarityMatrixCol])[1]/projCount - sampleScore

        newArray = populationArray[populationArray[:, diversityScoreCol].argsort()]
        populationArray = newArray
        sampleArray.append(populationArray[projCount - 1, :])
        sampleScore += populationArray[projCount - 1, diversityScoreCol]
        populationCovered = populationArray[projCount - 1, similarityMatrixCol]

    col_headers = projectScores.columns.to_list()
    df = pd.DataFrame(sampleArray)
    df.columns = col_headers
    df = df.drop(columns=['diversityScore', 'similarityMatrix'])

    return df

def testSampleDiversityRepresentativeness(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel=0.05) -> tuple[float, tuple[list, list]]:
    diversityScore = DiversityScore(population, variables)
    dScore = diversityScore.scoreSample(sample)[0]
    rScore = testSampleKS(sample, population, variables, sigLevel)

    return dScore, rScore

def testSampleKS(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel: float) -> tuple[list, list]:
    variablesValues = []
    varHypRejected = []
    for variable in variables:
        cdfFrame = stMod.ECDF(population[variable].to_numpy())
        ks = sp.ks_1samp(sample[variable], cdfFrame)
        variablesValues.append({'dimension': variable, 'test': 'one-sample-K-S', 'test-score': ks[0], 'p-value': ks[1]})
        if ks[1] < sigLevel:
            varHypRejected.append({'dimension': variable, 'test': 'one-sample-K-S', 'test-score': ks[0], 'p-value': ks[1]})

    return variablesValues, varHypRejected


def generateGroupsOutput(stratifiedSampleGroups: list, proportion: float) -> pd.DataFrame:
    groups = stratifiedSampleGroups.copy()

    for group in groups:
        group['sampleQty'] = round(group['groupQty'] * proportion)
        if group['sampleQty'] == 0:
            group['sampleQty'] = 1
        thresholds: dict = group['thresholds']
        for threshold in thresholds:
            key = list(threshold.keys())[0]
            values = list(threshold.values())[0]
            group[key + 'Min'] = values[0]
            group[key + 'Max'] = values[1]

    groupsDataframe = pd.DataFrame(groups)
    groupsDataframe.pop('thresholds')
    return groupsDataframe


def getProjectGroup (project: dict, groups: pd.DataFrame, dimensions: list[str]) -> pd.DataFrame:
    for dimension in dimensions:
        dimValue = project[dimension]
        groups = groups[dimValue >= groups[dimension + 'Min']]
        groups = groups[dimValue <= groups[dimension + 'Max']]

    return groups['groupId']
