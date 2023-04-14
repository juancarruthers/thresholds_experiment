import pandas as pd
import numpy as np
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
import random as rd
from DiversityScore import DiversityScore
import sklearn as sk

def createSimpleRandomSample(dataset: pd.DataFrame, numberElements: int) -> pd.DataFrame:
    sample = dataset.sample(numberElements)
    return sample

def createKMeansStratifiedSample(dataset: pd.DataFrame, dimensions: list[str], numberElements: int, nClusters = 6) -> pd.DataFrame:
    dimensionsKeys = []
    for dimension in dimensions:
        dimKey = dataset.columns.get_loc(dimension)
        dimensionsKeys.append(dimKey)

    analizedDimensions = dataset.iloc[:, dimensionsKeys]

    scaler = sk.preprocessing.StandardScaler()
    data_scaled = scaler.fit_transform(analizedDimensions)
    kmeans = sk.cluster.KMeans(n_clusters=nClusters, init='k-means++')
    kmeans.fit(data_scaled)
    proportion = numberElements / dataset.shape[0]
    sample = pd.DataFrame()
    for i in range(nClusters):
        clusters = np.where(kmeans.labels_ == i)[0]
        projects = dataset.iloc[clusters, :]
        quantity = round(len(clusters) * proportion)
        if quantity < 1:
            sample = pd.concat([sample, projects.sample(1)], ignore_index=True)
        else:
            sample = pd.concat([sample, projects.sample(quantity)], ignore_index=True)


    return sample

def createStratifiedSample(dataset: pd.DataFrame, dimensions: list[str], numberElements: int, configuration=[]) -> tuple[pd.DataFrame, list]:
    clusterizer = DiversityScore(dataset, dimensions, configuration)
    diverseSample = createDiverseSample(dataset, dimensions, configuration)

    cont = 0
    for column in diverseSample.columns.to_list():
        if column == 'id' or column == 'url':
            break
        cont += 1

    groups, outliers = clusterizer.clusterizePopulation(diverseSample, dataset)
    diverseSampleList = diverseSample.drop(columns=['matrix']).values.tolist()
    sample = []

    proportion = (numberElements - len(groups)) / dataset.shape[0]
    remainings = []
    for group in groups:
        realQty = (group['groupQty'] - 1) * proportion

        qty = round(realQty)

        if (realQty > qty):
            proj = rd.sample(list(enumerate(group['similarProjects'])), 1)
            group['similarProjects'].pop(proj[0][0])
            remainings.append(proj[0][1])

        if qty > 0:
            groupSample = rd.sample(group['similarProjects'], qty)
            sample += groupSample

        group.pop('similarProjects')

    sizeCorrection = numberElements - len(sample) - len(diverseSampleList)
    if (sizeCorrection > 0):
        sample += rd.sample(remainings, sizeCorrection)
    elif (sizeCorrection < 0):
        sizeCorrection *= -1
        projects = rd.sample(list(enumerate(sample)), sizeCorrection)
        for project in projects:
            sample.pop(project[0])

    sample += diverseSampleList
    col_headers = dataset.columns.to_list()
    df = pd.DataFrame(sample)
    df.columns = col_headers
    return df, groups

def sampleSize(populationSize: int, zscore=2, error= 0.05, sd = 0.5) -> int:
    a = pow(zscore, 2) * sd * (1-sd)
    numerator = a * populationSize
    denominator = a + populationSize * pow(error, 2)
    return round(numerator / denominator)

def createDiverseSample(dataset: pd.DataFrame, dimensions: list[str], configuration=[], sample=pd.DataFrame()) -> pd.DataFrame :
    diversityScore = DiversityScore(dataset, dimensions, configuration)
    projectScores = diversityScore.scoreProjects()
    diversityScoreCol: int = projectScores.columns.get_loc('diversityScore')
    similarityMatrixCol : int = projectScores.columns.get_loc('similarityMatrix')
    populationArray = projectScores.to_numpy()
    projCount = populationArray[:, 0].size

    sampleArray = []

    while len(populationArray) > 0:
        newArray = populationArray.copy()[populationArray[:, diversityScoreCol].argsort()][::-1][:projCount]
        id = newArray[0, :][0]
        score = projectScores[projectScores['id'] == id]['similarityMatrix'].values
        sampleArray.append(np.concatenate((newArray[0, :], score), axis=0))
        #sampleScore += newArray[0, diversityScoreCol]

        lastInsert = sampleArray[len(sampleArray) - 1][similarityMatrixCol]
        ids = np.where(lastInsert)[0]
        populationArray = np.delete(populationArray, ids, axis=0)
        projCount = populationArray[:, 0].size
        populationCovered = np.full((1, projCount), False)[0]

        for project in populationArray:
            project[similarityMatrixCol] = np.delete(project[similarityMatrixCol], ids)
            project[similarityMatrixCol] = project[similarityMatrixCol] | populationCovered
            project[diversityScoreCol] = np.bincount(project[similarityMatrixCol])[1]/projCount

    col_headers = projectScores.columns.to_list()
    col_headers.append('matrix')
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


def generateGroupsOutput(stratifiedSampleGroups: list, sample: pd.DataFrame) -> pd.DataFrame:
    groups = stratifiedSampleGroups.copy()
    sampleAux = sample.copy()
    for group in groups:
        sample2 = sampleAux.copy()

        thresholds: dict = group['thresholds']
        for threshold in thresholds:
            key = list(threshold.keys())[0]
            values = list(threshold.values())[0]
            group[key + 'Min'] = values[0]
            group[key + 'Max'] = values[1]
            sample2 = sample2[values[0] <= sample2[key]]
            sample2 = sample2[values[1] >= sample2[key]]

        group['sampleQty'] = sample2.shape[0]
        sampleAux = sampleAux[~sampleAux.isin(sample2)]

    groupsDataframe = pd.DataFrame(groups)
    groupsDataframe.pop('thresholds')
    return groupsDataframe


def getProjectGroup (project: dict, groups: pd.DataFrame, dimensions: list[str]) -> pd.DataFrame:
    for dimension in dimensions:
        dimValue = project[dimension]
        groups = groups[dimValue >= groups[dimension + 'Min']]
        groups = groups[dimValue <= groups[dimension + 'Max']]

    return groups['groupId']


