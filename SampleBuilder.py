import pandas as pd
import numpy as np
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
import random as rd
from DiversityScore import clusterizePopulation, diversityScore, getProjectsScoreSorted

def createSimpleRandomSample(dataset: pd.DataFrame, proportion=0.2) -> pd.DataFrame:
    sample = dataset.sample(int(len(dataset) * proportion))
    return sample

def createStratifiedSample(dataset: pd.DataFrame, dimensions: list[str], proportion=0.2, configuration=[]) -> pd.DataFrame:
    groups = clusterizePopulation(dataset, dimensions, configuration)
    sample = []
    for group in groups:
        qty = round(group['groupQty'] * proportion)
        if qty == 0:
            qty = 1
        groupSample = rd.sample(group['similarProjects'], qty)
        sample += groupSample

    col_headers = dataset.columns.to_list()
    df = pd.DataFrame(sample)
    df.columns = col_headers
    return df


def createDiverseSample(dataset: pd.DataFrame, dimensions: list[str], configuration=[], sample=pd.DataFrame()) -> pd.DataFrame:
    projectScores = getProjectsScoreSorted(dataset, dimensions, configuration)
    diversityScoreCol = projectScores.columns.get_loc('diversityScore')
    similarityMatrixCol = projectScores.columns.get_loc('similarityMatrix')
    populationArray = projectScores.to_numpy()
    projCount = populationArray[:, 0].size

    populationCovered = populationArray[0, similarityMatrixCol]
    sampleScore = populationArray[0, diversityScoreCol]
    sampleArray = [populationArray[0, :]]

    if sample.shape[0] > 0:
        sampleScore = diversityScore(sample, dataset, dimensions, configuration)
        populationCovered = sampleScore[2]
        sampleScore = sampleScore[0]
        sampleArray = []
        for index, project in getProjectsScoreSorted(dataset, dimensions, configuration, sample).iterrows():
            sampleArray.append(project.to_numpy())


    while sampleScore < 1:
        for project in populationArray:
            project[similarityMatrixCol] = project[similarityMatrixCol] | populationCovered
            project[diversityScoreCol] = np.bincount(project[similarityMatrixCol])[1]/projCount - sampleScore

        newArray = populationArray[populationArray[:,diversityScoreCol].argsort()]
        populationArray = newArray
        sampleArray.append(populationArray[projCount - 1, :])
        sampleScore += populationArray[projCount - 1, diversityScoreCol]
        populationCovered = populationArray[projCount - 1, similarityMatrixCol]

    col_headers = projectScores.columns.to_list()
    df = pd.DataFrame(sampleArray)
    df.columns = col_headers
    df = df.drop(columns=['diversityScore', 'similarityMatrix'])

    return df

def testSampleDiversityRepresentativeness(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel=0.05) -> tuple[tuple[float, list[float], np.ndarray], tuple[[dict], [dict]]]:
    dScore = diversityScore(sample, population, variables)
    rScore = testSampleKS(sample, population, variables, sigLevel)

    return dScore, rScore

def testSampleKS(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel: float) -> tuple[[dict], [dict]]:
    variablesValues = []
    varHypRejected = []
    for variable in variables:
        cdfFrame = stMod.ECDF(population[variable].to_numpy())
        ks = sp.ks_1samp(sample[variable], cdfFrame)
        variablesValues.append({'dimension': variable, 'test': 'one-sample-K-S', 'test-score': ks[0], 'p-value': ks[1]})
        if ks[1] < sigLevel:
            varHypRejected.append({'dimension': variable, 'test': 'one-sample-K-S', 'test-score': ks[0], 'p-value': ks[1]})

    return variablesValues, varHypRejected

if __name__ == '__main__':
    frame = pd.read_csv("./datasets/Larger/frameEngin.csv")
    dimensions = ['stargazerCount', 'forkCount', 'issues', 'totalSize', 'pullReqCount', 'commits']
    samples = []

    simpleRandom = createSimpleRandomSample(frame)
    stratified = createStratifiedSample(frame, dimensions)
    diverse = createDiverseSample(frame, dimensions)


    samples.append({'sampStrat': 'Simple Random', 'sample': simpleRandom})
    samples.append({'sampStrat': 'Stratified', 'sample': stratified})
    samples.append({'sampStrat': 'Diversity', 'sample': diverse})

    for sample in samples:
        dScore, rScore = testSampleDiversityRepresentativeness(sample['sample'], frame, dimensions)
        dScore = dScore[0]
        nullHypRej = len(rScore[1])
        print('Sampling Strategy:', sample['sampStrat'], ' -Diversity Score:', dScore, ' -Null Hypotesis Rejected:', nullHypRej)

    # Completar cobertura muestreo estratificado
    stratDiversified = createDiverseSample(frame, dimensions, sample=stratified)
    dScore, rScore = testSampleDiversityRepresentativeness(stratDiversified, frame, dimensions)
    dScore = dScore[0]
    nullHypRej = len(rScore[1])
    print('Sampling Strategy: Stratified covered  -Diversity Score:', dScore, ' -Null Hypotesis Rejected:', nullHypRej)

