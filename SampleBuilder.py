import pandas as pd
import numpy as np
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
import random as rd
from DiversityScore import DiversityScore
import json

def createSimpleRandomSample(dataset: pd.DataFrame, proportion=0.2) -> pd.DataFrame:
    sample = dataset.sample(int(len(dataset) * proportion))
    return sample

def createStratifiedSample(dataset: pd.DataFrame, dimensions: list[str], proportion=0.2, configuration=[], includeOutliers=False) -> tuple[pd.DataFrame, list]:
    clusterizer = DiversityScore(dataset, dimensions, configuration)
    diverseSample = createDiverseSample(frame, dimensions)

    cont = 0
    for column in diverseSample.columns.to_list():
        if column == 'id' or column == 'url':
            break
        cont += 1

    sampleArray = diverseSample.to_numpy()
    populationArray = dataset.to_numpy()
    for proj in sampleArray:
        index = np.argwhere(populationArray == proj[cont])[0][0]
        populationArray = np.delete(populationArray, index, 0)

    groups, outliers = clusterizer.clusterizePopulation(sampleArray, populationArray)
    sample: list = sampleArray.tolist()

    # ADD idGroup column in the output dataset, and the outliers are group 0
    for group in groups:
        qty = round((group['groupQty'] + 1) * proportion) - 1
        if qty > 0:
            groupSample = rd.sample(group['similarProjects'], qty)
            sample += groupSample
        group['groupQty'] += 1
        group.pop('similarProjects')

    if includeOutliers:
        qty = round(len(outliers) * proportion)
        groupSample = rd.sample(outliers.tolist(), qty)
        sample += groupSample

    col_headers = dataset.columns.to_list()
    df = pd.DataFrame(sample)
    df.columns = col_headers
    return df, groups


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

def testSampleDiversityRepresentativeness(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel=0.05) -> tuple[tuple[float, list, np.ndarray], tuple[list, list]]:
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

if __name__ == '__main__':
    frame = pd.read_csv("./datasets/2022829/filtered.csv")
    dimensions = ['stargazerCount', 'forkCount', 'closedIssuesCount', 'totalSize', 'closedPullReqCount', 'commits']
    samples = []


    #start_time = time.time()
    simpleRandom = createSimpleRandomSample(frame)
    stratified, groups = createStratifiedSample(frame, dimensions)
    diverse = createDiverseSample(frame, dimensions)

    samples.append({'sampStrat': 'Simple Random', 'sample': simpleRandom})
    samples.append({'sampStrat': 'Stratified', 'sample': stratified})
    samples.append({'sampStrat': 'Diversity', 'sample': diverse})

    for sample in samples:
        dScore, rScore = testSampleDiversityRepresentativeness(sample['sample'], frame, dimensions)
        dScore = dScore
        nullHypRej = len(rScore[1])
        print('Sampling Strategy:', sample['sampStrat'], ' -Diversity Score:', dScore, ' -Null Hypotesis Rejected:', nullHypRej)
        sample['sample'].to_csv("./datasets/2022829/" + sample['sampStrat'] + ".csv", index=False)

    for group in groups:
        thresholds: dict = group['thresholds']
        for threshold in thresholds:
            key = list(threshold.keys())[0]
            values = list(threshold.values())[0]
            group[key + 'Min'] = values[0]
            group[key + 'Max'] = values[1]

    groupsDataframe = pd.DataFrame(groups)
    groupsDataframe.pop('thresholds')
    groupsDataframe.to_csv("./datasets/2022829/groups.csv", index=False)

    hypRejected = True
    while hypRejected:
        stratified = createStratifiedSample(frame, dimensions)
        for variable in dimensions:
            cdfFrame = stMod.ECDF(frame[variable].to_numpy())
            ks = sp.ks_1samp(stratified[variable], cdfFrame)
            print(variable, ' -Pvalues:', ks[1])
            if ks[1] > 0.25:
                hypRejected = False
            else:
                hypRejected = True
                break

    #print("--- %s seconds ---" % (time.time() - start_time))
    stratified.to_csv("./datasets/2022829/stratified.csv", index=False)





    # Compare dataset waves
    dimensions = ['stargazerCount', 'forkCount', 'closedIssuesCount', 'totalSize', 'closedPullReqCount', 'commits']

    frame1 = pd.read_csv("./datasets/2022715/Stratified.csv")
    frame2 = pd.read_csv("./datasets/2022823/Stratified.csv")
    frame3 = pd.read_csv("./datasets/2022829/Stratified.csv")

    for variable in dimensions:
        res1 = sp.ks_2samp(frame1[variable], frame2[variable])
        res2 = sp.ks_2samp(frame2[variable], frame3[variable])
        res3 = sp.ks_2samp(frame1[variable], frame3[variable])
        print('Comparison 1 & 2 -- dimension: ' + variable + ' - test-score:' + str(res1[0]) + ' - p-value:' + str(res1[1]))
        print('Comparison 2 & 3 -- dimension: ' + variable + ' - test-score:' + str(res2[0]) + ' - p-value:' + str(res2[1]))
        print('Comparison 1 & 3 -- dimension: ' + variable + ' - test-score:' + str(res3[0]) + ' - p-value:' + str(res3[1]))
