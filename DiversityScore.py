import statistics as st
import math
import numpy as np
import pandas as pd

def _numericDimensionSimilaritySD(projectSample, projectsPopulation: np.array) -> list[bool]:
    stddev = st.stdev(projectsPopulation)
    lower = projectSample - stddev
    upper = projectSample + stddev
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(lower <= project <= upper)
    return similarityScore

def _numericDimensionSimilarity(projectSample, projectsPopulation: np.array)-> list[bool]:
    lower = pow(10, math.log10(projectSample + 1) - 0.5) - 1
    upper = pow(10, math.log10(projectSample + 1) + 0.5) - 1
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(lower <= project <= upper)
    return similarityScore

def _factorDimensionSimilarity(projectSample, projectsPopulation: np.array)-> list[bool]:
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(projectSample == project)
    return similarityScore

def _similarityScore(projectDimValue, populationDimValues: np.array, configuration)-> list[bool]:
    valueType = type(projectDimValue)
    similarityScore: list[bool]

    if not (configuration is None):
        similarityScore = configuration(projectDimValue,  populationDimValues)
    elif valueType == int or valueType == float:
        similarityScore = _numericDimensionSimilarity(projectDimValue,  populationDimValues)
    elif valueType == str:
        similarityScore = _factorDimensionSimilarity(projectDimValue,  populationDimValues)

    return similarityScore

def diversityScore (sample: pd.DataFrame, population: pd.DataFrame, dimensions: list[str], configuration=[]) -> tuple[float, list[float], np.ndarray]:

    #Validar que las cabeceras de las dimensiones sean iguales que a los datasets
    dimensionsKeysPop = []
    dimensionsKeysSam = []
    for dimension in dimensions:
        if not(dimension in population.columns and dimension in sample.columns):
            print('Population or sample dataset does not have', dimension, "variable")
            exit()
        dimensionsKeysSam.append(sample.columns.get_loc(dimension))
        dimensionsKeysPop.append(population.columns.get_loc(dimension))

    sampleArray = sample.to_numpy()
    populationArray = population.to_numpy()
    projPopCount = populationArray[:, 0].size
    projSamCount = sampleArray[:, 0].size
    dimensionCount = len(dimensions)

    dimIndexMatrix = np.full((dimensionCount, projPopCount), False)
    indexSet = np.full((1, projPopCount), False)[0]

    if projSamCount > 0:
        for project in sampleArray:
            projectIndexSet = np.full((1, projPopCount), True)[0]

            for i in range(dimensionCount):

                confDim = None
                if len(configuration) != 0:
                    confDim = configuration[i]
                similarityScore = _similarityScore(project[dimensionsKeysSam[i]], populationArray[:, dimensionsKeysPop[i]], confDim)

                dimIndexMatrix[i, :] = dimIndexMatrix[i, :] | similarityScore
                projectIndexSet = projectIndexSet & similarityScore

            indexSet = indexSet | projectIndexSet

    score = np.bincount(indexSet)[1]/projPopCount

    dimScore = []
    for i in range(dimensionCount):
        dimScore.append(np.bincount(dimIndexMatrix[i])[1]/projPopCount)

    return score, dimScore, indexSet

def clusterizePopulation(population: pd.DataFrame, dimensions: list[str], configuration=[]) -> list:
    dimensionsKeys = []
    for dimension in dimensions:
        if not(dimension in population.columns):
            print('Population dataset does not have', dimension, "variable")
            exit()
        dimKey = population.columns.get_loc(dimension)
        dimensionsKeys.append(dimKey)

    populationSorted = getProjectsScoreSorted(population, dimensions, configuration).drop(columns=['diversityScore', 'similarityMatrix'])
    populationArray = populationSorted.to_numpy()
    groups = []
    groupId = 1

    while populationArray[:, 0].size != 0:
        projectIndexSet = np.full((1, populationArray[:, 0].size), True)[0]
        i = 0
        for dimKey in dimensionsKeys:
            confDim = None
            if len(configuration) != 0:
                confDim = configuration[i]
            similarityScore = _similarityScore(populationArray[0, dimKey], populationArray[:, dimKey], confDim)

            i += 1
            projectIndexSet = projectIndexSet & similarityScore

        quantity = 0
        j = 0
        similarProjects = []
        while j < populationArray[:, 0].size:
            similar = projectIndexSet[j]
            if similar:
                similarProjects.append(populationArray[j, :])
                quantity += 1
                populationArray = np.delete(populationArray, j, 0)
                projectIndexSet = np.delete(projectIndexSet, j, 0)
            else:
                j += 1
        groups.append({'groupId': groupId, 'groupQty': quantity, 'similarProjects': similarProjects})
        groupId += 1

    return groups

# If you set the sample argument, it will calculate scores of the projects in the sample
def getProjectsScoreSorted(population: pd.DataFrame, dimensions: list[str], configuration=[], sample=pd.DataFrame()) -> pd.DataFrame:
    score: pd.DataFrame
    if sample.shape[0] == 0:
        scores = population.copy()
    else:
        scores = sample.copy()

    for i in range(scores.shape[0]):
        project = population.loc[[i]]
        projectScore = diversityScore(project, population, dimensions, configuration)
        scores.at[i, 'diversityScore'] = projectScore[0]
        scores.at[i, 'similarityMatrix'] = projectScore[2]

    return scores.sort_values(by=['diversityScore'], ascending=False)
