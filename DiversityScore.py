from statistics import stdev
from math import log10
import pandas as pd
import numpy
import random

def _numericDimensionSimilaritySD(projectSample, projectsPopulation):
    stddev = stdev(projectsPopulation)
    lower = projectSample - stddev
    upper = projectSample + stddev
    similarityScore = pd.DataFrame()
    for project in projectsPopulation:
        similarityScore.append(lower <= project <= upper)
    return similarityScore

def _numericDimensionSimilarity(projectSample, projectsPopulation):
    lower = pow(10, log10(projectSample + 1) - 0.5) - 1
    upper = pow(10, log10(projectSample + 1) + 0.5) - 1
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(lower <= project <= upper)
    return similarityScore

def _factorDimensionSimilarity(projectSample, projectsPopulation):
    similarityScore = pd.DataFrame()
    for project in projectsPopulation:
        similarityScore.append(projectSample == project)
    return similarityScore


def scoreProjects (sample, population, dimensions: list[str], configuration=[]):

    #Validar que las cabeceras de las dimensiones sean iguales que a los datasets

    projPopCount = len(population[dimensions[0]])
    projSamCount = len(sample[dimensions[0]])
    dimensionCount = len(dimensions)
    dimIndexMatrix = pd.DataFrame(numpy.full((dimensionCount, projPopCount), False))
    indexSet = pd.DataFrame(numpy.full((1, projPopCount), False))

    if projSamCount > 0:
        for index, project in sample.iterrows():
            projectIndexSet = pd.DataFrame(numpy.full((1, projPopCount), True))

            for dimKey in range(dimensionCount):
                dimValue = dimensions[dimKey]
                #Agregar calculo de similaridad por tipo de variable
                similarityScore = _numericDimensionSimilarity(project[dimValue], population[dimValue])


                dimIndexMatrix.loc[dimKey] = dimIndexMatrix.loc[dimKey] | similarityScore
                projectIndexSet = projectIndexSet & similarityScore

            indexSet = indexSet | projectIndexSet

    score = indexSet.T.groupby(0).size().to_dict()[True]/projPopCount

    dimIndexMatrixTranp = dimIndexMatrix.T
    dimScore = []
    for dim in range(dimensionCount):
        dimensionSimilarity = dimIndexMatrixTranp[dim].to_frame()
        dimScore.append(dimensionSimilarity.groupby(dim).size().to_dict()[True]/projPopCount)

    return score, dimScore

def _organizePopulationGroups(population, dimensions: list[str], configuration=[]):
    #Mejorar rendimiento utilizando ndarrays en vez de Dataframe
    dimensionCount = len(dimensions)
    groups = []
    projectsClustered = 0
    groupId = 1

    while len(population[dimensions[0]]) != 0:
        projectIndexSet = pd.DataFrame(numpy.full((1, len(population[dimensions[0]])), True))

        for dimKey in range(dimensionCount):
            dimValue = dimensions[dimKey]
            # Agregar calculo de similaridad por tipo de variable
            similarityScore = _numericDimensionSimilarity(population[dimValue].iloc[0], population[dimValue])

            projectIndexSet = projectIndexSet & similarityScore

        quantity = 0
        i=0
        similarProjects = []
        for index, project in population.iterrows():
            similar = projectIndexSet.T.iloc[i][0]
            if similar:
                quantity += 1
                #row = pd.concat([pd.DataFrame(data={'groupId': groupId}, index=['groupId']), population.loc[index]])
                #groups.append(row)
                similarProjects.append(population.loc[index])
                population = population.drop(index)
            i += 1
        groups.append({'groupId': groupId, 'groupQty': quantity, 'similarProjects': similarProjects})
        projectsClustered += quantity
        groupId += 1

    return groups

def createStratifiedSample(population, dimensions: list[str], sampleSize=0.2, configuration=[]):
    groups = _organizePopulationGroups(population, dimensions)
    sample = []
    for group in groups:
        qty = round(group['groupQty'] * sampleSize)
        if qty == 0:
            qty = 1
        groupSample = random.sample(group['similarProjects'], qty)
        sample = sample + groupSample

    df = pd.DataFrame(sample)
    df.to_csv("./datasets/Larger/stratified2.csv")


if __name__ == '__main__':
    #score = scoreProjects(pd.read_csv("./datasets/Larger/stratified2.csv"), pd.read_csv("./datasets/Larger/frameEngin.csv"), ['stargazerCount', 'forkCount', 'issues', 'totalSize', 'pullReqCount', 'commits'])
    #print(score[0]*831)
    #score[2].to_csv("./datasets/Larger/test2.csv")
    createStratifiedSample(pd.read_csv("./datasets/Larger/frameEngin.csv"), ['stargazerCount', 'forkCount', 'issues', 'totalSize', 'pullReqCount', 'commits'], 0.3)
    # CREAR FUNCION PARA GENERAR MUESTRA ESTRATIFICADA
'''
score.projects < - function(sample, universe, space, configuration=NA)
{
    variables < - all.vars(space)

    if (length(setdiff(variables, names(sample))) > 0)
stop(gettextf("variables '%s' not found in sample", paste(setdiff(variables, names(sample)), collapse=", ")), domain=NA)

if (length(setdiff(variables, names(universe))) > 0)
stop(gettextf("variables '%s' not found in universe", paste(setdiff(variables, names(universe)), collapse=", ")),
     domain=NA)

project_var < - variables[1]
dimension_vars < - variables[-1]

dim_index_matrix < - matrix(rep(F, length=length(dimension_vars) * nrow(universe)),
                            nrow=length(dimension_vars), ncol=nrow(universe), byrow=T)
index_set < - rep(F, length=nrow(universe))

if (nrow(sample) > 0)
for (pid in 1:nrow(sample)) {
project_index_set < - rep(T, length=nrow(universe))

for (dim in 1:length(dimension_vars)) {
    dimension < - dimension_vars[dim]

    if (dim <= length(configuration) & ! is.na(configuration[dim])) {
                                                                    is.similar < - configuration[[dim]](
    sample[pid, dimension], universe[, dimension])
d < - is.similar(universe[, dimension])
}
else if (is.numeric(universe[, dimension])) {
is.similar < - create.numeric.similarity(sample[pid, dimension], universe[, dimension])
d < - is.similar(universe[, dimension])
}
else if ( is.factor(universe[, dimension])) {
is.similar < - create.factor.similarity(sample[pid, dimension], universe[, dimension])
d < - is.similar(universe[, dimension])
}
else {
stop(gettextf("no similarity function defined for '%s'", dimension, domain = NA))
}

dim_index_matrix[dim, ] < - dim_index_matrix[dim, ] | d
project_index_set < - project_index_set & d
}
index_set < - index_set | project_index_set
}

score < - sum(index_set, na.rm=T) / length(index_set)
dimension_score < - apply(dim_index_matrix, 1, function(x) {sum(x, na.rm=T) / length(x)})
return (list(dimensions=dimension_vars,
             score=score, dimension.score=dimension_score,
        score.indexset=index_set, dimension.indexset=dim_index_matrix))
}
'''
