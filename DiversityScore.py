import statistics as st
import math
import numpy as np
import pandas as pd

class DiversityScore:

    # GUARDAR INDICES DE CADA DIMENSION, Y EL ARREGLO DE LA POBLACION NO EL DATAFRAME
    def __init__(self, population: pd.DataFrame, dimensions: list[str], configuration=[]):
        for dimension in dimensions:
            if not(dimension in population.columns):
                print('Population dataset does not have', dimension, "variable")
                exit()

        self.population = population
        self.dimensions = dimensions
        self.configuration = configuration

    def _numericDimensionSimilaritySD(self, projectSample: int | float, projectsPopulation: np.array) -> tuple[list[bool], list[float, float]]:
        stddev = st.stdev(projectsPopulation)
        lower = projectSample - stddev
        upper = projectSample + stddev
        similarityScore = []
        for project in projectsPopulation:
            similarityScore.append(lower <= project <= upper)
        return similarityScore, [lower, upper]

    def _numericDimensionSimilarity(self: int | float, projectSample, projectsPopulation: np.array)-> tuple[list[bool], list[float, float]]:
        lower = pow(10, math.log10(projectSample + 1) - 0.5) - 1
        upper = pow(10, math.log10(projectSample + 1) + 0.5) - 1
        similarityScore = []
        for project in projectsPopulation:
            similarityScore.append(lower <= project <= upper)
        return similarityScore, [lower, upper]

    def _factorDimensionSimilarity(self, projectSample: str, projectsPopulation: np.array)-> tuple[list[bool], list[str]]:
        similarityScore = []
        for project in projectsPopulation:
            similarityScore.append(projectSample == project)
        return similarityScore, [projectSample]

    def _similarityScore(self, projectDimValue: int | float | str, populationDimValues: np.array, similarityFunction)-> tuple[list[bool], list]:
        valueType = type(projectDimValue)
        similarityScore: list[bool]
        thresholds: list

        if not (similarityFunction is None):
            result = similarityFunction(projectDimValue,  populationDimValues)
            similarityScore = result[0]
            thresholds = result[1]
        elif valueType == int or valueType == float:
            result = self._numericDimensionSimilarity(projectDimValue,  populationDimValues)
            similarityScore = result[0]
            thresholds = result[1]
        elif valueType == str:
            result = self._factorDimensionSimilarity(projectDimValue,  populationDimValues)
            similarityScore = result[0]
            thresholds = result[1]

        return similarityScore, thresholds

    def scoreSample (self, sample: pd.DataFrame) -> tuple[float, list[float], np.array]:

        dimensionsKeysPop = []
        dimensionsKeysSam = []
        for dimension in self.dimensions:
            if not(dimension in sample.columns):
                print('Sample dataset does not have', dimension, "variable")
                exit()
            dimensionsKeysSam.append(sample.columns.get_loc(dimension))
            dimensionsKeysPop.append(self.population.columns.get_loc(dimension))

        sampleArray = sample.to_numpy()
        populationArray = self.population.to_numpy()
        projPopCount = populationArray[:, 0].size
        projSamCount = sampleArray[:, 0].size
        dimensionCount = len(self.dimensions)

        dimIndexMatrix = np.full((dimensionCount, projPopCount), False)
        indexSet = np.full((1, projPopCount), False)[0]

        if projSamCount > 0:
            for project in sampleArray:
                projectIndexSet = np.full((1, projPopCount), True)[0]

                for i in range(dimensionCount):

                    confDim = None
                    if len(self.configuration) != 0:
                        confDim = self.configuration[i]
                    similarityScore = self._similarityScore(project[dimensionsKeysSam[i]], populationArray[:, dimensionsKeysPop[i]], confDim)[0]

                    dimIndexMatrix[i, :] = dimIndexMatrix[i, :] | similarityScore
                    projectIndexSet = projectIndexSet & similarityScore

                indexSet = indexSet | projectIndexSet

        score = np.bincount(indexSet)[1]/projPopCount

        dimScore = []
        for i in range(dimensionCount):
            dimScore.append(np.bincount(dimIndexMatrix[i])[1]/projPopCount)

        return score, dimScore, indexSet

    def clusterizePopulation(self, diverseSample: np.array, population: np.ndarray) -> tuple[list, np.array]:
        dimensionsKeys = []
        for dimension in self.dimensions:
            dimKey = self.population.columns.get_loc(dimension)
            dimensionsKeys.append(dimKey)

        groups = []
        groupId = 1

        for project in diverseSample:
            projectIndexSet = np.full((1, population[:, 0].size), True)[0]
            thresholds = []
            i = 0
            for dimKey in dimensionsKeys:
                confDim = None
                if len(self.configuration) != 0:
                    confDim = self.configuration[i]
                result = self._similarityScore(project[dimKey], population[:, dimKey], confDim)
                similarityScore = result[0]
                thresholds.append({self.dimensions[i]: result[1]})

                i += 1
                projectIndexSet = projectIndexSet & similarityScore

            quantity = 0
            j = 0
            similarProjects = []
            while j < population[:, 0].size:
                similar = projectIndexSet[j]
                if similar:
                    project: np.ndarray = population[j, :]
                    project = np.append(project, groupId)
                    similarProjects.append(project)
                    quantity += 1
                    population = np.delete(population, j, 0)
                j += 1
            groups.append({'groupId': groupId, 'groupQty': quantity, 'thresholds': thresholds, 'similarProjects': similarProjects})

            groupId += 1

        return groups, population

    # If you set the sample argument, it will calculate scores of the projects in the sample
    def scoreProjectsSorted(self, sample=pd.DataFrame()) -> pd.DataFrame:
        score: pd.DataFrame
        if sample.shape[0] == 0:
            scores = self.population.copy()
        else:
            scores = sample.copy()

        for i in range(scores.shape[0]):
            project = self.population.loc[[i]]
            projectScore = self.scoreSample(project)
            scores.at[i, 'diversityScore'] = projectScore[0]
            scores.at[i, 'similarityMatrix'] = projectScore[2]

        return scores.sort_values(by=['diversityScore'], ascending=False)

