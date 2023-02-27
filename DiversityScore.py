import numpy as np
import pandas as pd
import SimilarityFunc as SF
from operator import itemgetter

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

    def similarityScore(self, projectDimValue: int | float | str, populationDimValues: np.ndarray, similarityFunction)-> tuple[list[bool], list]:
        valueType = type(projectDimValue)
        similarityScore: list[bool] = []
        thresholds: list = []

        if not (similarityFunction is None):
            result = similarityFunction(projectDimValue,  populationDimValues)
            similarityScore = result[0]
            thresholds = result[1]
        elif valueType == int or valueType == float:
            result = SF.numericDimensionSimilarity(projectDimValue,  populationDimValues)
            similarityScore = result[0]
            thresholds = result[1]
        elif valueType == str or valueType == bool:
            result = SF.factorDimensionSimilarity(projectDimValue,  populationDimValues)
            similarityScore = result[0]
            thresholds = result[1]

        return similarityScore, thresholds

    def scoreSample (self, sample: pd.DataFrame) -> tuple[float, list[float], np.ndarray]:

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
                dimIndexMatrixOUTPUT, projectIndexSet = self.scoreProject(project, projPopCount, dimensionCount, dimensionsKeysSam, dimensionsKeysPop, populationArray)
                dimIndexMatrix = dimIndexMatrix | dimIndexMatrixOUTPUT
                indexSet = indexSet | projectIndexSet

        score = np.bincount(indexSet)[1]/projPopCount

        dimScore = []
        for i in range(dimensionCount):
            dimScore.append(np.bincount(dimIndexMatrix[i])[1]/projPopCount)

        return score, dimScore, indexSet

    def scoreProject(self, project: np.ndarray, projPopCount: int,  dimensionCount: int, dimensionsKeysSam, dimensionsKeysPop, populationArray):

        projectIndexSet = np.full((1, projPopCount), True)[0]
        dimIndexMatrix = np.full((dimensionCount, projPopCount), False)

        for i in range(dimensionCount):

            confDim = None
            if len(self.configuration) != 0:
                confDim = self.configuration[i]
            similarityScore = self.similarityScore(project[dimensionsKeysSam[i]], populationArray[:, dimensionsKeysPop[i]], confDim)[0]

            dimIndexMatrix[i, :] = dimIndexMatrix[i, :] | similarityScore
            projectIndexSet = projectIndexSet & similarityScore
        return dimIndexMatrix, projectIndexSet

    def clusterizePopulation(self, diverseSample: pd.DataFrame, population: pd.DataFrame) -> tuple[list, pd.DataFrame]:
        dimensionsKeys = []
        for dimension in self.dimensions:
            dimKey = self.population.columns.get_loc(dimension)
            dimensionsKeys.append(dimKey)

        similarityMatrix = diverseSample[['matrix']]
        matrix = []

        for i, row in similarityMatrix.iterrows():
            matrix.append(row[0])

        matrix = np.array(matrix)
        sampleIndexes = population[population['id'].isin(diverseSample['id'])].index.tolist()
        matrix = np.delete(matrix, sampleIndexes, axis=1)

        populationAux = population[~population['id'].isin(diverseSample['id'])].copy()

        sampleArray = diverseSample.to_numpy()
        populationArray = populationAux.to_numpy()
        groups = []
        groupId = 0

        while groupId < len(sampleArray):
            popIndexes = np.where(matrix[groupId, :])[0]
            quantity = 1
            similarProjects = []
            if len(popIndexes) >= 1:
                similarProjects = list(itemgetter(*popIndexes)(populationArray))
            if len(popIndexes) == 1:
                similarProjects = [itemgetter(*popIndexes)(populationArray)]

            quantity += len(similarProjects)

            populationArray = np.delete(populationArray, matrix[groupId, :], axis=0)
            matrix = np.delete(matrix, matrix[groupId, :], axis=1)

            thresholds = []
            dimIndex = 0

            for dimKey in dimensionsKeys:
                confDim = None
                if len(self.configuration) != 0:
                    confDim = self.configuration[dimIndex]

                projectValue = sampleArray[groupId, dimKey]
                popValues = pd.DataFrame(population.iloc[:, dimKey]).T.to_numpy()[0]
                result = self.similarityScore(projectValue, popValues, confDim)

                thresholds.append({self.dimensions[dimIndex]: result[1]})
                dimIndex += 1

            groups.append({'groupId': groupId, 'groupQty': quantity, 'thresholds': thresholds, 'similarProjects': similarProjects})

            groupId += 1

        return groups, population

    # If you set the sample argument, it will calculate scores of the projects in the sample
    def scoreProjects(self, sample=pd.DataFrame()) -> pd.DataFrame:
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

        return scores

