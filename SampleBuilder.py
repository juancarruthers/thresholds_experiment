import pandas as pd
import numpy as np
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
import random as rd
import SizeThresholds
from DiversityScore import DiversityScore
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

def createSimpleRandomSample(dataset: pd.DataFrame, numberElements: int) -> pd.DataFrame:
    sample = dataset.sample(numberElements)
    return sample

def createKMeansStratifiedSample(dataset: pd.DataFrame, dimensions: list[str], numberElements: int, nClusters = 6, seed=0) -> pd.DataFrame:
    dimensionsKeys = []
    for dimension in dimensions:
        dimKey = dataset.columns.get_loc(dimension)
        dimensionsKeys.append(dimKey)

    analizedDimensions = dataset.iloc[:, dimensionsKeys]

    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(analizedDimensions)
    kmeans = KMeans(n_clusters=nClusters, init='k-means++', n_init=10, random_state=seed)
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

    sampleDifference = sample.shape[0] - numberElements
    if sampleDifference > 0:
        sample = sample[~sample['id'].isin(sample.sample(sampleDifference)['id'])]
    elif sampleDifference < 0:
        sample = pd.concat([sample, dataset[~dataset['id'].isin(sample['id'])].sample(sampleDifference * -1)])

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
    diverseSampleList = list(diverseSample.drop(columns=['matrix']).values)
    if len(diverseSampleList) > numberElements:
        print(f'Sample must have {len(diverseSampleList)} or more elements')
        exit()
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

def testSampleDiversityRepresentativeness(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel=0.05) -> tuple[float, tuple[list, list], tuple[list, list]]:
    diversityScore = DiversityScore(population, variables)
    dScore = diversityScore.scoreSample(sample)[0]
    mwRej = testSampleRepresentativeness(sample, population, variables, sigLevel, 'mw')
    ksRej = testSampleRepresentativeness(sample, population, variables, sigLevel, 'ks')

    return dScore, mwRej, ksRej

def testSampleRepresentativeness(sample: pd.DataFrame, population: pd.DataFrame, variables: list[str], sigLevel: float, test: str) -> tuple[list, list]:
    variablesValues = []
    varHypRejected = []
    for variable in variables:
        testResult: any
        if test == 'ks':
            cdfFrame = stMod.ECDF(population[variable].to_numpy())
            testResult = sp.ks_1samp(sample[variable], cdfFrame)
        else:
            testResult = sp.mannwhitneyu(population[variable], sample[variable])

        variablesValues.append({'dimension': variable, 'test': 'one-sample-K-S', 'test-score': testResult[0], 'p-value': testResult[1]})
        if testResult[1] < sigLevel:
            varHypRejected.append({'dimension': variable, 'test': 'one-sample-K-S', 'test-score': testResult[0], 'p-value': testResult[1]})

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

def createKMeansSample(frame: pd.DataFrame, sampleSize: int, colName="totalSize"):

    numberOfProj = frame.shape[0]
    groups = getGroupsKMeans(frame, colName)
    sample = pd.DataFrame()

    for group in groups:
        quantity = round((group.shape[0] / numberOfProj) * sampleSize)
        sample = pd.concat([sample, group.sample(quantity, ignore_index=True)])

    if sampleSize != sample.shape[0]:
        groupSizes = [group.shape[0] for group in groups]
        indexOfLargerGroup = groupSizes.index(max(groupSizes))
        projects = groups[indexOfLargerGroup]
        if sampleSize < sample.shape[0]:
            id = sample[sample['id'].isin(projects['id'])].sample(1)['id'].values[0]
            sample = sample[sample['id'] != id]
        else:
            sample = pd.concat([sample, projects[~projects['id'].isin(sample['id'])].sample(1)])


    return sample

def getGroupsKMeans(frame: pd.DataFrame, colName="totalSize", nClusters=5):
    groups: list[pd.DataFrame] = []
    thresholds = SizeThresholds.getThresholds(frame, colName, nClusters)

    groups.append(frame[frame[colName] <= thresholds[0]])

    for i in range(1, len(thresholds)):
        projects = frame.loc[(thresholds[i-1] < frame[colName]) & (thresholds[i] >= frame[colName])]
        groups.append(projects)

    groups.append(frame[frame[colName] > thresholds[-1]])

    return groups


def characterizeSample(sample: pd.DataFrame, dimensions):
    sampleCharacteristics = np.array([])
    for dimension in dimensions:
        dimensionValues = sample[dimension]
        mode = np.argmax(np.bincount(dimensionValues))
        median = np.median(dimensionValues)
        mean = np.mean(dimensionValues)
        minimum = np.min(dimensionValues)
        maximum = np.max(dimensionValues)
        first_quartile = np.percentile(dimensionValues, 25)
        third_quartile = np.percentile(dimensionValues, 75)
        variance = np.var(dimensionValues)
        standard_deviation = np.std(dimensionValues)
        skewness = sp.skew(dimensionValues)
        kurtosis_value = sp.kurtosis(dimensionValues)
        dimensionsChar = np.array([mode, median, mean, minimum, maximum, first_quartile, third_quartile, variance, standard_deviation, skewness, kurtosis_value])
        sampleCharacteristics = np.append(sampleCharacteristics, dimensionsChar)

    return sampleCharacteristics

'''
    stochastic equality is readily seen to be equivalent to the p÷ = .5

    In the continuous case p+ > .5 clearly implies that population 1 is
    stochastically larger than population 2, and p+ < .5 implies that population 1 is
    stochastically smaller than population 2

    Small: 0.56
    Medium: 0.64
    Large: 0.71

    Vargha, A., & Delaney, H. D. (2000). A critique and improvement of the CL common language effect size statistics of McGraw and Wong. Journal of Educational and Behavioral Statistics, 25(2), 101-132.
'''
def varghaDelaney(a, b, scaled=True):
    m = len(a)
    n = len(b)
    U = sp.rankdata(np.concatenate([a, b]))
    R1 = U[:m].sum()
    R2 = U[m:].sum()
    v = (R1/m - (m+1)/2)/n
    if scaled:
        return abs((v-0.5)*2)
    else:
        return v


