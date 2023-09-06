import numpy as np
import pandas as pd
import SampleBuilder as SB
from DiversityScore import DiversityScore
from GithubGraphQL import GithubGraphQL
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


class Maintenance:

    def __init__(self,  p_dimensions: list[str]):
        #self._GQL = p_GQL
        self._dimensions = p_dimensions

    '''
    methods: DR y DT
    clustering: nagg y kmeans
    *args nClusters = 6
    '''
    def updateSample(self, frame:pd.DataFrame, sample: pd.DataFrame, method: str, clustering:str, sampleExpectedSize = 0, **args) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        sampleUpdated = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        frameWithoutUpdatedSample = frame[~frame['id'].isin(sampleUpdated['id'])].reset_index(drop=True)

        sampleSize = sampleExpectedSize
        if sampleSize == 0:
            sampleSize = SB.sampleSize(frame.shape[0])

        nClusters = args['nClusters']
        if clustering == 'kmeans':
            if method == 'DT':
                sampleUpdated = self._dynamicThresholds(frame, frameWithoutUpdatedSample, sampleUpdated, sampleSize, nClusters)
            elif method == "DR":
                sampleUpdated = self._directReplacement(sample, frame, nClusters)
        else:
            sampleUpdated = self._naggClustering(frame, frameWithoutUpdatedSample, sampleUpdated, sampleSize, method)

        '''sampleDifference = sampleSize - sampleUpdated.shape[0]
        if 0 <= sampleDifference <= pool.shape[0]:
            projects = pool.sample(sampleDifference)
        else:
            frameWithoutUpdatedAndPool = frameWithoutUpdatedSample[~frameWithoutUpdatedSample['id'].isin(pool['id'])]
            projects = pd.concat([pool, frameWithoutUpdatedAndPool.sample(sampleDifference - pool.shape[0])])

        sampleUpdated = pd.concat([sampleUpdated, projects], ignore_index=True)'''

        sampleExcluded = sample[~sample['id'].isin(sampleUpdated['id'])]
        sampleIncluded = sampleUpdated[~sampleUpdated['id'].isin(sample['id'])]

        return sampleUpdated, sampleExcluded, sampleIncluded

    def _dynamicThresholds(self, frame, frameAux, sampleUpdated, sampleSize, nClusters):
        dimensionsKeys = []
        for dimension in self._dimensions:
            dimKey = frame.columns.get_loc(dimension)
            dimensionsKeys.append(dimKey)
        analizedDimensions = frame.iloc[:, dimensionsKeys]
        scaler = StandardScaler()
        data_scaled = scaler.fit_transform(analizedDimensions)
        kmeans = KMeans(n_clusters=nClusters, init='k-means++', n_init=10)
        kmeans.fit(data_scaled)
        proportion = sampleSize / frame.shape[0]

        for i in range(nClusters):
            clusters: np.ndarray = np.where(kmeans.labels_ == i)[0]
            elementsInTheCluster: pd.DataFrame = frame.iloc[clusters, :]
            sampleFiltered = sampleUpdated[sampleUpdated['id'].isin(elementsInTheCluster['id'])]
            frameFiltered = frameAux[frameAux['id'].isin(elementsInTheCluster['id'])]
            sampleQty = sampleFiltered.shape[0]
            realQty = round(elementsInTheCluster.shape[0] * proportion)
            if realQty == 0: realQty = 1
            difference = realQty - sampleQty

            if difference >= 0:
                randElem = frameFiltered.sample(difference)
                sampleUpdated = pd.concat([sampleUpdated, randElem], ignore_index=True)
            elif sampleFiltered.shape[0] > abs(difference):
                randElem = sampleFiltered.sample(abs(difference))
                sampleUpdated = sampleUpdated[~sampleUpdated['id'].isin(randElem['id'])]

        return sampleUpdated

    def _directReplacement(self, sample, frame, clusters):
        dimensionsKeys = []
        for dimension in self._dimensions:
            dimKey = sample.columns.get_loc(dimension)
            dimensionsKeys.append(dimKey)
        scaler = StandardScaler()
        data_scaled = scaler.fit_transform(sample.iloc[:, dimensionsKeys])
        kmeans = KMeans(n_clusters=clusters, init='k-means++', n_init=10)
        kmeans.fit(data_scaled)

        oldProj = sample[~sample['id'].isin(frame['id'])]
        sampleUpdated = sample[sample['id'].isin(frame['id'])].reset_index(drop=True)
        newProj = frame[~frame['id'].isin(sampleUpdated['id'])].reset_index(drop=True)
        data_scaled = scaler.fit_transform(newProj.iloc[:, dimensionsKeys])
        predictedValues = kmeans.predict(data_scaled)

        for i in range(clusters):
            projectsIds: np.ndarray = np.where(kmeans.labels_ == i)[0]
            numberToElim = oldProj[oldProj.index.isin(projectsIds)].shape[0]
            projectsIds: np.ndarray = np.where(predictedValues == i)[0]
            toInclude = newProj[newProj.index.isin(projectsIds)]
            numberToIncl = toInclude.shape[0]
            if numberToElim <= numberToIncl:
                sampleUpdated = pd.concat([sampleUpdated, toInclude.sample(numberToElim)])
            else:
                sampleUpdated = pd.concat([sampleUpdated, toInclude])

        return sampleUpdated


    def _naggClustering(self, frame, frameAux, sampleUpdated, sampleSize, method: str):
        diverseSample = SB.createDiverseSample(frame, self._dimensions)
        DS = DiversityScore(frame, self._dimensions)
        groups, outliers = DS.clusterizePopulation(diverseSample, frame)
        diverseSample = diverseSample.drop('matrix', axis=1)
        groups = SB.generateGroupsOutput(groups, sampleUpdated)
        proportion = (sampleSize - len(groups)) / (frame.shape[0] - len(groups))
        groupsDF = pd.DataFrame(groups)

        #pool = pd.DataFrame()

        for id, group in groupsDF.iterrows():
            elementsInTheGroup = pd.concat([pd.DataFrame(group['similarProjects'], columns=frame.columns),
                                            pd.DataFrame(diverseSample.iloc[id]).T], ignore_index=True)
            sampleFiltered = sampleUpdated[sampleUpdated['id'].isin(elementsInTheGroup['id'])].copy()
            frameFiltered = frameAux[frameAux['id'].isin(elementsInTheGroup['id'])].copy()

            sampleQty = sampleFiltered.shape[0]
            realQty = (group['groupQty'] - 1) * proportion + 1
            propQty = round(realQty)
            difference = propQty - sampleQty

            if difference > 0:
                randElem = frameFiltered.sample(difference)
                sampleUpdated = pd.concat([sampleUpdated, randElem], ignore_index=True)
            elif sampleFiltered.shape[0] > 0 and method == 'DT':
                randElem = sampleFiltered.sample(difference * -1)
                sampleUpdated = sampleUpdated[~sampleUpdated['id'].isin(randElem['id'])]

        return sampleUpdated