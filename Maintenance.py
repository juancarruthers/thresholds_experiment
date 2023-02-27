import numpy as np
import pandas as pd
import SampleBuilder as SB
from DiversityScore import DiversityScore
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
from GithubGraphQL import GithubGraphQL
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


class Maintenance:

    def __init__(self, p_GQL: GithubGraphQL, p_dimensions: list[str]):
        self._GQL = p_GQL
        self._dimensions = p_dimensions


    def updateSampleDR(self, frame:pd.DataFrame, sample: pd.DataFrame, sampleExpectedSize = 0, ksScore = 0.05) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        sampleUpdated: pd.DataFrame = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        frameWithoutUpdatedSample: pd.DataFrame = frame[~frame['id'].isin(sampleUpdated['id'])].reset_index(drop=True)

        sampleSize = sampleExpectedSize
        if sampleSize == 0:
            sampleSize = SB.sampleSize(frame.shape[0])

        sampleDifference = sampleSize - sampleUpdated.shape[0]
        diverseSample = SB.createDiverseSample(frame, self._dimensions)

        representative = False
        sampleUpdatedAux = pd.DataFrame()
        DS = DiversityScore(frame, self._dimensions)

        groups, outliers = DS.clusterizePopulation(diverseSample, frame)
        diverseSample = diverseSample.drop('matrix', axis=1)
        groups = SB.generateGroupsOutput(groups, sampleUpdated)
        proportion = (sampleSize - len(groups)) / (frame.shape[0] - len(groups))
        groupsDF = pd.DataFrame(groups)

        while not (representative):
            sampleAux = sampleUpdated.copy()
            frameAux = frameWithoutUpdatedSample.copy()
            pool = pd.DataFrame()

            for id, group in groupsDF.iterrows():
                elementsInTheGroup = pd.concat([pd.DataFrame(group['similarProjects'], columns=frame.columns),
                                                pd.DataFrame(diverseSample.iloc[id]).T], ignore_index=True)
                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameAux[frameAux['id'].isin(elementsInTheGroup['id'])].copy()

                sampleQty = sampleFiltered.shape[0]
                realQty = (group['groupQty'] - 1) * proportion + 1
                propQty = round(realQty)
                difference = propQty - sampleQty

                if difference > 0:
                    randElem = frameFiltered.sample(difference)
                    pool = pd.concat([pool, randElem], ignore_index=True)

            projects = pool.sample(sampleDifference)
            sampleUpdatedAux = pd.concat([sampleAux, projects], ignore_index=True)
            representative = self.testRepresentativeness(sampleUpdatedAux, frame, ksScore)

        sampleExcluded = sample[~sample['id'].isin(sampleUpdatedAux['id'])]
        sampleIncluded = sampleUpdatedAux[~sampleUpdatedAux['id'].isin(sample['id'])]

        return sampleUpdatedAux, sampleExcluded, sampleIncluded

    def updateSampleDRKMeans(self, frame:pd.DataFrame, sample: pd.DataFrame, sampleExpectedSize = 0, nClusters=6, ksScore = 0.05) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        dimensionsKeys = []
        for dimension in self._dimensions:
            dimKey = frame.columns.get_loc(dimension)
            dimensionsKeys.append(dimKey)

        analizedDimensions = frame.iloc[:, dimensionsKeys]

        sampleUpdated: pd.DataFrame = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        frameWithoutUpdatedSample: pd.DataFrame = frame[~frame['id'].isin(sampleUpdated['id'])].reset_index(drop=True)

        sampleSize = sampleExpectedSize
        if sampleSize == 0:
            sampleSize = SB.sampleSize(frame.shape[0])

        sampleDifference = sampleSize - sampleUpdated.shape[0]

        representative = False
        sampleUpdatedAux = pd.DataFrame()

        scaler = StandardScaler()
        data_scaled = scaler.fit_transform(analizedDimensions)
        kmeans = KMeans(n_clusters=nClusters, init='k-means++')
        kmeans.fit(data_scaled)
        proportion = sampleSize / frame.shape[0]

        while not (representative):
            sampleAux = sampleUpdated.copy()
            frameAux = frameWithoutUpdatedSample.copy()
            pool = pd.DataFrame()

            for i in range(nClusters):
                clusters: np.ndarray = np.where(kmeans.labels_ == i)[0]
                elementsInTheCluster: pd.DataFrame = frame.iloc[clusters, :]
                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheCluster['id'])]
                frameFiltered = frameAux[frameAux['id'].isin(elementsInTheCluster['id'])]
                sampleQty = sampleFiltered.shape[0]
                realQty = round(elementsInTheCluster.shape[0] * proportion)
                if realQty == 0: realQty = 1
                difference = realQty - sampleQty

                if difference > 0:
                    randElem = frameFiltered.sample(difference).copy()
                    pool = pd.concat([pool, randElem], ignore_index=True)

            projects = pool.sample(sampleDifference)
            sampleUpdatedAux = pd.concat([sampleAux, projects], ignore_index=True)
            representative = self.testRepresentativeness(sampleUpdatedAux, frame, ksScore)

        sampleExcluded = sample[~sample['id'].isin(sampleUpdatedAux['id'])]
        sampleIncluded = sampleUpdatedAux[~sampleUpdatedAux['id'].isin(sample['id'])]

        return sampleUpdatedAux, sampleExcluded, sampleIncluded

    def updateSampleST(self, frame:pd.DataFrame, sample: pd.DataFrame, groups: pd.DataFrame, sampleExpectedSize = 0, ksScore = 0.05, STQ = 'dynamic') -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        sampleUpdated: pd.DataFrame = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleOutput = pd.DataFrame

        sampleSize = sampleExpectedSize
        if sampleSize == 0:
            sampleSize = SB.sampleSize(frame.shape[0])
        proportion = (sampleSize - groups.shape[0]) / (frame.shape[0] - groups.shape[0])

        representative = False

        while not (representative):
            frameAux = frame.copy()
            sampleAux = sampleUpdated.copy()
            sampleOutput = pd.DataFrame()

            for id, group in groups.iterrows():
                elementsInTheGroup = frameAux.copy()

                for dimension in self._dimensions:
                    elementsInTheGroup = elementsInTheGroup[group[dimension + 'Min'] <= elementsInTheGroup[dimension]]
                    elementsInTheGroup = elementsInTheGroup[group[dimension + 'Max'] >= elementsInTheGroup[dimension]]

                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameAux[frameAux['id'].isin(elementsInTheGroup['id'])].copy()

                frameQuantity = 0
                if STQ == 'static':
                    frameQuantity = int(group['sampleQty'])
                elif STQ == 'dynamic':
                    frameQuantity = round(frameFiltered.shape[0] * proportion)
                    if frameQuantity == 0:
                        frameQuantity = 1

                sampleOutput = pd.concat([sampleOutput, sampleFiltered], ignore_index=True)
                sampleQuantity = sampleFiltered.shape[0]
                difference = frameQuantity - sampleQuantity

                if difference > 0:
                    frameFiltered = frameFiltered[~frameFiltered['id'].isin(sampleFiltered['id'])]
                    if frameFiltered.shape[0] < difference:
                        difference = frameFiltered.shape[0]
                    randElem = frameFiltered.sample(difference)
                    sampleOutput = pd.concat([sampleOutput, randElem], ignore_index=True)
                elif difference < 0:
                    randElem = sampleFiltered.sample(difference * -1)
                    sampleOutput = sampleOutput[~sampleOutput['id'].isin(randElem['id'])]


                frameAux = frameAux[~frameAux['id'].isin(elementsInTheGroup['id'])]
                sampleAux = sampleAux[~sampleAux['id'].isin(elementsInTheGroup['id'])]

            representative = self.testRepresentativeness(sampleOutput, frame, ksScore)

        sampleExcluded = sample[~sample['id'].isin(sampleOutput['id'])]
        sampleIncluded = sampleOutput[~sampleOutput['id'].isin(sample['id'])]

        return sampleOutput, sampleExcluded, sampleIncluded


    def updateSampleDTDQ(self, frame:pd.DataFrame, sample: pd.DataFrame, sampleExpectedSize = 0, ksScore = 0.05) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        sampleUpdated: pd.DataFrame = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleSize = sampleExpectedSize
        if sampleSize == 0:
            sampleSize = SB.sampleSize(frame.shape[0])

        sampleAux = pd.DataFrame
        representative = False

        diverseSample = SB.createDiverseSample(frame, self._dimensions)
        frameWithoutUpdatedSample = frame[~frame['id'].isin(sampleUpdated['id'])]

        DS = DiversityScore(frame, self._dimensions)

        groups, outliers = DS.clusterizePopulation(diverseSample, frame)
        diverseSample = diverseSample.drop('matrix', axis=1)
        groups = SB.generateGroupsOutput(groups, sampleUpdated)
        proportion = (sampleSize - len(groups)) / (frame.shape[0] - len(groups))
        groupsDF = pd.DataFrame(groups)

        while not (representative):
            sampleAux = sampleUpdated.copy()
            frameAux = frameWithoutUpdatedSample.copy()
            remainings = pd.DataFrame()

            for id, group in groupsDF.iterrows():
                elementsInTheGroup = pd.concat([pd.DataFrame(group['similarProjects'], columns=frame.columns), pd.DataFrame(diverseSample.iloc[id]).T], ignore_index=True)
                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameAux[frameAux['id'].isin(elementsInTheGroup['id'])].copy()

                sampleQty = sampleFiltered.shape[0]
                realQty = (group['groupQty'] - 1) * proportion + 1
                propQty = round(realQty)
                if propQty == 1:
                    realQty = 1
                difference = propQty - sampleQty

                if difference > 0:
                    randElem = frameFiltered.sample(difference)
                    sampleAux = pd.concat([sampleAux, randElem], ignore_index=True)
                elif sampleFiltered.shape[0] > 0:
                    randElem = sampleFiltered.sample(difference * -1)
                    sampleAux = sampleAux[~sampleAux['id'].isin(randElem['id'])]

                project = pd.DataFrame()

                if (realQty < propQty):
                    project = elementsInTheGroup[elementsInTheGroup['id'].isin(sampleAux['id'])].sample(1)
                elif (realQty > propQty):
                    project = elementsInTheGroup[~elementsInTheGroup['id'].isin(sampleAux['id'])].sample(1)

                remainings = pd.concat([remainings, project], ignore_index=True)

            sizeCorrection = sampleSize - sampleAux.shape[0]
            frameAux = frameAux[~frameAux['id'].isin(sample['id'])].copy()

            if (sizeCorrection > 0):
                remainings = remainings[remainings['id'].isin(frameAux['id'])]
                projects = remainings.sample(sizeCorrection, ignore_index=True)
                sampleAux = pd.concat([sampleAux, projects], ignore_index=True)
            elif (sizeCorrection < 0):
                remainings = remainings[remainings['id'].isin(sampleAux['id'])]
                projects = remainings.sample(sizeCorrection * -1)
                sampleAux = sampleAux[~sampleAux['id'].isin(projects['id'])]


            representative = self.testRepresentativeness(sampleAux, frame, ksScore)

        sampleExcluded = sample[~sample['id'].isin(sampleAux['id'])]
        sampleIncluded = sampleAux[~sampleAux['id'].isin(sample['id'])]

        return sampleAux, sampleExcluded, sampleIncluded

    def testRepresentativeness (self, sample, population, ksScore) -> bool:

        for variable in self._dimensions:
            cdfFrame = stMod.ECDF(population[variable].to_numpy())
            ks = sp.ks_1samp(sample[variable], cdfFrame)
            if ks[1] < ksScore:
                return False
        return True