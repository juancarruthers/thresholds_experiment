import pandas as pd
import SampleBuilder as SB
from DiversityScore import DiversityScore
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
from GithubGraphQL import GithubGraphQL


class Maintenance:

    def __init__(self, p_GQL: GithubGraphQL, p_dimensions: list[str]):
        self._GQL = p_GQL
        self._dimensions = p_dimensions


    def updateSampleDR(self, frame:pd.DataFrame, sample: pd.DataFrame, sampleExpectedSize = 0, ksScore = 0.05) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        sampleUpdated: pd.DataFrame = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleExcluded: pd.DataFrame = sample[~sample['id'].isin(sampleUpdated['id'])].reset_index(drop=True)
        frameWithOutUpdated: pd.DataFrame = frame[~frame['id'].isin(sampleUpdated['id'])].reset_index(drop=True)

        sampleSize = sampleExpectedSize
        if sampleExpectedSize == 0:
            sampleSize = SB.sampleSize(frame.shape[0])

        DS = DiversityScore(frameWithOutUpdated, self._dimensions)

        dimensionsKeysSam = []
        dimensionsKeysPop = []
        similar = []
        for dimension in self._dimensions:
            dimensionsKeysSam.append(sample.columns.get_loc(dimension))
            dimensionsKeysPop.append(frameWithOutUpdated.columns.get_loc(dimension))

        sampArray = sampleExcluded.to_numpy()
        frameWithOutUpdatedArray = frameWithOutUpdated.to_numpy()

        for project in sampArray:
            projScore = DS.scoreProject(project, frameWithOutUpdated.shape[0], len(self._dimensions), dimensionsKeysSam, dimensionsKeysPop, frameWithOutUpdatedArray)[1]
            projScoreDF = pd.DataFrame(projScore)
            similarProj = frameWithOutUpdated[frameWithOutUpdated.index.isin(projScoreDF[projScoreDF[0]].index.values)].reset_index(drop=True)
            similar.append(similarProj)

        representative = False
        sampleUpdatedAux = pd.DataFrame()
        while not (representative):
            sampleUpdatedAux = sampleUpdated.copy()
            frameWithOutUpdatedAux = frameWithOutUpdated.copy()
            for projects in similar:
                projects = projects[~projects['id'].isin(sampleUpdatedAux['id'])]
                if projects.shape[0] > 0:
                    randProj = projects.sample(1, ignore_index=True)
                else:
                    randProj = frameWithOutUpdatedAux.sample(1, ignore_index=True)
                sampleUpdatedAux = pd.concat([sampleUpdatedAux, randProj], ignore_index=True)
                frameWithOutUpdatedAux = frameWithOutUpdatedAux[frameWithOutUpdatedAux['id'] != randProj['id'][0]]

            sizeCorrection = sampleSize - sampleUpdatedAux.shape[0]

            if sizeCorrection >= 0:
                completeSample = frameWithOutUpdatedAux.sample(sizeCorrection)
                sampleUpdatedAux = pd.concat([sampleUpdatedAux, completeSample], ignore_index=True)
            else:
                projects = sampleUpdatedAux.sample(sizeCorrection * -1)
                sampleUpdatedAux = sampleUpdatedAux[~sampleUpdatedAux['id'].isin(projects['id'])]

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
        sampleArray = diverseSample.to_numpy()
        frameWithoutDiverse = frame[~frame['id'].isin(diverseSample['id'])]
        populationArray = frameWithoutDiverse.to_numpy()

        DS = DiversityScore(frame, self._dimensions)

        groups, outliers = DS.clusterizePopulation(sampleArray, populationArray)
        groups = SB.generateGroupsOutput(groups, sampleUpdated)
        proportion = (sampleSize - len(groups)) / (frame.shape[0] - len(groups))
        groupsDF = pd.DataFrame(groups)

        while not (representative):
            sampleAux = sampleUpdated[~sampleUpdated['id'].isin(diverseSample['id'])].copy()
            frameAux = frameWithoutDiverse.copy()
            remainings = pd.DataFrame()

            for id, group in groupsDF.iterrows():
                elementsInTheGroup = pd.DataFrame(group['similarProjects'], columns=frame.columns)
                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameAux[frameAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameFiltered[~frameFiltered['id'].isin(sampleFiltered['id'])]

                sampleQty = sampleFiltered.shape[0]
                realQty = (group['groupQty'] - 1) * proportion
                propQty = round(realQty)
                difference = propQty - sampleQty
                '''
                if (realQty > propQty):
                    if difference > 0:
                        proj = frameFiltered.sample(1)
                        frameFiltered = frameFiltered[frameFiltered['id'] != proj['id'].item()]
                        remainings = pd.concat([remainings, proj], ignore_index=True)
                    elif difference < 0:
                        proj = sampleFiltered.sample(1)
                        sampleFiltered = sampleFiltered[sampleFiltered['id'] != proj['id'].item()]
                        remainings = pd.concat([remainings, proj], ignore_index=True)
                        difference += 1
                '''

                if difference > 0:
                    randElem = frameFiltered.sample(difference)
                    sampleAux = pd.concat([sampleAux, randElem], ignore_index=True)
                elif sampleFiltered.shape[0] > 0:
                    randElem = sampleFiltered.sample(difference * -1)
                    sampleAux = sampleAux[~sampleAux['id'].isin(randElem['id'])]
            '''
            sizeCorrection = sampleSize - (sampleAux.shape[0] + diverseSample.shape[0])
            if (sizeCorrection > 0):
                projects = remainings.sample(sizeCorrection, ignore_index=True)
                sampleAux = pd.concat([sampleAux, projects], ignore_index=True)
            elif (sizeCorrection < 0):
                projects = sampleAux.sample(sizeCorrection * -1)
                sampleAux = sampleAux[~sampleAux['id'].isin(projects['id'])]
            '''

            sampleAux = pd.concat([sampleAux, diverseSample], ignore_index=True)
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