import random

import numpy as np
import pandas as pd
import SampleBuilder as SB
from DiversityScore import DiversityScore
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
from GithubGraphQL import GithubGraphQL
import Utilities as util


class Maintenance:

    def __init__(self, p_GQL: GithubGraphQL, p_dimensions: list[str], p_dateLastCommit: str):
        self._GQL = p_GQL
        self._dimensions = p_dimensions
        self._dateLastCommit = p_dateLastCommit

    def _updateProject(self, project, repoDataQuery)-> bool | dict:
        variables = {'repoName': project['name'], 'owner': project['owner']}
        repoUpdate = {'query': repoDataQuery, 'variables': variables}
        jsonResponse = self._GQL.makeRequest(repoUpdate)['data']['repository']
        self._GQL.updateLanguage(jsonResponse, False)
        result = self._GQL.updateCommits(jsonResponse, False)

        if result:
            return False

        else:
            jsonResponse["owner"] = project['owner']
            self._GQL.updateIssues(jsonResponse, project['owner'], project['name'], False)
            self._GQL.updatePullRequests(jsonResponse, project['owner'], project['name'], False)
            self._GQL.updateContributors(jsonResponse, project['owner'], project['name'], False)
            return jsonResponse


    def updateFrame(self, frame: pd.DataFrame) -> pd.DataFrame:
        repoDataQuery = util.readFile("./APIQueries/repositoryUpdate")
        frameFiltered = frame[frame['dateLastCommit'] < self._dateLastCommit]
        for id, project in frameFiltered.iterrows():
            jsonResponse = self._updateProject(project, repoDataQuery)
            if jsonResponse:
                newRow = pd.Series(jsonResponse)
                frame.loc[id] = newRow

            else:
                frame.drop(id, inplace=True)

        return frame


    def updateSample(self, frame:pd.DataFrame, sample: pd.DataFrame, ksScore = 0.2) -> pd.DataFrame:
        sampleUpdated: pd.DataFrame = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleExcluded: pd.DataFrame = sample[~sample['id'].isin(sampleUpdated['id'])].reset_index(drop=True)
        frameWithOutUpdated: pd.DataFrame = frame[~frame['id'].isin(sampleUpdated['id'])].reset_index(drop=True)

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

            sampleSize = SB.sampleSize(frame.shape[0])
            completeSample = frameWithOutUpdatedAux.sample(sampleSize - sampleUpdatedAux.shape[0])
            sampleUpdatedAux = pd.concat([sampleUpdatedAux, completeSample], ignore_index=True)

            representative = self.testRepresentativeness(sampleUpdatedAux, frame, ksScore)

        return sampleUpdatedAux

    def updateSampleST(self, frame:pd.DataFrame, sample: pd.DataFrame, groups: pd.DataFrame, ksScore = 0.2, STQ = 'dynamic') -> pd.DataFrame:

        sampleUpdated = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleAux = pd.DataFrame
        representative = False

        while not (representative):
            frameAux = frame.copy()
            sampleAux = sampleUpdated.copy()

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
                    frameQuantity = round(frameFiltered.shape[0] * 0.2)
                    if frameQuantity == 0:
                        frameQuantity = 1

                sampleQuantity = sampleFiltered.shape[0]
                difference = frameQuantity - sampleQuantity

                if difference > 0:
                    if frameFiltered.shape[0] < difference:
                        difference = frameFiltered.shape[0]
                    frameFiltered = frameFiltered[~frameFiltered['id'].isin(sampleFiltered['id'])]
                    randElem = frameFiltered.sample(difference)
                    sampleAux = pd.concat([sampleAux, randElem], ignore_index=True)
                else:
                    randElem = sampleFiltered.sample(difference * -1)
                    sampleAux = sampleAux[~sampleAux['id'].isin(randElem['id'])]

                frameAux = frameAux[~frameAux['id'].isin(elementsInTheGroup['id'])]

            representative = self.testRepresentativeness(sampleAux, frame, ksScore)

        return sampleAux


    def updateSampleDTDQ(self, frame:pd.DataFrame, sample: pd.DataFrame, ksScore = 0.2) -> pd.DataFrame:

        sampleUpdated = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleAux = pd.DataFrame
        representative = False

        diverseSample = SB.createDiverseSample(frame, self._dimensions)
        sampleArray = diverseSample.to_numpy()
        populationArray = frame.to_numpy()

        DS = DiversityScore(frame, self._dimensions)
        groups, outliers = DS.clusterizePopulation(sampleArray, populationArray)
        proportion = SB.sampleSize(frame.shape[0]) / frame.shape[0]
        groups = SB.generateGroupsOutput(groups, proportion)
        groupsDF = pd.DataFrame(groups)

        while not (representative):

            sampleAux = sampleUpdated[~sampleUpdated['id'].isin(diverseSample['id'])].copy()
            frameAux = frame[~frame['id'].isin(diverseSample['id'])].copy()

            for id, group in groupsDF.iterrows():
                elementsInTheGroup = pd.DataFrame(group['similarProjects'], columns=frame.columns)
                sampleFiltered = sampleAux[sampleAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameAux[frameAux['id'].isin(elementsInTheGroup['id'])].copy()
                frameFiltered = frameFiltered[~frameFiltered['id'].isin(sampleFiltered['id'])]

                quantity = sampleFiltered.shape[0]
                difference = int(group['sampleQty'] - quantity)

                projDiv = diverseSample.iloc[[id]]
                if difference > 0:
                    randElem = frameFiltered.sample(difference - 1)
                    sampleAux = pd.concat([sampleAux, randElem], ignore_index=True)
                else:
                    randElem = sampleFiltered.sample((difference - 1) * -1)
                    randElem = randElem[projDiv['id'].item() != randElem['id']]
                    sampleAux = sampleAux[~sampleAux['id'].isin(randElem['id'])]

                sampleAux = pd.concat([sampleAux, projDiv], ignore_index=True)

            representative = self.testRepresentativeness(sampleAux, frame, ksScore)

        return sampleAux

    def testRepresentativeness (self, sample, population, ksScore = 0.2) -> bool:

        for variable in self._dimensions:
            cdfFrame = stMod.ECDF(population[variable].to_numpy())
            ks = sp.ks_1samp(sample[variable], cdfFrame)
            if ks[1] < ksScore:
                return False
        return True