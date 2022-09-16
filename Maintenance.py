import pandas as pd
import SampleBuilder as SB
from DiversityScore import DiversityScore
import scipy.stats as sp
import statsmodels.distributions.empirical_distribution as stMod
from GithubGraphQL import GithubGraphQL


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
        repoDataQuery = self._GQL.readFile("./APIQueries/repositoryUpdate")
        frameFiltered = frame[frame['dateLastCommit'] < self._dateLastCommit]
        for id, project in frameFiltered.iterrows():
            jsonResponse = self._updateProject(project, repoDataQuery)
            if jsonResponse:
                newRow = pd.Series(jsonResponse)
                frame.loc[id] = newRow

            else:
                frame.drop(id, inplace=True)

        return frame





    def updateSampleDTDQ(self, frame:pd.DataFrame, sample: pd.DataFrame, ksScore = 0.2) -> pd.DataFrame:

        sampleUpdated = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)
        sampleAux = pd.DataFrame
        representative = False

        diverseSample = SB.createDiverseSample(frame, self._dimensions)
        sampleArray = diverseSample.to_numpy()
        populationArray = frame.to_numpy()

        DS = DiversityScore(frame, self._dimensions)
        groups, outliers = DS.clusterizePopulation(sampleArray, populationArray)
        groups = SB.generateGroupsOutput(groups)
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
                    randElem = pd.concat([frameFiltered.sample(difference - 1)], ignore_index=True)
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