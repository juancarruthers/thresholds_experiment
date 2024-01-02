import numpy as np
import pandas as pd
import SampleBuilder as SB
from scipy.spatial.distance import euclidean

class Maintenance:

    def __init__(self,  p_dimensions: list[str]):
        self._dimensions = p_dimensions


    def updateSample(self, frame:pd.DataFrame, sample: pd.DataFrame, method: str, sampleExpectedSize: int, **args) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        sampleUpdated = frame[frame['id'].isin(sample['id'])].reset_index(drop=True)

        if method == 'DR':
            projectsToUpdate = sample[~sample['id'].isin(frame['id'])]
            replacements = self.directReplacement(sampleUpdated, frame, projectsToUpdate)
            sampleUpdated = pd.concat([sampleUpdated, replacements])

        elif method == 'DT':
            sampleUpdated = self._kMeansThresholds(frame, sampleUpdated, sampleExpectedSize, 'totalSize', **args)

        sampleExcluded = sample[~sample['id'].isin(sampleUpdated['id'])]
        sampleIncluded = sampleUpdated[~sampleUpdated['id'].isin(sample['id'])]

        return sampleUpdated, sampleExcluded, sampleIncluded


    def _kMeansThresholds(self, frame: pd.DataFrame, sampleUpdated:pd.DataFrame, sampleSize: int, colName= 'totalSize', nClusters=5):
        numberOfProj = frame.shape[0]
        popGroups = SB.getGroupsKMeans(frame, colName, nClusters)

        for i in range(len(popGroups)):
            proportion = round((popGroups[i].shape[0] / numberOfProj) * sampleSize)
            samGroup = sampleUpdated[sampleUpdated['id'].isin(popGroups[i]['id'])]
            difference = samGroup.shape[0] - proportion
            if difference > 0:
                ids = sampleUpdated[sampleUpdated['id'].isin(samGroup['id'])].sample(difference)
                sampleUpdated = sampleUpdated[~sampleUpdated['id'].isin(ids)]
            else:
                newProj = popGroups[i][~popGroups[i]['id'].isin(sampleUpdated['id'])].sample(difference * -1)
                sampleUpdated = pd.concat([sampleUpdated, newProj])

        if sampleSize != sampleUpdated.shape[0]:
            groupSizes = [group.shape[0] for group in popGroups]
            indexOfLargerGroup = groupSizes.index(max(groupSizes))
            projects = popGroups[indexOfLargerGroup]
            if sampleSize < sampleUpdated.shape[0]:
                id = sampleUpdated[sampleUpdated['id'].isin(projects['id'])].sample(1)['id'].values[0]
                sampleUpdated = sampleUpdated[sampleUpdated['id'] != id]
            else:
                sampleUpdated = pd.concat([sampleUpdated, projects[~projects['id'].isin(sampleUpdated['id'])].sample(1)])

        return sampleUpdated

    def directReplacement(self, sampleUpdated, frame, projectsToUpdate, order=0):

        dimensionsKeys = []
        for dimension in self._dimensions:
            dimKey = sampleUpdated.columns.get_loc(dimension)
            dimensionsKeys.append(dimKey)

        columns = sampleUpdated.columns.tolist()
        replacements = pd.DataFrame()
        frame = frame[~frame['id'].isin(sampleUpdated['id'])]
        frameArray = frame.values


        for id, project in projectsToUpdate.iterrows():

            distances = np.apply_along_axis(lambda row: euclidean(row[dimensionsKeys], project[self._dimensions].values), 1, frameArray)
            distances = distances.reshape(-1, 1)
            sorted = np.concatenate((frameArray, distances), axis=1)
            sorted = sorted[sorted[:, -1].argsort()]
            similar = sorted[order, :-1]
            similar = similar.reshape(1, -1)
            frameArray = sorted[1:, :-1]

            replacements = pd.concat([replacements, pd.DataFrame(similar, columns=columns)])

        for column in replacements.columns:
            replacements[column] = replacements[column].astype(sampleUpdated[column].dtypes)

        return replacements
