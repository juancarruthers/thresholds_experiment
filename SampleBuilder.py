import pandas as pd
import SizeThresholds


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




