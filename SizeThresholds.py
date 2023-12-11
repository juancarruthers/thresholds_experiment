import numpy as np
import pandas as pd
from sklearn.cluster import KMeans


def getThresholds(data, dimension='totalSize', nClusters=5):

    groups = []
    outlierBoundValue = upperBound(data[dimension])
    dataWithoutOutliers = data[data[dimension] < outlierBoundValue]
    vector = dataWithoutOutliers[dimension].values.reshape(-1, 1)
    kmeans = KMeans(n_clusters=nClusters, init='k-means++', n_init=10)
    kmeans.fit(vector)

    for id in range(nClusters):
        cluster: np.ndarray = np.where(kmeans.labels_ == id)[0]
        elementsInTheCluster = vector[cluster, :].reshape(1, -1)[0]
        groups.append(max(elementsInTheCluster))

    groups.sort()
    return groups


# MIN-MAX NORMALIZATION, paper "Qualitative Clustering of Software Repositories Based on Software Metrics"
def dataNormalization(data: np.ndarray):
    minVal = min(data)
    maxVal = max(data)

    return (data - minVal) / (maxVal - minVal)

# ALVES ET AL. OUTLIER DETECTION
def upperBound(data: np.ndarray):
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)
    IQR = Q3 - Q1

    upper_bound = Q3 + 1.5 * IQR

    return upper_bound

def getOutliers(dataset, dimensions):
    outliers = pd.DataFrame()
    for dimension in dimensions:
        aux = systemsThresholds(dataset, dimension)
        threshold = upperBound(aux['90th'])
        aux = aux[aux['90th'] >= threshold]
        if outliers.shape[0] == 0:
            outliers = aux.copy()
        else:
            outliers = pd.concat([outliers, aux])

    return outliers['System'].drop_duplicates()


def systemsThresholds(dataset, metric):
    thresholds = pd.DataFrame()

    totalLocBySystem = dataset.groupby('Repository')['LOC'].sum()
    dataset = dataset.join(totalLocBySystem, on='Repository', rsuffix='_total')
    dataset['LOC_weights'] = dataset['LOC'] / (dataset['LOC_total'])
    entitiesAgg = dataset.groupby(['Repository', metric])['LOC_weights'].sum().reset_index()
    systems = totalLocBySystem.shape[0]
    entitiesAgg['LOC_weights_norm'] = entitiesAgg['LOC_weights'] / systems

    for system in totalLocBySystem.items():
        systemValues = entitiesAgg[entitiesAgg['Repository'] == system[0]].copy()
        systemValues = systemValues.sort_values(by=metric)
        systemValues['LOC_weights_cum'] = systemValues['LOC_weights'].cumsum().to_frame()

        p70 = systemValues[systemValues['LOC_weights_cum'] >= 0.70][metric].values[0]
        p80 = systemValues[systemValues['LOC_weights_cum'] >= 0.80][metric].values[0]
        p90 = systemValues[systemValues['LOC_weights_cum'] >= 0.90][metric].values[0]
        systemThresholds = pd.DataFrame([{'System': system[0], '70th': p70, '80th': p80, '90th': p90}])
        thresholds = pd.concat([thresholds, systemThresholds])

    return thresholds