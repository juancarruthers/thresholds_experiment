# defining the libraries
import pandas as pd
import numpy as np
from numpy import ndarray
from scipy import stats
from statsmodels.distributions.empirical_distribution import ECDF

def createSimpleRandomSample(dataset, proportion=0.2):
    sample = dataset.sample(int(len(dataset) * proportion))
    sample.to_csv("./datasets/Larger/sampleRand.csv")

def testKS(framePath: str, samplePath: str, variables: list[str]):

    frame = pd.read_csv(framePath)
    sample = pd.read_csv(samplePath)

    for variable in variables:
        cdfFrame = ECDF(frame[variable].to_numpy())
        ks = stats.ks_1samp(sample[variable], cdfFrame)
        print(variable + " - p-value: " + str(ks[1]))

if __name__ == '__main__':
    testKS("./datasets/Larger/stratified.csv", "./datasets/Larger/sampleEnginRand.csv", ['stargazerCount', 'forkCount', 'issues', 'totalSize', 'pullReqCount', 'commits'])

