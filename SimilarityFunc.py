import statistics as st
import math
import numpy as np

def numericDimensionSimilaritySD(projectSample: int | float, projectsPopulation: np.ndarray) -> tuple[
    list[bool], list[float, float]]:
    stddev = st.stdev(projectsPopulation)
    lower = projectSample - stddev
    upper = projectSample + stddev
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(lower <= project <= upper)
    return similarityScore, [lower, upper]


def numericDimensionSimilarity(projectSample, projectsPopulation: np.ndarray) -> tuple[
    list[bool], list[float, float]]:
    lower = pow(10, math.log10(projectSample + 1) - 0.5) - 1
    upper = pow(10, math.log10(projectSample + 1) + 0.5) - 1
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(lower <= project <= upper)
    return similarityScore, [lower, upper]


def factorDimensionSimilarity(projectSample: str, projectsPopulation: np.ndarray) -> tuple[list[bool], list[str]]:
    similarityScore = []
    for project in projectsPopulation:
        similarityScore.append(projectSample == project)
    return similarityScore, [projectSample]

