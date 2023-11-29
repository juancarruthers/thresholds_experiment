import os
import subprocess
from datetime import datetime


class SourceMeter:
    def __init__(self, resultsDir: str, toolPath: str, projPath: str, numberOfThreads = 1):
        self._resultsDir = os.path.abspath(resultsDir)
        self._toolPath = os.path.abspath(toolPath)
        self._projectsPath = os.path.abspath(projPath)
        self._threadsNumber = numberOfThreads

    def analyze(self, projectName, projectFolder):


        command = f'{self._toolPath} -projectName={projectName} -projectBaseDir={self._projectsPath}/{projectFolder} -resultsDir={self._resultsDir} ' \
                  f'-runPMD=false -runFB=false -runAndroidHunter=false -runVulnerabilityHunter=false -runFaultHunter=false ' \
                  f'-runMetricHunter=false -runRTEHunter=false -runDCF=false -runMET=true -runUDM=false ' \
                  f'-runLIM2Patterns=false -maximumThreads={self._threadsNumber}'

        try:
            stdout = subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True, check=True, text=True,
                                shell=True).stdout
        except Exception as error:
            print(error)
            self.logAnalysisError(projectName)
            raise Exception("Analysis execution error")


    def logAnalysisError(self, name):
        logFilePath = os.path.abspath(f'{self._resultsDir}/error-log.txt')
        if not os.path.exists(logFilePath):
            open(logFilePath, "x")

        with open(logFilePath, "a") as file:
            file.write(f"{datetime.now()} - Error analyzing {name}\n")

    def getResultsDir(self):
        return self._resultsDir

