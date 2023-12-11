import os
from datetime import datetime
import pandas as pd
import concurrent.futures
from pathlib import Path
import psutil
from git import Repo
import tarfile
import zipfile
from SourceMeter import SourceMeter
from Utilities import Utilities

class DatasetGenerator:
    def __init__(self, analyzer: SourceMeter, downloadPath: str, targetDate=""):
        self._analyzer = analyzer
        self._downloadPath = downloadPath
        self._targetDate = targetDate
        self._util = Utilities()

    def generateDataset(self, dataset: pd.DataFrame, threads=4) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        total_memory = psutil.virtual_memory().total
        total_memory_gb = total_memory / (1024 ** 3)

        if threads == 1:
            os.environ['_JAVA_OPTIONS'] = f'-Xmx{int(total_memory_gb * 0.7)}g'
        else:
            os.environ['_JAVA_OPTIONS'] = f'-Xmx{int(total_memory_gb * 0.2)}g'

        classData = pd.DataFrame()
        methodData = pd.DataFrame()
        packageData = pd.DataFrame()
        repoQuantity = dataset.shape[0]
        step = 10

        for i in range(0, repoQuantity, step):
            set = dataset.iloc[i: i + step, :]
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
            futures = {executor.submit(self._downloadRepositoryData, value) for key, value in set.iterrows()}

            for future in concurrent.futures.as_completed(futures):
                classDataProj, methodDataProj, packageDataProj = future.result()
                if classDataProj.shape[0] > 0:
                    classData = pd.concat([classData, classDataProj], axis=0)
                    methodData = pd.concat([methodData, methodDataProj], axis=0)
                    packageData = pd.concat([packageData, packageDataProj], axis=0)

        if classData.shape[0] == 0:
            remainingProjects = dataset
        else:
            remainingProjects = dataset[~dataset['url'].isin(packageData['Repository'])]

        return classData, methodData, packageData, remainingProjects

    def _downloadRepositoryData(self, project: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        try:
            start = datetime.now()
            print(f'Downloading Repository {project["url"]}')

            self._cloneRepository(project, self._downloadPath)

            finish = datetime.now()
            print(f'Finish {project["url"]} download - Time: {finish - start}')

            if self._targetDate:
                self._checkoutRepoByDate(project["url"], f'{self._downloadPath}/{project["name"]}')
                print(f'Checkout {project["url"]}')

            return self._scanProject(project, self._downloadPath, project['name'])

        except Exception as error:
            self._util.deleteFolder(os.path.abspath(f"{self._downloadPath}/{project['name']}"))
            print(f'Error downloading {project["url"]} - {error}')
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


    def _scanProject(self, project: pd.Series, path: str, folderName: str):
        try:
            start = datetime.now()
            print(f'Scanning Repository {project["url"]}')

            self._analyzer.analyze(project["name"], folderName)

            classMetricsPath = f'{self._analyzer.getResultsDir()}/{project["name"]}/java/'
            p = Path(classMetricsPath)
            sub = [x for x in p.iterdir() if x.is_dir()]
            classData = pd.read_csv(f'{sub[-1]}/{project["name"]}-Class.csv')
            methodData = pd.read_csv(f'{sub[-1]}/{project["name"]}-Method.csv')
            packageData = pd.read_csv(f'{sub[-1]}/{project["name"]}-Package.csv')

            self._util.deleteFolder(f'{self._analyzer.getResultsDir()}/{project["name"]}')

            if (classData.shape[0] == 0):
                raise Exception("Empty Analysis Files")

            finish = datetime.now()
            print(f'Finish {project["url"]} Scan - Time: {finish - start}')

            self._util.deleteFolder(os.path.abspath(f'{path}/{folderName}'))

            classData['Repository'] = project['url']
            methodData['Repository'] = project['url']
            packageData['Repository'] = project['url']
            return classData, methodData, packageData

        except Exception as error:
            self._util.deleteFolder(os.path.abspath(f'{path}/{folderName}'))
            self._logError(project['url'], error)
            print(f'Error scanning {project["url"]} - {error}')
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


    def _cloneRepository(self, project: pd.Series, clonePath: str) -> None:
        filePath = f'{clonePath}/{project["name"]}'
        url = f'https://github.com/{project["owner"]}/{project["name"]}.git'

        try:
            Repo.clone_from(url, os.path.abspath(filePath))

        except Exception as error:
            self._logError(project['url'], error)
            raise Exception('Error cloning repository')


    def _checkoutRepoByDate(self, project: pd.Series, repoPath: str) -> None:
        repo = Repo(repoPath)
        branch = repo.head.ref.name
        commits = list(repo.iter_commits(branch, until=self._targetDate))

        if commits:
            commit = commits[0]
            try:
                repo.git.checkout(commit.hexsha)
            except Exception as error:
                self._logError(project['url'], error)
                raise Exception('Error in repository checkout')

    def generateQualitasMetrics(self, projectsPath: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        classData = pd.DataFrame()
        methodData = pd.DataFrame()
        packageData = pd.DataFrame()

        dataset = pd.DataFrame({'name': os.listdir(projectsPath)})

        try:
            for project in os.listdir(projectsPath):
                version = os.listdir(f'{projectsPath}/{project}')[-1]
                fullPath = os.path.abspath(f'{projectsPath}/{project}/{version}/compressed')
                for file in os.listdir(fullPath):
                    if file.endswith(('.tar.gz', '.tar.bz2', '.tgz')):
                        tar = tarfile.open(f'{fullPath}/{file}')
                        tar.extractall(path=fullPath)
                        tar.close()

                    elif file.endswith(('.jar', '.zip')):
                        jar = zipfile.ZipFile(f'{fullPath}/{file}')
                        jar.extractall(path=fullPath)
                        jar.close()
                print(f"Extracted {project}")

                project = pd.Series({'name': project, 'url': project})
                classDataProj, methodDataProj, packageDataProj = self._scanProject(project, fullPath, '')

                if classDataProj.shape[0] > 0:
                    classData = pd.concat([classData, classDataProj], axis=0)
                    methodData = pd.concat([methodData, methodDataProj], axis=0)
                    packageData = pd.concat([packageData, packageDataProj], axis=0)

            remainingProjects = dataset[~dataset['name'].isin(classData['Repository'])]

            return classData, methodData, packageData, remainingProjects

        except Exception as error:
            print(error)

    def _logError(self, project: str, message):
        logFilePath = os.path.abspath(f'{self._analyzer.getResultsDir()}/error-log.txt')
        if not os.path.exists(logFilePath):
            open(logFilePath, "x")

        with open(logFilePath, "a") as file:
            file.write(f"{datetime.now()} - {project} - {message}\n")