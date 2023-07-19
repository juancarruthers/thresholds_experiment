import os
import pandas as pd
from Filters.GraphqlFilter import GraphqlFilter
from datetime import datetime
from dateutil.relativedelta import relativedelta

class AgeFilter(GraphqlFilter):

    def xiaUpdateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        if self.filter['age']['active']:
            oneYearAgo = date - relativedelta(years=self.filter['age']['years'])
        else:
            oneYearAgo = date
        folder = os.path.dirname(os.path.dirname(path))
        dataset = pd.read_csv(f'{folder}/allProjects.csv')
        dataset['createdAt'] = pd.to_datetime(dataset['createdAt'], format='%Y-%m-%dT%H:%M:%SZ')
        url = repository['url'][0]
        project = dataset[dataset['url'] == url]
        projectDate = project.iat[0,2]

        if projectDate > oneYearAgo:
            return True
        else:
            repository['id'] = project.iat[0,0]
            repository['createdAt'] = projectDate
            return False