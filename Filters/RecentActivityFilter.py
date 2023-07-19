from Filters.GraphqlFilter import GraphqlFilter
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

class RecentActivityFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:

        dateLastCommitCond = json['dateLastCommit'] < self.filter['dateLastActivity']
        dateLastClosedPullReq = json['closedPullReqLastDate'] < self.filter['dateLastActivity']
        dateLastMergedPullReq = json['mergedPullReqLastDate'] < self.filter['dateLastActivity']

        return dateLastCommitCond and dateLastClosedPullReq and dateLastMergedPullReq

    def xiaUpdateFrame(self, repository: dict, path: str, date: datetime):
        if self.filter['activity']['active']:
            oneMonthAgo = date - relativedelta(months=self.filter['activity']['months'])
        else:
            oneMonthAgo = date

        fmt = '%Y-%m-%d %H:%M:%S'

        commData = pd.read_csv(f'{path}_commits_and_comments.csv')
        commData['committed_at'] = pd.to_datetime(commData['committed_at'], format=fmt)
        rows = commData[(commData['committed_at'] >= oneMonthAgo) & (commData['committed_at'] < date)]
        totalCommits = rows.shape[0]

        prData = pd.read_csv(f'{path}_pr.csv')
        date1 = pd.to_datetime(prData['closed_at'], errors='coerce', format=fmt)
        date2 = pd.to_datetime(prData['closed_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
        prData['closed_at'] = date1.fillna(date2)
        rows = prData[(prData['state'] =='closed') & (prData['closed_at'] >= oneMonthAgo) & (prData['closed_at'] < date)]
        totalPullReq = rows.shape[0]

        recentActivities = totalCommits + totalPullReq

        if recentActivities == 0:
            return True
        else:
            return False