from Filters.GraphqlFilter import GraphqlFilter
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from multipledispatch import dispatch


class RecentActivityFilter(GraphqlFilter):

    @dispatch(dict, str, str)
    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:

        dateLastCommitCond = json['dateLastCommit'] < self.filter['dateLastActivity']
        dateLastClosedPullReq = json['closedPullReqLastDate'] < self.filter['dateLastActivity']
        dateLastMergedPullReq = json['mergedPullReqLastDate'] < self.filter['dateLastActivity']

        return dateLastCommitCond and dateLastClosedPullReq and dateLastMergedPullReq

    @dispatch(dict, str, datetime)
    def updateFrame(self, repository: dict, path: str, date: datetime):

        oneMonthAgo = date - relativedelta(months=1)
        commData = pd.read_csv(f'{path}_commits_and_comments.csv')
        commData['committed_at'] = pd.to_datetime(commData['committed_at'])
        rows = commData[(commData['committed_at'] >= oneMonthAgo) & (commData['committed_at'] <= date)]
        totalCommits = rows.shape[0]

        prData = pd.read_csv(f'{path}_pr.csv')
        prData['closed_at'] = pd.to_datetime(prData['closed_at'])
        rows = prData[(prData['state']=='closed') & (prData['closed_at']>=oneMonthAgo) & (prData['closed_at']<=date)]
        totalPullReq = rows.shape[0]

        recentActivities = totalCommits + totalPullReq

        if recentActivities == 0:
            return True
        else:
            return False