import pandas as pd
from Filters.GraphqlFilter import GraphqlFilter
from datetime import datetime
from dateutil.relativedelta import relativedelta

class CommitFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        commits = json['defaultBranchRef']['target']['history']['totalCount']
        if commits < self.filter['commits']:
            return True

        newJson = {"commits": commits,
                   "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
        json.pop('defaultBranchRef', None)
        json.update(newJson)
        return False

    def xiaUpdateFrame(self, repository: dict, path: str, date: datetime) -> bool:
        fmt = '%Y-%m-%d %H:%M:%S'
        commData = pd.read_csv(f'{path}_commits_and_comments.csv')
        commData['committed_at'] = pd.to_datetime(commData['committed_at'], format=fmt)
        rowsBeforeDate = commData[commData['committed_at'] < date]
        lastDate = rowsBeforeDate['committed_at'].max()
        totalCommits = rowsBeforeDate.shape[0]

        aMonthAgo = date - relativedelta(months=1)
        monthlyCommits = rowsBeforeDate[rowsBeforeDate['committed_at'] >= aMonthAgo]

        if totalCommits < self.filter['commits']:
            return True
        else:
            repository["commits"] = totalCommits
            repository["dateLastCommit"] = lastDate
            repository["monthlyCommits"] = monthlyCommits.shape[0]
            return False