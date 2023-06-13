from Filters.GraphqlFilter import GraphqlFilter


class RecentActivityFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        if filtersFlag:
            return True

        dateLastCommitCond = json['dateLastCommit'] < self.filter['dateLastActivity']
        dateLastClosedPullReq = json['closedPullReqLastDate'] < self.filter['dateLastActivity']
        dateLastMergedPullReq = json['mergedPullReqLastDate'] < self.filter['dateLastActivity']

        return dateLastCommitCond and dateLastClosedPullReq and dateLastMergedPullReq