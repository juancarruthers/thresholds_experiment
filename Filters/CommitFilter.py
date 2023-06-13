from Filters.GraphqlFilter import GraphqlFilter


class CommitFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        commits = json['defaultBranchRef']['target']['history']['totalCount']
        if filtersFlag or commits < self.filter['commits']:
            return True

        newJson = {"commits": commits,
                   "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
        json.pop('defaultBranchRef', None)
        json.update(newJson)
        return False