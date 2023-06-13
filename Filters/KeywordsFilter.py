from Filters.GraphqlFilter import GraphqlFilter


class KeywordsFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        if filtersFlag or any(keyword in repositoryName for keyword in self.filter['keywords']):
            return True

        return False