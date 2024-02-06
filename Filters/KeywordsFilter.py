from Filters.GraphqlFilter import GraphqlFilter


class KeywordsFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:
        name = repositoryName.lower()
        if any(keyword in name for keyword in self.filter['keywords']):
            return True

        return False