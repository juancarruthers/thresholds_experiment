class GraphqlFilter:
    def __init__(self, p_filter: dict):
        self.filter = p_filter

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        pass