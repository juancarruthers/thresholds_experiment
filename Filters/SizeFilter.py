from Filters.GraphqlFilter import GraphqlFilter
class SizeFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:

        totalSize: int = json['languages']['totalSize']

        if filtersFlag or totalSize < self.filter['totalSize']:
            return True

        if totalSize > 0:
            newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'],
                       "totalSize": json['languages']['totalSize']}
        else:
            newJson = {"primaryLanguage": "-", "totalSize": 0}

        json.pop('languages', None)
        json.update(newJson)
        return False