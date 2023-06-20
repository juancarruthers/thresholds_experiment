from Filters.GraphqlFilter import GraphqlFilter
class SizeFilter(GraphqlFilter):

    def updateFrame(self, json: dict, owner: str, repositoryName: str) -> bool:

        totalSize: int = json['languages']['totalSize']

        if totalSize < self.filter['totalSize']:
            return True

        if totalSize > 0:
            newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'],
                       "totalSize": json['languages']['totalSize']}
        else:
            newJson = {"primaryLanguage": "-", "totalSize": 0}

        json.pop('languages', None)
        json.update(newJson)
        return False