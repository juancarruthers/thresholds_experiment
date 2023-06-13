from Filters.GraphqlFilter import GraphqlFilter
from Utilities import Utilities


class ContributorsFilter(GraphqlFilter):

    def __init__(self, p_filter: dict, p_itemsPageContrQuery=100):
        super().__init__(p_filter)
        util = Utilities()
        self._elementPerPageContribQuery = str(p_itemsPageContrQuery)
        self._tokens = util.readFile("token").split(",\n")


    def updateFrame(self, json: dict, owner: str, repositoryName: str, filtersFlag: bool) -> bool:
        if filtersFlag:
            return True
        util = Utilities()

        url = "https://api.github.com/repos/" + owner + "/" + repositoryName + "/contributors?per_page="+ self._elementPerPageContribQuery +"&page="
        i = 1
        response = util.makeRequest("", "GET", url + str(i))
        acum = 0
        contribAcum = 0

        if type(response) is list:

            while len(response) > 0:
                acum += len(response)
                i += 1
                for contributor in response:
                    contribAcum += contributor['contributions']
                response = util.makeRequest("", "GET", url + str(i))

        if acum < self.filter['contributors']:
            return True
        else:
            json.update({"contributors": acum, "contributions": contribAcum})
            return False