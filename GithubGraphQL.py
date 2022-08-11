import requests
import pandas as pd
import datetime
import time
import concurrent.futures

def main():
    start = datetime.datetime.now()
    startSize = 10000
    queryVar = "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:"
    queryFile = readFile("APIQueries/repositoryMetadata")
    repoCountQueryFile = readFile("APIQueries/repositoryCount")
    token = readFile("token")
    repoCountQuery = {'query': repoCountQueryFile, 'variables': {'query': queryVar + ">=" + str(startSize)}}
    repoCount = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

    if repoCount > 0:
        df_data = []
        sizeInc = 2000

        while repoCount > 0:

            repoCountQuery = {'query': repoCountQueryFile, 'variables': {'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}}
            repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

            j = 1
            while (repoCountSubQuery >= 990) | (repoCountSubQuery == 0):
                if repoCountSubQuery >= 990:
                    sizeInc -= j
                else:
                    sizeInc += j
                repoCountQuery = {'query': repoCountQueryFile, 'variables': {'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}}
                repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']
                j += j

            repoCount -= repoCountSubQuery
            cursor = None
            hasNextPage = True

            while hasNextPage:
                elementsPerPage = 30
                variables = {'first': elementsPerPage, 'cursor': cursor, 'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}
                repoQuery = {'query': queryFile, 'variables': variables}
                jsonResponse = makeRequests(repoQuery, token)
                repositories = jsonResponse['data']['search']

                #CONCURRENCY
                executor = concurrent.futures.ProcessPoolExecutor(max_workers=4)
                futures = {executor.submit(replaceNestedPropertiesValues, repoProperties, token) for repoProperties in repositories['edges']}
                for future in concurrent.futures.as_completed(futures):
                    repositoryProperties = future.result()
                    df_data.append(repositoryProperties)

                hasNextPage = repositories['pageInfo']['hasNextPage']
                if hasNextPage:
                    cursor = repositories['pageInfo']['endCursor']

            startSize += sizeInc
        finish = datetime.datetime.now()
        difference = finish - start
        print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)
        df = pd.DataFrame(df_data)
        df.to_csv("largerFrame.csv")

def replaceNestedPropertiesValues(repoProperties: dict, token: str):
    properties = repoProperties['node']
    owner = properties['owner']['login']
    repositoryName = properties['name']

    #NORMALIZE OUTPUT
    updateLanguage(properties)
    updateIssues(properties, token, owner, repositoryName)
    updatePullRequests(properties, token, owner, repositoryName)
    updateCommits(properties)
    updateContributors(properties, token, owner, repositoryName)

    print(datetime.datetime.now(), "- Owner:", owner, "- Repository:", repositoryName)

    return properties

def updateLanguage(json):
    if json['languages']['totalSize'] > 0:
        newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'], "totalSize": json['languages']['totalSize']}
    else:
        newJson = {"primaryLanguage": "-", "totalSize": 0}
    json.pop('languages', None)
    json.update(newJson)

def updateCommits(json):
    newJson = {"commits": json['defaultBranchRef']['target']['history']['totalCount'], "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
    json.pop('defaultBranchRef', None)
    json.update(newJson)

def updateIssues(json, token ,owner, repoName):
    if (json['issues']['totalCount'] > 0):
        closed = readFile("APIQueries/Issues/closedIssues")
        open = readFile("APIQueries/Issues/openIssues")
        states = {'closed': closed, 'open': open}
        newJson = {}
        for state, query in states.items():
            variables = {'owner': owner, 'repoName': repoName}
            lastPullReqQuery = {'query': query, 'variables': variables}
            jsonResponse = makeRequests(lastPullReqQuery, token)
            pullReqCount = jsonResponse['data']['repository']['issues']['totalCount']
            if pullReqCount > 0:
                newJson.update({state+"IssuesCount": pullReqCount, state+"IssueLastDate": jsonResponse['data']['repository']['issues']['nodes'][0]['createdAt']})
            else:
                newJson.update({state + "IssuesCount": pullReqCount, state + "IssueLastDate": '-'})
    else:
        newJson = {"closedIssuesCount": 0, "closedIssueLastDate": "-", "openIssuesCount": 0, "openIssueLastDate": "-"}
    json.pop('issues', None)
    json.update(newJson)

'''
1)OBTENER CANTIDAD PULL REQUESTS CERRADAS DEL REPO NO MERGEADAS DE OTRO REPO
2)FECHA ULTIMO PULL REQUEST
'''
def updatePullRequests(json, token, owner, repoName):
    if (json['pullRequests']['totalCount'] > 0):
        closed = readFile("APIQueries/PullReq/closedPullReq")
        merged = readFile("APIQueries/PullReq/mergedPullReq")
        open = readFile("APIQueries/PullReq/openPullReq")
        states = {'closed': closed, 'merged': merged, 'open': open}
        newJson = {}
        for state, query in states.items():
            variables = {'owner': owner, 'repoName': repoName}
            lastPullReqQuery = {'query': query, 'variables': variables}
            jsonResponse = makeRequests(lastPullReqQuery, token)
            pullReqCount = jsonResponse['data']['repository']['pullRequests']['totalCount']
            if pullReqCount > 0:
                newJson.update({state+"PullReqCount": pullReqCount, state+"PullReqLastDate": jsonResponse['data']['repository']['pullRequests']['nodes'][0]['createdAt']})
            else:
                newJson.update({state + "PullReqCount": pullReqCount, state + "PullReqLastDate": '-'})
    else:
        newJson = {"closedPullReqCount": 0, "closedPullReqLastDate": "-", "mergedPullReqCount": 0, "mergedPullReqLastDate": "-", "openPullReqCount": 0, "openPullReqLastDate": "-"}
    json.pop('pullRequests', None)
    json.update(newJson)

def updateContributors(json, token, owner, repoName):
    elementsPerPage = "100"
    url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page="+ elementsPerPage +"&page="
    i = 1
    response = makeRequests("", token, "GET", url + str(i))
    acum = 0

    while len(response) > 0:
        acum += len(response)
        i += 1
        response = makeRequests("", token, "GET", url + str(i))

    json.pop('owner', None)
    json.update({"contributors": acum})


def makeRequests (query: str | dict, token: str, reqType="POST", url='https://api.github.com/graphql') -> dict:
    headers = {'Authorization': 'Bearer ' + token}
    response: dict
    condition: bool
    if reqType == "POST":
        response = requests.post(url, json=query, headers=headers).json()
        condition = response.get("errors", False)
    else:
        response = requests.get(url, headers=headers).json()
        condition = not(type(response) is list)

    while condition:
        time.sleep(300)
        if reqType == "POST":
            response = requests.post(url, json=query, headers=headers).json()
            condition = response.get("errors", False)
        else:
            response = requests.get(url, headers=headers).json()
            condition = not(type(response) is list)

    return response


def readFile(fileName: str):
    file = open(fileName).readlines()
    query = ""

    for lines in file:
        query += lines

    return query

''' Dataset con contributors


def getContributorsTest():
    df_data = []
    frame = pd.read_csv('./datasets/Larger/owner.csv')
    j = 1
    for index, row in frame.iterrows():
        url = "https://api.github.com/repos/" + row['url'] + "/contributors?per_page=100&page="
        headers = {'Authorization': 'Bearer ' + "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"}
        acum = 0
        i = 1
        req = json.loads(requests.get(url + str(i), headers=headers).text)

        while len(req) > 0:
            if type(req) is list:
                acum += len(req)
                i += 1
            else:
                time.sleep(300)
            req = json.loads(requests.get(url + str(i), headers=headers).text)

        print (str(datetime.datetime.now()) + ': ' + str(j) + ' - ' + row['url'] + ' - ' + str(acum))
        j +=1
        df_data.append({"contributors": acum})
    df = pd.DataFrame(df_data)
    df.to_csv("largerFrame.csv")
'''

if __name__ == '__main__':
    main()