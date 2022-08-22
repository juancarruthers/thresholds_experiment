import os.path
import random
import shutil
import requests
import pandas as pd
import datetime
import time
import concurrent.futures

def main():
    start = datetime.datetime.now()
    startSize, sizeInc, df_data = restoreCheckPoint()
    savethreshold = 1000
    queryVar = "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:"
    queryFile = readFile("APIQueries/repositoryMetadata")
    repoCountQueryFile = readFile("APIQueries/repositoryCount")
    tokens = readFile("token").split(",\n")
    repoCountQuery = {'query': repoCountQueryFile, 'variables': {'query': queryVar + ">=" + str(startSize)}}
    repoCount = makeRequest(repoCountQuery, tokens)['data']['search']['repositoryCount']

    if repoCount > 0:
        projectsSaved = 0

        while repoCount > 0:

            repoCountQuery = {'query': repoCountQueryFile, 'variables': {'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}}
            repoCountSubQuery = makeRequest(repoCountQuery, tokens)['data']['search']['repositoryCount']

            j = 1
            while (repoCountSubQuery >= 990) | (repoCountSubQuery == 0):
                if repoCountSubQuery >= 990:
                    sizeInc -= j
                else:
                    sizeInc += j
                repoCountQuery = {'query': repoCountQueryFile, 'variables': {'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}}
                repoCountSubQuery = makeRequest(repoCountQuery, tokens)['data']['search']['repositoryCount']
                j += j

            repoCount -= repoCountSubQuery
            cursor = None
            hasNextPage = True

            while hasNextPage:
                elementsPerPage = 10
                variables = {'first': elementsPerPage, 'cursor': cursor, 'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}
                repoQuery = {'query': queryFile, 'variables': variables}
                jsonResponse = makeRequest(repoQuery, tokens)
                repositories = jsonResponse['data']['search']

                #CONCURRENCY
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
                futures = {executor.submit(replaceNestedPropertiesValues, repoProperties, tokens) for repoProperties in repositories['edges']}
                for future in concurrent.futures.as_completed(futures):
                    repositoryProperties = future.result()
                    df_data.append(repositoryProperties)

                hasNextPage = repositories['pageInfo']['hasNextPage']
                if hasNextPage:
                    cursor = repositories['pageInfo']['endCursor']

            projectsSaved += repoCountSubQuery

            if projectsSaved > savethreshold:
                saveCheckPoint(startSize, sizeInc, df_data)
                projectsSaved = 0

            startSize += sizeInc
        finish = datetime.datetime.now()
        difference = finish - start
        print('Start:', start, '- Finish:', datetime.datetime.now(), " -  Time:", difference)
        df = pd.DataFrame(df_data)
        df.to_csv("./datasets/githubquery" + str(finish.month) + str(finish.year) + ".csv")
        shutil.rmtree('./.backup')

def replaceNestedPropertiesValues(repoProperties: dict, tokens: str):
    properties = repoProperties['node']
    owner = properties['owner']['login']
    repositoryName = properties['name']

    #NORMALIZE OUTPUT
    updateLanguage(properties)
    updateIssues(properties, tokens, owner, repositoryName)
    updatePullRequests(properties, tokens, owner, repositoryName)
    updateCommits(properties)
    updateContributors(properties, tokens, owner, repositoryName)

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

def updateIssues(json, tokens ,owner, repoName):
    if (json['issues']['totalCount'] > 0):
        closed = readFile("APIQueries/Issues/closedIssues")
        open = readFile("APIQueries/Issues/openIssues")
        states = {'closed': closed, 'open': open}
        newJson = {}
        for state, query in states.items():
            variables = {'owner': owner, 'repoName': repoName}
            lastPullReqQuery = {'query': query, 'variables': variables}
            jsonResponse = makeRequest(lastPullReqQuery, tokens)
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
def updatePullRequests(json, tokens, owner, repoName):
    if (json['pullRequests']['totalCount'] > 0):
        closed = readFile("APIQueries/PullReq/closedPullReq")
        merged = readFile("APIQueries/PullReq/mergedPullReq")
        open = readFile("APIQueries/PullReq/openPullReq")
        states = {'closed': closed, 'merged': merged, 'open': open}
        newJson = {}
        for state, query in states.items():
            variables = {'owner': owner, 'repoName': repoName}
            lastPullReqQuery = {'query': query, 'variables': variables}
            jsonResponse = makeRequest(lastPullReqQuery, tokens)
            pullReqCount = jsonResponse['data']['repository']['pullRequests']['totalCount']
            if pullReqCount > 0:
                newJson.update({state+"PullReqCount": pullReqCount, state+"PullReqLastDate": jsonResponse['data']['repository']['pullRequests']['nodes'][0]['createdAt']})
            else:
                newJson.update({state + "PullReqCount": pullReqCount, state + "PullReqLastDate": '-'})
    else:
        newJson = {"closedPullReqCount": 0, "closedPullReqLastDate": "-", "mergedPullReqCount": 0, "mergedPullReqLastDate": "-", "openPullReqCount": 0, "openPullReqLastDate": "-"}
    json.pop('pullRequests', None)
    json.update(newJson)

def updateContributors(json, tokens, owner, repoName):
    elementsPerPage = "100"
    url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page="+ elementsPerPage +"&page="
    i = 1
    response = makeRequest("", tokens, "GET", url + str(i))

    if not(type(response) is list):
        acum = 1000
    else:
        acum = 0

        while len(response) > 0:
            acum += len(response)
            i += 1
            response = makeRequest("", tokens, "GET", url + str(i))

    json.pop('owner', None)
    json.update({"contributors": acum})


def makeRequest (query: str | dict, tokens: list[str], reqType="POST", url='https://api.github.com/graphql') -> dict:
    headers = {'Authorization': 'Bearer ' + random.choice(tokens)}
    response: dict
    response, condition = _requestCondition(query, reqType, url, headers)

    while condition:
        time.sleep(random.choice([50, 100, 150, 200, 250, 300]))
        response, condition = _requestCondition(query, reqType, url, headers)

    return response


def _requestCondition (query: str | dict, reqType: str, url: str, headers: dict) -> tuple[dict, bool]:
    condition: bool
    if reqType == "POST":
        response = requests.post(url, json=query, headers=headers).json()
        condition = response.get("errors", False)
    else:
        response = requests.get(url, headers=headers).json()
        if not (type(response) is list):
            message: str = response['message']
            condition = message != 'The history or contributor list is too large to list contributors for this repository via the API.'
        else:
           condition = not (type(response) is list)

    return response, condition


def readFile(fileName: str):
    file = open(fileName).readlines()
    query = ""

    for lines in file:
        query += lines

    return query

def restoreCheckPoint() -> tuple[int, int, list]:
    startSize = 10000
    sizeInc = 2000
    path = './.backup'
    if os.path.isdir(path):
        backupData = pd.read_csv(path + '/largerFrame.csv', encoding='unicode_escape').to_dict('records')
        queryState = pd.read_csv(path + '/queryState.csv')
        return int(queryState.iloc[0, 0]), int(queryState.iloc[0, 1]), backupData
    else:
        return startSize , sizeInc, []

def saveCheckPoint(startSize, sizeInc, dataset):
    path = "./.backup"
    if not (os.path.isdir(path)):
        os.mkdir(path)

    df = pd.DataFrame(dataset)
    df.to_csv(path + '/largerFrame.csv', index=False)
    queryState = pd.DataFrame([{'startSize': startSize, 'sizeInc': sizeInc}])
    queryState.to_csv(path + "/queryState.csv", index=False)

if __name__ == '__main__':
    main()