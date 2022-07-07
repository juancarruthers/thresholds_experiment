import requests
import json
import pandas as pd
import datetime

def main():
    queryVar = {'query': "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:>=10000"}
    repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': queryVar}
    token = readFile("token")
    repoCount = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

    if repoCount > 0:
        df_data = []
        i = 1
        startSize = 10000
        sizeInc = 1000

        while repoCount > 0:

            queryVar = {'query': "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:" + str(startSize) + ".." + str(startSize + sizeInc)}
            repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': queryVar}
            repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

            while repoCountSubQuery > 1000:
                sizeInc -= 100
                queryVar = {'query': "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:" + str(startSize) + ".." + str(startSize + sizeInc)}
                repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': queryVar}
                repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

            while repoCountSubQuery == 0:
                sizeInc += 100
                queryVar = {'query': "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:" + str(startSize) + ".." + str(startSize + sizeInc)}
                repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': queryVar}
                repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

            repoCount -= repoCountSubQuery
            cursor = None
            hasNextPage = True

            while hasNextPage:
                variables = {'first': 30, 'cursor': cursor, 'query': "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:"+ str(startSize) + ".." + str(startSize + sizeInc)}
                repoQuery = {'query': readFile("query"), 'variables': variables}
                jsonResponse = makeRequests(repoQuery, token)
                repositories = jsonResponse['data']['search']
                for repoProperties in repositories['edges']:
                    print(str(datetime.datetime.now()) + " - N: " + str(i) +", Cursor:" + repoProperties['cursor'] + ", Owner: " + repoProperties['node']['owner'][
                        'login'] + ", RepoName: " + repoProperties['node']['name'])
                    df_data.append(replaceNestedValues(repoProperties['node']))
                    i += 1

                hasNextPage = repositories['pageInfo']['hasNextPage']
                if hasNextPage:
                    cursor = repositories['pageInfo']['endCursor']

            startSize += sizeInc

        df = pd.DataFrame(df_data)
        df.to_csv("dataset2.csv")


def replaceNestedValues(json):
    json = getLanguage(json)
    json = getIssues(json)
    json = getPullRequests(json, json['owner']['login'], json['name'])
    json = getCommits(json)
    getContributors(json, json['owner']['login'], json['name'])
    return json


def getLanguage(json):
    if json['languages']['totalSize'] > 0:
        newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'], "totalSize": json['languages']['totalSize']}
    else:
        newJson = {"primaryLanguage": "-", "totalSize": 0}
    json.pop('languages', None)
    json.update(newJson)
    return json

def getCommits(json):
    newJson = {"commits": json['defaultBranchRef']['target']['history']['totalCount'], "dateLastCommit": json['defaultBranchRef']['target']['history']['nodes'][0]['committedDate']}
    json.pop('defaultBranchRef', None)
    json.update(newJson)
    return json

def getIssues(json):
    json["issues"] = json['issues']['totalCount']
    return json

'''
1)OBTENER CANTIDAD PULL REQUESTS CERRADAS DEL REPO NO MERGEADAS DE OTRO REPO
2)FECHA ULTIMO PULL REQUEST
'''
def getPullRequests(json, owner, repoName):
    if (json['pullRequests']['totalCount']>0):
        variables = {'owner': owner, 'repoName': repoName}
        lastPullReqQuery = {'query': readFile("lastPullReqQuery"), 'variables': variables}
        jsonResponse = makeRequests(lastPullReqQuery, readFile("token"))
        newJson = {"pullReqCount": json['pullRequests']['totalCount'], "dateLastPullReq": jsonResponse['data']['repository']['pullRequests']['nodes'][0]['createdAt']}
    else:
        newJson = {"pullReqCount": 0, "dateLastPullReq": "-"}
    json.pop('pullRequests', None)
    json.update(newJson)
    return json

def getContributors(response, owner, repoName):
    url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page=100&page="
    acum = 0
    i = 1
    while len(json.loads(requests.get(url + str(i)).text)) == 100:
        acum += 100
        i += 1
    acum += len(json.loads(requests.get(url + str(i)).text))
    response.pop('owner', None)
    return response.update({"contributors": acum})


def makeRequests (query: str, token: str):
    url = 'https://api.github.com/graphql'
    headers = {'Authorization': 'Bearer ' + token}
    jsonReq = json.loads(requests.post(url, json=query, headers=headers).text)
    if not (jsonReq.get("error") is None):
        jsonReq = json.loads(requests.post(url, json=query, headers=headers).text)

    return jsonReq


def readFile(fileName: str):
    file = open(fileName).readlines()
    query = ""

    for lines in file:
        query += lines

    return query

main()