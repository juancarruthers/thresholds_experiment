import requests
import json
import pandas as pd
import datetime
import time

def main():
    startSize = 10000
    queryVar = "is:public, language:java, archived:false, mirror:false, forks:>=10, created:<=2021-07-06, size:"
    repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': {'query': queryVar + ">=" + str(startSize)}}
    token = readFile("token")
    repoCount = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

    if repoCount > 0:
        df_data = []
        i = 1
        sizeInc = 2000

        while repoCount > 0:

            repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': {'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}}
            repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

            j = 1
            while (repoCountSubQuery >= 990) | (repoCountSubQuery == 0):
                if repoCountSubQuery >= 990:
                    sizeInc -= j
                else:
                    sizeInc += j
                repoCountQuery = {'query': readFile("repoCountQuery"), 'variables': {'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}}
                repoCountSubQuery = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']
                j += j

            repoCount -= repoCountSubQuery
            cursor = None
            hasNextPage = True

            while hasNextPage:
                variables = {'first': 30, 'cursor': cursor, 'query': queryVar + str(startSize) + ".." + str(startSize + sizeInc)}
                repoQuery = {'query': readFile("query"), 'variables': variables}
                jsonResponse = makeRequests(repoQuery, token)
                repositories = jsonResponse['data']['search']
                for repoProperties in repositories['edges']:
                    print(str(datetime.datetime.now()) + " - N: " + str(i) +", Cursor:" + repoProperties['cursor'] + ", Owner: " + repoProperties['node']['owner'][
                        'login'] + ", RepoName: " + repoProperties['node']['name'])
                    df_data.append(replaceNestedValues(repoProperties['node'], token))
                    i += 1

                hasNextPage = repositories['pageInfo']['hasNextPage']
                if hasNextPage:
                    cursor = repositories['pageInfo']['endCursor']

            startSize += sizeInc

        df = pd.DataFrame(df_data)
        df.to_csv("largerFrame.csv")


def replaceNestedValues(json, token):
    json = getLanguage(json)
    json = getIssues(json)
    json = getPullRequests(json, json['owner']['login'], json['name'])
    json = getCommits(json)
    getContributors(json, token, json['owner']['login'], json['name'])
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

def getContributors(response, token, owner, repoName):
    url = "https://api.github.com/repos/" + owner + "/" + repoName + "/contributors?per_page=100&page="
    headers = {'Authorization': 'Bearer ' + token}
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

    response.pop('owner', None)
    return response.update({"contributors": acum})


def makeRequests (query: str, token: str):
    url = 'https://api.github.com/graphql'
    headers = {'Authorization': 'Bearer ' + token}
    jsonReq = json.loads(requests.post(url, json=query, headers=headers).text)
    while not (jsonReq.get("error") is None):
        time.sleep(300)
        jsonReq = json.loads(requests.post(url, json=query, headers=headers).text)

    return jsonReq


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

