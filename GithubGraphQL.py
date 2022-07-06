import requests
import json
import pandas as pd

def main():
    repoCountQuery = {'query': readFile("repoCountQuery")}
    token = readFile("token")

    repoCount = makeRequests(repoCountQuery, token)['data']['search']['repositoryCount']

    variables = {'first': 1, 'cursor': None}
    df_data = []
    for i in range(repoCount):

        repoQuery = {'query': readFile("query"), 'variables': variables}
        jsonResponse = makeRequests(repoQuery, token)
        repoProperties = jsonResponse['data']['search']['edges']
        if len(repoProperties) != 0:
            cursor = repoProperties[0]['cursor']
            print(str(i) + ": Cursor:" + cursor + ", Owner: " + repoProperties[0]['node']['owner'][
                'login'] + ", RepoName: " + repoProperties[0]['node']['name'])
            df_data.append(replaceNestedValues(repoProperties[0]['node']))
            variables = {'first': 1, 'cursor': cursor}
        else:
            break

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
    newJson = {"primaryLanguage": json['languages']['edges'][0]['node']['name'], "totalSize": json['languages']['totalSize']}
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
    req = requests.post(url, json=query, headers=headers)
    return json.loads(req.text)


def readFile(fileName: str):
    file = open(fileName).readlines()
    query = ""

    for lines in file:
        query += lines

    return query

main()