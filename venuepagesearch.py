import json
import boto3
import requests
import re
from general.tableiterator import TableIterator


TABLENAME = "YelpVenues"
ATTRIBUTES = "address_key, #L"
LOCATIONCENTER = '37.78, -122.417'
DISTANCE = '500'
secretsLocation = '/Users/tgebhart/Documents/Work/aivibe/code/api_stuff/keys/accesskeys.json'
app_id = ""
app_secret = ""
authHash = ""


def findSecrets():
    with open(secretsLocation) as data_file:
        jason = json.load(data_file)
    for s in jason:
        if s == "facebook":
            app_id = jason[s][0]['app_id']
            app_secret = jason[s][1]['app_secret']
    authHash = app_id + "|" + app_secret
    return authHash


def iterateFirstResponse():
    tableiter = TableIterator(TABLENAME)
    response = tableiter.batchGetItemWithNameFirst(ATTRIBUTES)
    return response

def iterateResponses():
    totalIdCount = 0
    tableiter = TableIterator(TABLENAME)
    firstResponse = iterateFirstResponse()
    lastResponse = firstResponse['LastEvaluatedKey']

    for response in firstResponse['Items']:
        facebookNames = facebookSearchName(response['name'])
        for name in facebookNames['data']:
            facebookAddress = facebookGetAddress(name['id'])
            try:
                print(facebookAddress['location']['street'], " || ", response['location']['display_address'][0])
                if addressEquals(facebookAddress['location']['street'], response['location']['display_address'][0]):
                    totalIdCount += 1
                    print("==================match on first response: ", totalIdCount)
                    tableiter.updateItem(response, 'facebook_id', name['id'], 'S')
                    break
            except KeyError:
                try:
                    parentId = facebookAddress['location']['located_in']
                    facebookAddress = facebookGetAddress(parentId)
                    if facebookAddress['location']['street'] == response['location']['display_address'][0]:
                        totalIdCount += 1
                        print("=================match on first parent: ", totalIdCount)
                        tableiter.updateItem(response, 'facebook_id', name['id'], 'S')
                        break
                except Exception:
                    pass


    nextResponse = tableiter.batchGetItemWithName(ATTRIBUTES, lastResponse)
    keyErr = False
    while keyErr == False:
        for response in nextResponse['Items']:
            facebookNames = facebookSearchName(response['name'])
            for name in facebookNames['data']:
                facebookAddress = facebookGetAddress(name['id'])
                try:
                    print(facebookAddress['location']['street'], " || ", response['location']['display_address'][0])
                    if addressEquals(facebookAddress['location']['street'], response['location']['display_address'][0]):
                        totalIdCount += 1
                        print("=================match on next response: ", totalIdCount)
                        tableiter.updateItem(response, 'facebook_id', name['id'], 'S')
                        break
                except KeyError:
                    try:
                        parentId = facebookAddress['location']['located_in']
                        facebookAddress = facebookGetAddress(parentId)
                        print("second try: ", facebookAddress)
                        if facebookAddress['location']['street'] == response['location']['display_address'][0]:
                            totalIdCount += 1
                            print("=================match on next parent: ", totalIdCount)
                            tableiter.updateItem(response, 'facebook_id', name['id'], 'S')
                            break
                    except Exception:
                        pass

        tableiter.narcolepsy()
        try:
            lastResponse = firstResponse['LastEvaluatedKey']
            nextResponse = tableiter.batchGetItemWithName(ATTRIBUTES, lastResponse)
        except KeyError:
            keyErr = True

def addressEquals(facebook, aws):
    facebookAddress = re.sub('[!@#$.]', '', facebook)
    response = re.sub('[!@#$.]', '', aws)
    if standardizeAddress(facebookAddress) == standardizeAddress(response):
        return True
    return False

def standardizeAddress(s):
    if 'Street' in s:
        s.replace('Street', 'St')
    if 'Avenue' in s:
        s.replace('Avenue', 'Ave')
    if 'Road' in s:
        s.replace('Road', 'Rd')
    if 'Parkway' in s:
        s.replace('Parkway', 'Pkwy')

    return s


def facebookSearchName(name):
    payload = {'q' : name, 'type' : 'page', 'center' : LOCATIONCENTER, 'distance' : DISTANCE, 'topic_filter' : 'all', 'access_token' : authHash}
    return json.loads(requests.get("https://graph.facebook.com/search", params=payload).text)

def facebookGetAddress(id):
    payload = {'id' : id, 'fields' : {'name', 'location'}, 'access_token' : authHash}
    return json.loads(requests.get("https://graph.facebook.com/v2.5/", params=payload).text)


authHash = findSecrets()
iterateResponses()
