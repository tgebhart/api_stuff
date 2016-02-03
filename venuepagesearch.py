import json
import boto3
import requests
import re
from general.tableiterator import TableIterator
from facebookapi.facebookapi import FacebookApi

TABLENAME = "FoursquareVenues"
ATTRIBUTES = "address_key, #L"
secretsLocation = '/Users/tgebhart/Documents/Work/aivibe/code/api_stuff/keys/accesskeys.json'



def iterateFirstResponse():
    tableiter = TableIterator(TABLENAME)
    response = tableiter.batchGetItemWithNameFirst(ATTRIBUTES)
    return response

def iterateResponses():
    facebookapi = FacebookApi()
    facebookapi.generateAccessToken()
    totalIdCount = 0
    tableiter = TableIterator(TABLENAME)
    firstResponse = iterateFirstResponse()
    last_response = ""
    try:
        last_response = firstResponse['LastEvaluatedKey']
    except KeyError:
        pass


    for response in firstResponse['Items']:
        facebookNames = facebookapi.facebookSearchName(response['name'])
        for name in facebookNames['data']:
            print(name)
            facebookAddress = facebookapi.facebookGetAddress(name['id'])
            try:
                print(facebookAddress['location']['street'], " || ", response['location']['display_address'][0])
                if addressEquals(facebookAddress['location']['street'], response['location']['display_address'][0]):
                    totalIdCount += 1
                    print("==================match on first response: ", totalIdCount)
                    tableiter.updateItemSet(response['address_key'], 'facebook_id', name['id'], 'S')
                    break
            except KeyError:
                try:
                    parentId = facebookAddress['location']['located_in']
                    facebookAddress = facebookapi.facebookGetAddress(parentId)
                    if facebookAddress['location']['street'] == response['location']['display_address'][0]:
                        totalIdCount += 1
                        print("=================match on first parent: ", totalIdCount)
                        tableiter.updateItemSet(response['address_key'], 'facebook_id', name['id'], 'S')
                        break
                except Exception:
                    pass

    key_err = False
    if last_response != "":
        key_err = True

    while key_err == False:
        for response in nextResponse['Items']:
            facebookNames = facebookapi.facebookSearchName(response['name'])
            for name in facebookNames['data']:
                facebookAddress = facebookapi.facebookGetAddress(name['id'])
                try:
                    print(facebookAddress['location']['street'], " || ", response['location']['display_address'][0])
                    if addressEquals(facebookAddress['location']['street'], response['location']['display_address'][0]):
                        totalIdCount += 1
                        print("=================match on next response: ", totalIdCount)
                        tableiter.updateItemSet(response['address_key'], 'facebook_id', name['id'], 'S')
                        break
                except KeyError:
                    try:
                        parentId = facebookAddress['location']['located_in']
                        facebookAddress = facebookapi.facebookGetAddress(parentId)
                        print("second try: ", facebookAddress)
                        if facebookAddress['location']['street'] == response['location']['display_address'][0]:
                            totalIdCount += 1
                            print("=================match on next parent: ", totalIdCount)
                            tableiter.updateItemSet(response['address_key'], 'facebook_id', name['id'], 'S')
                            break
                    except Exception:
                        pass

        tableiter.narcolepsy()
        try:
            last_response = firstResponse['LastEvaluatedKey']
            nextResponse = tableiter.batchGetItemWithName(ATTRIBUTES, last_response)
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

iterateResponses()
