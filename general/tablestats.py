import json
import boto3
import requests
import re
from decimal import Decimal
from tableiterator import TableIterator

STAT_TABLE_NAME = "YelpVenuesFiltered"
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
stat_table = dynamodb.Table(STAT_TABLE_NAME)

def main():
    getVenueInfo()


def getTotalAttendeeEvents():
    attributes = "raw_events_attended"
    number_resp = 0
    next_key = ""
    attendee_iterator = TableIterator("FacebookAttendees", "facebook_user_id")
    resp = attendee_iterator.batchGetItemAttributes(attributes)
    number_resp += len(resp['Items'])
    lookAtAttendeeResponses(resp)
    try:
        next_key = resp['LastEvaluatedKey']
    except KeyError:
        pass

    while next_key != "":
        resp = attendee_iterator.batchGetItemAttributes(attributes, startkey=next_key)
        number_resp += len(resp['Items'])
        lookAtAttendeeResponses(resp)
        try:
            next_key = resp['LastEvaluatedKey']
        except KeyError:
            next_key = ""
            print("done!", number_resp)
            pass


def lookAtAttendeeResponses(resp):
    maximum = 0
    big5count = 0
    for response in resp['Items']:
        if len(response['raw_events_attended']) > maximum:
            maximum = len(response['raw_events_attended'])
        if len(response['raw_events_attended']) > 2:
            big5count += 1
    print(maximum)
    print(big5count)



def getAttendingEventInfo():
    attributes = 'raw_attending_count'
    event_list = []
    number_resp = 0
    next_key = ""
    event_iterator = TableIterator("FacebookEvents", "facebook_event_id")
    resp = event_iterator.batchGetItemAttributes(attributes)
    number_resp += len(resp['Items'])
    event_list.append(resp['Items'])

    try:
        next_key = resp['LastEvaluatedKey']
    except KeyError:
        pass

    while next_key != "":
        resp = event_iterator.batchGetItemAttributes(attributes, startkey=next_key)
        number_resp += len(resp['Items'])
        event_list.append(resp['Items'])
        try:
            next_key = resp['LastEvaluatedKey']
        except KeyError:
            next_key = ""
            print("done!", number_resp)
            lookAtEventResponses(event_list)
            pass

def lookAtEventResponses(resp):
    maximum = 0
    summand = 0
    for response in resp:
        summand += response['raw_attending_count']
        if len(response['raw_attending_count']) > maximum:
            maximum = len(response['raw_attending_count'])
    average = summand/len(resp)
    print(maximum)
    print(average)



def getPageInfo():
    attributes = 'number_of_events'
    page_list = []
    number_resp = 0
    next_key = ""
    page_iterator = TableIterator("FacebookPagesFiltered", "address_key")
    resp = page_iterator.batchGetItemAttributes(attributes)
    number_resp += len(resp['Items'])
    page_list.append(resp['Items'])

    try:
        next_key = resp['LastEvaluatedKey']
    except KeyError:
        pass

    while next_key != "":
        resp = page_iterator.batchGetItemAttributes(attributes, startkey=next_key)
        number_resp += len(resp['Items'])
        page_list.append(resp['Items'])
        try:
            next_key = resp['LastEvaluatedKey']
        except KeyError:
            next_key = ""
            pass
    print("done!", number_resp)
    lookAtPageResponses(page_list)

def lookAtPageResponses(resp):
    maximum = 0
    summand = 0
    for response in resp[0]:
        print(resp)
        summand += response['number_of_events']
        if response['number_of_events'] > maximum:
            maximum = response['number_of_events']
    average = summand/len(resp[0])
    print(maximum)
    print(average)



def getVenueInfo():
    attributes = 'facebook_id'
    page_list = []
    number_resp = 0
    next_key = ""
    page_iterator = TableIterator("YelpVenuesFiltered", "address_key")
    resp = page_iterator.batchGetItemAttributes(attributes)
    number_resp += len(resp['Items'])
    page_list.append(resp['Items'])

    try:
        next_key = resp['LastEvaluatedKey']
    except KeyError:
        pass

    while next_key != "":
        resp = page_iterator.batchGetItemAttributes(attributes, startkey=next_key)
        number_resp += len(resp['Items'])
        page_list.append(resp['Items'])
        try:
            next_key = resp['LastEvaluatedKey']
        except KeyError:
            next_key = ""
            #lookAtEventResponses(page_list)
            pass

    print("done!", number_resp)


if __name__ == "__main__":
    main()
