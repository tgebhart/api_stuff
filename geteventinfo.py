import json
import boto3
import requests
import re
from decimal import Decimal
from general.tableiterator import TableIterator
from facebookapi.facebookapi import FacebookApi


ITERATORTABLENAME = "FacebookPages"
ATTENDEETABLENAME = "FacebookAttendees"
EVENTTABLENAME = "FacebookEvents"
ATTRIBUTES = "facebook_event_id"
EVENTATTRIBUTES = "attending"
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
eventtable = dynamodb.Table(EVENTTABLENAME)


def getAttending():
    totalIdCount = 0
    tableiter = TableIterator(EVENTTABLENAME)
    tableiter.setPrimaryKey('facebook_event_id')
    attendeeiter = TableIterator(ATTENDEETABLENAME)
    attendeeiter.setPrimaryKey('facebook_user_id')
    first_response = tableiter.batchGetItemAttributes(ATTRIBUTES)
    last_key = first_response['LastEvaluatedKey']
    facebookapi = FacebookApi()
    facebookapi.generateAccessToken()

    for response in first_response['Items']:
        print(response)
        attendee_list = makeAttendeeList(facebookapi, response)
        updateAttendees(response, attendee_list, tableiter, attendeeiter)

    while last_key is not None:
        next_response = tableiter.batchGetItemAttributes(ATTRIBUTES, startkey=last_key)
        print(next_response)
        for response in next_response['Items']:
            attendee_list = makeAttendeeList(facebookapi, response)
            updateAttendees(response, attendee_list, tableiter, attendeeiter)
        try:
            last_key = next_response['LastEvaluatedKay']
        except KeyError:
            last_key = None
            pass



def updateAttendees(response, attendee_list, tableiter, attendeeiter):
    if attendee_list is not None:
        num_attendees = len(attendee_list)
        tableiter.updateItemSet(response['facebook_event_id'], 'attending', attendee_list)
        tableiter.updateItemSet(response['facebook_event_id'], 'raw_attending_count', num_attendees)
        for attendee in attendee_list:
            print(attendee)
            new_attendee = {}
            new_attendee['facebook_user_id'] = attendee['id']
            new_attendee['facebook_name'] = attendee['name']
            new_attendee['raw_events_attended'] = []
            attendeeiter.putItem(new_attendee)
            attendeeiter.updateItemSetList(new_attendee['facebook_user_id'], 'raw_events_attended', [response['facebook_event_id']])


def makeAttendeeList(facebookapi, response):
    attendee_list = None
    nextpage = None
    eventResponse = facebookapi.getEventInfo(response['facebook_event_id'], EVENTATTRIBUTES)

    try:
        attendee_list = eventResponse['attending']['data']
        try:
            nextpage = eventResponse['attending']['paging']['next']
        except KeyError:
            nextpage = None
    except KeyError:
        return None
        pass

    while nextpage is not None:
        nextresponse = facebookapi.getEventInfo(response['facebook_event_id'], EVENTATTRIBUTES, startkey=nextpage)
        try:
            next_list = nextresponse['data']
            for person in next_list:
                print(person)
                attendee_list.append(person)
            try:
                nextpage = nextresponse['paging']['next']
            except KeyError:
                nextpage = None
        except KeyError:
            pass

    return attendee_list


getAttending()
