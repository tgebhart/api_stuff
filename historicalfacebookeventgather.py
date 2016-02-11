import json
import boto3
import requests
import re
from decimal import Decimal
from general.tableiterator import TableIterator
from facebookapi.facebookapi import FacebookApi


ITERATORTABLENAME = "YelpVenuesFiltered"
PAGETABLENAME = "FacebookPagesFiltered"
EVENTTABLENAME = "FacebookEventsFiltered"
ATTRIBUTES = "address_key, #L"
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
pagetable = dynamodb.Table(PAGETABLENAME)
eventtable = dynamodb.Table(EVENTTABLENAME)

tableiter = TableIterator(ITERATORTABLENAME, 'address_key')
response = tableiter.batchGetItemWithFBID()
lastkey = response['LastEvaluatedKey']
facebookapi = FacebookApi()
facebookapi.generateAccessToken()

while lastkey is not None:
    for item in response['Items']:
        print(item)
        eventidlist = []
        (events, nextkey) = facebookapi.getHistoricalEvents(item['facebook_id'])
        if events is not None:
            eventslist = []
            for pagedevent in events:
                eventslist.append(pagedevent)
                while nextkey is not None:
                    facebookapi.chronicNarcolepsy()
                    (nextevents, nextkey) = facebookapi.getHistoricalEvents(item['facebook_id'], startkey=nextkey)
                    for nextpagedevent in nextevents:
                        eventslist.append(nextpagedevent)
                for event in eventslist:
                    venue_facebook_id = None
                    print("event", event)
                    event_id = event['id']
                    try:
                        venue_facebook_id = event['place']['id']
                    except KeyError:
                        pass
                    eventidlist.append(event_id)
                    event['address_key'] = item['address_key']
                    event['facebook_venue_id'] = venue_facebook_id
                    event['facebook_event_id'] = event_id
                    try:
                        event['place']['location']['latitude'] = Decimal(event['place']['location']['latitude']).quantize(5)
                        event['place']['location']['longitude'] = Decimal(event['place']['location']['longitude']).quantize(5)
                    except KeyError:
                        pass
                    print("=========================")
                    print("bottom event", event['name'])
                    eresp = eventtable.put_item(Item=event)
                    print("event response: ", eresp)
                item['event_list'] = eventidlist
                item['number_of_events'] = len(eventidlist)
                item['first_event_start_date'] = eventslist[len(eventslist) - 1]['start_time']
                item['last_event_start_date'] = eventslist[0]['start_time']
                presp = pagetable.put_item(Item=item)
                print("pagetable response: ", presp)

    response = tableiter.batchGetItemWithFBID(startkey=lastkey)
    try:
        lastkey = response['LastEvaluatedKey']
    except KeyError:
        lastkey = None
