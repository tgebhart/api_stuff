import json
import boto3
import requests
import re
from random import randint
from time import sleep

class FacebookApi:

    secretsLocation = '/home/tgebhart/Documents/Work/aivibe/code/api_stuff/keys/accesskeys.json'
    app_id = ""
    app_secret = ""
    authHash = ""
    access_token = ""
    eventsSince = "1/31/2013"
    APIENDPOINT = "https://graph.facebook.com/v2.5"
    SEARCHENDPOINT = "https://graph.facebook.com/search"
    LOCATIONCENTER = '37.78, -122.417'
    DISTANCE = '500'

    def _init_(self):
        print('locationapi init')
        self.findSecrets()

    def findSecrets(self):
        with open(self.secretsLocation) as data_file:
            jason = json.load(data_file)
        for s in jason:
            if s == "facebook":
                self.app_id = jason[s][0]['app_id']
                self.app_secret = jason[s][1]['app_secret']
        self.authHash = self.app_id + "|" + self.app_secret

    def generateAccessToken(self):
        self.findSecrets()
        append = "/oauth/access_token"
        payload = {'client_id' : self.app_id, 'client_secret' : self.app_secret, 'grant_type' : 'client_credentials'}
        resp = json.loads(requests.get(self.APIENDPOINT + append, params=payload).text)
        self.access_token = resp['access_token']

    def getSecretsLocation(self):
        return self.secretsLocation

    def setSecretsLocation(self, newString):
        self.secretsLocation = newString

    def getTimeStamp(self):
        return time.time()

    def acuteNarcolepsy(self):
        randy = randint(1, 5)
        print("Quick Nap", randy)
        sleep(randy)

    def chronicNarcolepsy(self):
        randy = randint(20, 25)
        print("Shhhh...sleeping", randy)
        sleep(randy)

    def getEventsSince(self):
        return self.eventsSince

    def setEventsSince(self, date):
        self.eventsSince = date

    def getEventInfo(self, id, attribute, startkey=None):
        if startkey is None:
            payload = {'id' : id, 'fields' : attribute, 'access_token' : self.access_token}
            return json.loads(requests.get(self.APIENDPOINT, params=payload).text)
        return json.loads(requests.get(startkey).text)

    def getHistoricalEvents(self, id, startkey=None):
        if startkey is None:
            payload = {'id' : id, 'fields' : 'events', 'access_token' : self.access_token}
            events = json.loads(requests.get(self.APIENDPOINT, params=payload).text)
        else:
            events = json.loads(requests.get(startkey).text)
        nextpage = ""
        ret = None

        try:
            ret = events['events']['data']
        except KeyError:
            try:
                ret = events['data']
            except KeyError:
                print("keyerror line 63", events)
                pass
            pass

        try:
            nextpage = events['events']['paging']['next']
        except KeyError:
            try:
                nextpage = events['next']
            except KeyError:
                pass
            #print("keyerror line 69")
            pass

        if nextpage is not "" and ret is not None:
            return (ret, nextpage)
        if ret is not None:
            return (ret, None)
        else:
            return (None, None)

    def facebookSearchName(self, name):
        payload = {'q' : name, 'type' : 'page', 'center' : self.LOCATIONCENTER, 'distance' : self.DISTANCE, 'topic_filter' : 'all', 'access_token' : self.access_token}
        return json.loads(requests.get(self.SEARCHENDPOINT, params=payload).text)

    def facebookGetAddress(self, id):
        payload = {'id' : id, 'fields' : {'name', 'location'}, 'access_token' : self.access_token}
        return json.loads(requests.get(self.APIENDPOINT, params=payload).text)
