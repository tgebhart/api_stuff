from oautheventsapi import OauthEventsApi
from datetime import date
import json
import requests


class EventBriteEventApi(OauthEventsApi):
    baseUrl = 'http://www.eventbriteapi.com'
    search_path = '/v3/events/search/'
    jsonLocation = '../responses/events/eventbriteeventsapi.json'
    oath_consumer_key = ""
    oath_token = ""
    oath_token_secret = ""
    oath_signature_method = "hmac-sha1"
    oath_signature = ""
    oath_nonce = ""


    def _init_(self):
        print('init')

    def generalVenueExploreR(self, venue=None):
        self.findSecrets("eventbrite")
        bearerString = "Bearer " + self.oath_token
        headerSet = {"Authorization" : bearerString}
        r = requests.get(self.baseUrl + self.search_path, headers=headerSet, verify=True)
        print(r.text)
        return r


    def parseResponse(self):
        jason = self.generalVenueExploreR().text

    def saveJSONResponse(self, venue=None):
        op = open(self.jsonLocation, 'w')
        json.dump(self.generalVenueExploreR(venue).text, op)
        op.close


ebapi = EventBriteEventApi()
ebapi.saveJSONResponse()
