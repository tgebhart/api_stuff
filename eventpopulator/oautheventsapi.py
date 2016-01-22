from eventsapi import EventsApi
import json
import boto3
import oauth2
import sys
import urllib.request as urllib2
import urllib.parse as urllib
import requests

class OauthEventsApi(EventsApi):

    def superSecureRequest(self, host, path, url_params=None):
        """Prepares OAuth authentication and sends the request to the API.
        Args:
            host (str): The domain host of the API.
            path (str): The path of the API after the domain.
            url_params (dict): An optional set of query parameters in the request.
        Returns:
            dict: The JSON response from the request.
        Raises:
            urllib2.HTTPError: An error occurs from the HTTP request.
        """
        url_params = url_params or {}
        url = 'https://{0}{1}?'.format(host, urllib.quote(path.encode('utf8')))

        consumer = oauth2.Consumer(self.oath_consumer_key, self.oath_consumer_secret)
        oauth_request = oauth2.Request(
            method="GET", url=url, parameters=url_params)

        oauth_request.update(
            {
                'oauth_nonce': oauth2.generate_nonce(),
                'oauth_timestamp': oauth2.generate_timestamp(),
                'oauth_token': self.oath_token,
                'oauth_consumer_key': self.oath_consumer_key
            }
        )
        token = oauth2.Token(self.oath_token, self.oath_token_secret)
        oauth_request.sign_request(
            oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
        signed_url = oauth_request.to_url()

        conn = urllib2.urlopen(signed_url, None)
        try:
            response = json.loads(conn.read().decode())
        finally:
            conn.close()

        return response

    def findSecrets(self, serviceprovider):
        with open(self.getSecretsLocation()) as data_file:
            jason = json.load(data_file)
        for s in jason:
            if s == serviceprovider:
                self.oath_consumer_key = jason[s][0]['oath_consumer_key']
                self.oath_consumer_secret = jason[s][1]['oath_consumer_secret']
                self.oath_token = jason[s][2]['oath_token']
                self.oath_token_secret = jason[s][3]['oath_token_secret']
