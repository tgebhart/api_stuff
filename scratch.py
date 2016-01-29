import json
import boto3
import requests
import re
from decimal import Decimal
from general.tableiterator import TableIterator
from facebookapi.facebookapi import FacebookApi

EVENTATTRIBUTES = "attending"

facebookapi = FacebookApi()
facebookapi.generateAccessToken()
print(facebookapi.getEventInfo('201180990007144', EVENTATTRIBUTES))
