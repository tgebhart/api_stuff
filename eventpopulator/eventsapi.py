class EventsApi:
    nearString = 'San Francisco, CA'
    secretsLocation = '../keys/accesskeys.json'
    def _init_(self):
        print('locationapi init')

    def getNearString(self):
        return self.nearString

    def setNearString(self, newString):
        self.nearString = newString

    def getSecretsLocation(self):
        return self.secretsLocation

    def setSecretsLocation(self, newString):
        self.secretsLocation = newString
