import json
import boto3


class TableIterator:

    BATCHGETITEMSIZE = 20
    TABLE_NAME = ""
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table("")
    defaultJSON = {}
    PRIMARYKEY = ''
    PRIMARYKEYTYPE = 'S'
    UPDATEEXPRESSION = 'SET'


    def __init__(self, table, primary_key):
        self.TABLE_NAME = table
        self.table = self.dynamodb.Table(self.TABLE_NAME)
        self.PRIMARYKEY = primary_key

    def getTableName(self):
        return self.TABLE_NAME

    def setTableName(self, newTableName):
        self.TABLE_NAME = newTableName
        self.table = dynamodb.Table(TABLE_NAME)

    def getPrimaryKey(self):
        return self.PRIMARYKEY

    def setPrimaryKey(self, primarykey):
        self.PRIMARYKEY = primarykey

    def getPrimaryKeyType(self):
        return self.PRIMARYKEYTYPE

    def setPrimaryKeyType(self, primary_key_type):
        self.PRIMARYKEYTYPE = primary_key_type

    def getUpdateExpression(self):
        return self.UPDATEEXPRESSION

    def setUpdateExpression(self, update_expression):
        self.UPDATEEXPRESSION = update_expression

    def narcolepsy(self):
        randy = randint(10, 20)
        print("Sleeping:", randy)
        sleep(randy)

    def putItem(self, item):
        self.table.put_item(Item=item)

    def getItem(self, key_value, attributes):
        projection = attributes
        key = {self.PRIMARYKEY : key_value}
        print(key)
        return self.table.get_item(TableName=self.TABLE_NAME, Key=key, ProjectionExpression=projection)

    def getBatchGetItemSize(self):
        return self.BATCHGETITEMSIZE

    def setBatchGetItemSize(self, newSize):
        self.BATCHGETITEMSIZE = newSize

    def batchGetItemAttributeFirst(self, attribute):
        response = self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=attribute)
        return response

    def batchGetItemWithNameFirst(self, attributes):
        projection = '#N,' + attributes
        response = self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=projection, ExpressionAttributeNames={'#L' : 'location', '#N' : 'name'})
        return response

    def batchGetItemAttributes(self, attributes, startkey=None):
        if startkey is not None:
            return self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=attributes, ExclusiveStartKey=startkey)
        return self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=attributes)

    def batchGetItemWithName(self, attributes, startKey):
        projection = '#N,' + attributes
        response = self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=projection, ExclusiveStartKey=startKey, ExpressionAttributeNames= {'#L' : 'location', '#N' : 'name'})
        return response

    def batchGetItemAll(self):
        select = "ALL_ATTRIBUTES"
        response = self.table.scan(TableName=self.TABLE_NAME, select=select, ComparisonOperator=comparisonOperator)

    def updateItemSet(self, itemkey, attributeToUpdate, updatedValue):
        key = itemkey
        updateExpression = 'SET' + " " + attributeToUpdate + "= :l"
        self.table.update_item(TableName=self.TABLE_NAME, Key={self.PRIMARYKEY: key}, UpdateExpression=updateExpression, ExpressionAttributeValues={":l" : updatedValue})
        #try:
        #    self.table.update_item(TableName=self.TABLE_NAME, Key={self.PRIMARYKEY: {self.PRIMARYKEYTYPE : address_key}}, AttributeUpdates={attributeToUpdate : {'Value' : {attributeValueType : updatedValue}}})
        #except Exception:
        #    print('failed to update item')

    def updateItemSetList(self, itemkey, attributeToUpdate, updatedValue):
        key = itemkey
        updateExpression = "SET " + attributeToUpdate + " =list_append(" + attributeToUpdate + ", :l)"
        self.table.update_item(TableName=self.TABLE_NAME, Key={self.PRIMARYKEY: key}, UpdateExpression=updateExpression, ExpressionAttributeValues={":l" : updatedValue})

    def updateItemAdd(self, itemkey, attributeToUpdate, updatedValue):
        key = itemkey
        print("attributetoupdate", updatedValue)
        updateExpression = 'ADD' + " " + attributeToUpdate + " :l"
        self.table.update_item(TableName=self.TABLE_NAME, Key={self.PRIMARYKEY: key}, UpdateExpression=updateExpression, ExpressionAttributeValues={":l" : updatedValue})


    def batchGetItemWithFBID(self, attributes=None, startkey=None):
        if attributes is not None:
            projection = 'address_key, facebook_id, ' + attributes
        else:
            projection = 'address_key, facebook_id'
        filterexpression = 'attribute_exists(facebook_id)'
        if startkey is not None:
            response = self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=projection, FilterExpression=filterexpression, ExclusiveStartKey=startkey)
            return response
        response = self.table.scan(TableName=self.TABLE_NAME, ProjectionExpression=projection, FilterExpression=filterexpression)
        return response
