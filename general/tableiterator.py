import json
import boto3


class TableIterator:

    BATCHGETITEMSIZE = 20
    tableName = ""
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table("")
    defaultJSON = {}
    PRIMARYKEY = 'address_key'
    PRIMARYKEYTYPE = 'S'
    UPDATEEXPRESSION = 'SET'


    def __init__(self, table):
        self.tableName = table
        self.table = self.dynamodb.Table(self.tableName)

    def getTableName(self):
        return tableName

    def setTableName(self, newTableName):
        self.tableName = newTableName
        self.table = dynamodb.Table(tableName)

    def narcolepsy(self):
        randy = randint(10, 20)
        print("Sleeping:", randy)
        sleep(randy)

    def getBatchGetItemSize(self):
        return self.BATCHGETITEMSIZE

    def setBatchGetItemSize(self, newSize):
        self.BATCHGETITEMSIZE = newSize

    def batchGetItemAttributeFirst(self, attribute):
        response = self.table.scan(TableName=self.tableName, ProjectionExpression=attribute)
        return response

    def batchGetItemWithNameFirst(self, attributes):
        projection = '#N,' + attributes
        response = self.table.scan(TableName=self.tableName, ProjectionExpression=projection, ExpressionAttributeNames={'#L' : 'location', '#N' : 'name'})
        return response

    def batchGetItemAttributes(self, attributes, startKey):
        response = self.table.scan(TableName=self.tableName, ProjectionExpression=attributes, ExclusiveStartKey=startKey)
        return response

    def batchGetItemWithName(self, attributes, startKey):
        projection = '#N,' + attributes
        response = self.table.scan(TableName=self.tableName, ProjectionExpression=projection, ExclusiveStartKey=startKey, ExpressionAttributeNames= {'#L' : 'location', '#N' : 'name'})
        return response

    def batchGetItemAll(self):
        select = "ALL_ATTRIBUTES"
        response = self.table.scan(TableName=self.tableName, select=select, ComparisonOperator=comparisonOperator)

    def updateItem(self, item, attributeToUpdate, updatedValue, attributeValueType):
        address_key = item[self.PRIMARYKEY]
        print(address_key)
        updateExpression = self.UPDATEEXPRESSION + " " + attributeToUpdate + "=:" + updatedValue
        self.table.update_item(TableName=self.tableName, Key={self.PRIMARYKEY: address_key}, UpdateExpression=updateExpression, ExpressionAttributeValues={":"+updatedValue : updatedValue})
        #try:
        #    self.table.update_item(TableName=self.tableName, Key={self.PRIMARYKEY: {self.PRIMARYKEYTYPE : address_key}}, AttributeUpdates={attributeToUpdate : {'Value' : {attributeValueType : updatedValue}}})
        #except Exception:
        #    print('failed to update item')
