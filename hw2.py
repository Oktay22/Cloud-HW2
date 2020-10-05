import boto3
import csv

s3 = boto3.resource('s3',
    aws_access_key_id= 'access ket',
    aws_secret_access_key='secret access key'
)

try:
    s3.create_bucket(Bucket='datacont-oktay9922', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print ("this may already exist")


bucket = s3.Bucket("datacont-oktay9922")
bucket.Acl().put(ACL='public-read')

body = open('experiments.csv', 'rb')

o = s3.Object('datacont-oktay9922', 'test').put(Body=body )

s3.Object('datacont-oktay9922', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='access key',
    aws_secret_access_key='secret access key'
)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
        }
    )
except:
 #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print (item)
        body = open("experiments.csv", 'rb')
        s3.Object('datacont-oktay', item[3]).put(Body=body )
        md = s3.Object('datacont-oktay', item[3]).Acl().put(ACL='public-read')
        
        url = " https://668979971710.signin.aws.amazon.com/console "+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description' : item[4], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print ("item may already be there or another failure")
['experiment1', '1', '3/15/2002', 'exp1', 'this is the comment']
['experiment1', '2', '3/15/2002', 'exp2', 'this is the comment2']
['experiment2', '3', '3/16/2002', 'exp3', 'this is the comment3']
['experiment3', '4', '3/16/2002', 'exp4', 'this is the comment233']

response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
item = response['Item']
print(item)
