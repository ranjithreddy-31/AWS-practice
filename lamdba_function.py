import io
import csv
import boto3
import json

s3Client = boto3.client('s3')

def lambda_handler(event, contest):
    '''
    This lambda function is triggered whenever there is a file upload to s3. It fetches data from that file and prints it.
    '''
    # Get Bucket and File name
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Get our object
    response = s3Client.get_object(Bucket=bucket, Key=key)

    #Process and print the data
    data = response['Body'].read.decode('utf-8')
    reader = csv.reader(io.StringIO(data))
    next(reader)
    for row in data:
        print(row)

