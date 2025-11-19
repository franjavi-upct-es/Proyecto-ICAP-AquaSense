# This function is triggered by an object being created in an Amazon S3 bucket.
# The file is downloaded and each line is inserted into a DynamoDB table.
import json, urllib, boto3, csv
from datetime import date
# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
# Connect to the DynamoDB tables
measureTable = dynamodb.Table('Measures')
# This handler is run every time the Lambda function is triggered
def lambda_handler(event, context):
# Show the incoming event in the debug log
   print("Event received by Lambda function: " + json.dumps(event, indent=2))
# Get the bucket and object key from the Event
   bucket = event['Records'][0]['s3']['bucket']['name']
   key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
   localFilename = '/tmp/measures.csv'
# Download the file from S3 to the local filesystem
   try:
      s3.meta.client.download_file(bucket, key, localFilename)
   except Exception as e:
      print(e)
      print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
      raise e
# Read the Inventory CSV file
   with open(localFilename) as csvfile:
      reader = csv.DictReader(csvfile, delimiter=',')
# Read each row in the file
      rowCount = 0
      for row in reader:
         rowCount += 1
# Show the row in the debug log
         try:
            fecha = date.strptime(f"{row['Fecha']}", "%Y/%m/%d")
            mes = fecha.month
            año = fecha.year
            if fecha.day < 4:
               mes -= 1
               if mes == 0:
                  mes = 12
                  año -= 1

# Insert Store, Item and Count into the Inventory table
            measureTable.put_item(
               Item={
               'date': row['Fecha'],
               'month': mes,
               'year': año,
               'avg': float(row['Medias']),
               'sd': float(row['Desviaciones'])})
         except Exception as e:
            print(e)
            print("Unable to insert data into DynamoDB table".format(e))
# Finished!

   return "%d counts inserted" % rowCount
