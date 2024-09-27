import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')


def lambda_handler(event, context):

    fromBucket = event['fromBucket']
    fromKey = event['fromKey']
    toBucket = event['toBucket']
    toKey = event['toKey']

    #create source dictionary
    copy_source = {
        'Bucket': 'fromBucket',
        'Key': 'fromKey'
    }

    copyReturn = s3.meta.client.copy(copy_source, 'toBucket', 'toKey')

    print("copy completed")

    logger.info(f"CloudWatch logs group: {context.log_group_name}")

    #return the output as a JSON string

    return json.dumps(copyReturn)

