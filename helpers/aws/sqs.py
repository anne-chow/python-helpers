import json
from . import client


# set to 5 mins
DEFAULT_VISIBILITY_TIMEOUT = 300


def create_client():
    return client.create_client('sqs')


def send_message(queue_url, message_body, sqs_client=None):
    if sqs_client is None:
        sqs_client = create_client()

    response = sqs_client.send_message(QueueUrl=queue_url,
                                       MessageBody=message_body)

    return response['MessageId']


def get_message_count(queue_url, sqs_client=None):
    if sqs_client is None:
        sqs_client = create_client()

    response = sqs_client.get_queue_attributes(QueueUrl=queue_url,
                                               AttributeNames=['ApproximateNumberOfMessages'])

    return int(response['Attributes']['ApproximateNumberOfMessages'])


def poll_for_message(queue_url, max_message=None, visibility_timeout=DEFAULT_VISIBILITY_TIMEOUT,
                     wait_time_seconds=None, sqs_client=None):
    if visibility_timeout is None:
        visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

    if sqs_client is None:
        sqs_client = create_client()

    kwargs = {'VisibilityTimeout': visibility_timeout}
    if max_message:
        kwargs['MaxNumberOfMessages'] = max_message
    if wait_time_seconds:
        kwargs['WaitTimeSeconds'] = wait_time_seconds

    response = sqs_client.receive_message(QueueUrl=queue_url, **kwargs)

    return response['Messages'] if response is not None and 'Messages' in response else []


def change_visibility(queue_url, receipt_handle, visibility_timeout=DEFAULT_VISIBILITY_TIMEOUT, sqs_client=None):
    if visibility_timeout is None:
        visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

    if sqs_client is None:
        sqs_client = create_client()

    sqs_client.change_message_visiblity(QueueUrl=queue_url,
                                        ReceiptHandle=receipt_handle,
                                        VisibilityTimeout=visibility_timeout)


def delete_message(queue_url, receipt_handle, sqs_client=None):
    if sqs_client is None:
        sqs_client = create_client()

    sqs_client.delete_message(QueueUrl=queue_url,
                              ReceiptHandle=receipt_handle)


def is_test_event_message(message_record):
    if 'Body' in message_record:
        json_message = json.loads(message_record['Body'], encoding='utf-8')

        if 'Service' in json_message and json_message['Service'] == 'Amazon S3' \
                and 'Event' in json_message and json_message['Event'] == 's3:TestEvent':
            return True

    return False


def parse_s3_objects_from_message(message_record):
    s3_objects = []

    if 'Body' in message_record:
        json_message = json.loads(message_record['Body'], encoding='utf-8')

        if 'Records' in json_message:
            for record in json_message['Records']:
                if 's3' in record \
                        and 'bucket' in record['s3'] and 'name' in record['s3']['bucket'] \
                        and 'object' in record['s3'] and 'key' in record['s3']['object']:
                    s3_objects.append({
                        'bucket': record['s3']['bucket']['name'],
                        'key': record['s3']['object']['key'],
                        'size': record['s3']['object']['size']
                    })

    return s3_objects
