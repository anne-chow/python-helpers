import os
import boto3
import pytest
from moto import mock_s3, mock_secretsmanager


BUCKET = 'test_bucket'


@pytest.fixture(scope='module')
def aws_credentials():
    # set environment so that real infrasture is not accidentally changed
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

    # null this environment variable out to not use roles
    os.environ['METIS_AWS_ASSUME_ROLE'] = ""


@pytest.fixture(scope='module')
def s3_client(aws_credentials):
    with mock_s3():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=BUCKET)

        base_dir = os.path.dirname(os.path.realpath(__file__))

        for (parent, subdirs, files) in os.walk(os.path.join(base_dir, 'datafiles')):
            file_count = 0
            for name in files:
                if '__pycache__' not in parent and name != '.empty_folder':
                    file_count += 1
                    s3.upload_file(os.path.join(parent, name), BUCKET,
                                   os.path.join(parent.replace(base_dir+'/', ''), name))
            if not file_count:
                s3.put_object(Bucket=BUCKET, Key=parent.replace(base_dir+'/', '') + '/')

        yield s3


@pytest.fixture(scope='module')
def secretsmanager_client(aws_credentials):
    with mock_secretsmanager():
        sm = boto3.client('secretsmanager')
        sm.create_secret(Name='plain_secret', SecretString='A bad password')
        sm.create_secret(Name='json_secret', SecretString='{"key1": "val1", "key2": "val2"}')

        yield sm
