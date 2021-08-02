import os
import boto3
from cachetools.func import ttl_cache
from pprint import pprint


@ttl_cache(maxsize=None, ttl=43200)
def assume_role():
    sts_client = boto3.client('sts')

    role_arn = os.getenv('METIS_AWS_ROLE_ARN')
    if role_arn is None:
        role_arn = input('***** Enter AWS Role ARN: ')

    mfa_token = os.getenv('METIS_AWS_MFA_TOKEN')
    if mfa_token is None:
        mfa_token = input('***** Enter AWS MFA token: ')

    serial_number = os.getenv('METIS_AWS_MFA_ARN')
    if serial_number is None:
        serial_number = input('***** Enter AWS MFA ARN: ')

    assumed_role = sts_client.assume_role(RoleArn=role_arn,
                                          RoleSessionName='MetisCoreSession',
                                          SerialNumber=serial_number,
                                          DurationSeconds=43200,
                                          TokenCode=mfa_token)
    pprint(assumed_role)

    return assumed_role


def create_client(resource_type, use_role=None, region_name=None, assumed_role=None):
    if region_name is None:
        region_name = os.getenv('AWS_REGION_NAME', 'us-east-1')

    kwargs = {'region_name': region_name}

    if use_role is None:
        use_role = os.getenv('METIS_AWS_ASSUME_ROLE', False)

    if use_role:
        assumed_role = assumed_role if assumed_role else assume_role()
        client = boto3.client(resource_type,
                              aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                              aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                              aws_session_token=assumed_role['Credentials']['SessionToken'],
                              **kwargs)
    else:
        client = boto3.client(resource_type, **kwargs)

    return client


def create_resource(resource_type, use_role=None, region_name=None, assumed_role=None):
    if region_name is None:
        region_name = os.getenv('AWS_REGION_NAME', 'us-east-1')

    kwargs = {'region_name': region_name}

    if use_role is None:
        use_role = os.getenv('METIS_AWS_ASSUME_ROLE', False)

    if use_role:
        assumed_role = assumed_role if assumed_role else assume_role()
        session = boto3.Session(aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                                aws_session_token=assumed_role['Credentials']['SessionToken'])
        resource = session.resource(resource_type, **kwargs)
    else:
        resource = boto3.resource(resource_type, **kwargs)

    return resource
