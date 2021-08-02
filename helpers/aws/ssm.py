import time
from . import client
from .. import util
from botocore.exceptions import ClientError


# APIs for AWS Systems Manager
def get_parameter(parameter_name, region_name=None, required=True):
    parameter = _get_parameter(parameter_name, region_name, required)

    if not parameter:
        return parameter

    parameter_value = parameter['Parameter']['Value']

    return util.convert_string_to_json(parameter_value)


def _get_parameter(parameter_name, region_name=None, required=True):
    ssm_client = client.create_client('ssm', region_name)

    retry_count = 5

    while retry_count > 0:
        try:
            return ssm_client.get_parameter(Name=parameter_name)
        except ClientError as e:
            # ignore exception if parameter store is not required and it doesn't exist
            if not required and e.response['Error']['Code'] == 'ParameterNotFound':
                return {}
            elif e.response['Error']['Code'] == 'ThrottlingException':
                retry_count -= 1
                if retry_count > 0:
                    print("Got throttled while getting parameter store for {}, will retry: {}"
                          .format(parameter_name, e))
                    time.sleep(1)
                else:
                    print("Got throttled while getting parameter store for {}: {}".format(parameter_name, e))
                    raise e
            else:
                print("Failed to get parameter store for {}: {}".format(parameter_name, e))
                raise e
