import base64
import simplejson as json
from . import client
from .. import log
from botocore.exceptions import ClientError
from cachetools.func import ttl_cache


@ttl_cache(maxsize=None, ttl=900)
def get_secret(secret_id, region_name=None):
    sm = client.create_client('secretsmanager', region_name=region_name)

    try:
        resp = sm.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        log.get_logger().warning("Failed to retrieve secret for ID {}: {}".format(secret_id, e))
        raise e
    else:
        if 'SecretString' in resp:
            secret = resp['SecretString']
        elif 'SecretBinary' in resp:
            secret = base64.b64decode(resp['SecretBinary'])
        else:
            raise ValueError(f"Failed to parse result for secret for ID {secret_id}, resp={resp}")

        if secret is not None:
            # if secret is json, decode to a dictionary; otherwise, return secret as a string
            try:
                decoded_secret = json.loads(secret)
            except ValueError as e:
                ### not json
                pass
            else:
                secret = decoded_secret

    return secret
