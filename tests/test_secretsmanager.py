import pytest
from botocore.exceptions import ClientError
from helpers.aws import secretsmanager


def test_prefix_exists(secretsmanager_client):
    assert secretsmanager.get_secret('plain_secret') == 'A bad password'
    assert secretsmanager.get_secret('json_secret') == {'key1': 'val1', 'key2': 'val2'}

    with pytest.raises(ClientError):
        secretsmanager.get_secret('dummy')

