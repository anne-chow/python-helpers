import pytest
import os
from helpers.aws import s3

BUCKET = 'test_bucket'


def test_prefix_exists(s3_client):
    assert s3.prefix_exists(BUCKET, 'datafiles/test.txt')
    assert s3.prefix_exists(BUCKET, 'datafiles/subfolder1/test3.txt')
    assert not s3.prefix_exists(BUCKET, 'datafiles/subfolder/dummy')


def test_uri_exists(s3_client):
    assert s3.uri_exists(f"s3://{BUCKET}/datafiles/test.txt")
    assert s3.uri_exists(f"s3://{BUCKET}/datafiles/subfolder1/test3.txt")
    assert not s3.uri_exists(f"s3://{BUCKET}/datafiles/subfolder/dummy")


def test_yield_file_list():
    assert sorted(list(s3.yield_file_list(BUCKET, 'datafiles'))) == \
           sorted(['datafiles/subfolder1/test3.txt', 'datafiles/test.txt', 'datafiles/test2.txt'])

    assert sorted(list(s3.yield_file_list(BUCKET, 'datafiles', max_keys=2))) == \
           sorted(['datafiles/subfolder1/test3.txt', 'datafiles/test.txt'])

    assert list(s3.yield_file_list(BUCKET, 'dummy')) == []


def test_get_file_list():
    actual_file_list, cont_token = s3.get_file_list(BUCKET, 'datafiles')
    assert sorted(actual_file_list) == sorted(['datafiles/test.txt',
                                               'datafiles/test2.txt',
                                               'datafiles/subfolder1/test3.txt'])
    assert cont_token is None

    actual_file_list, cont_token = s3.get_file_list(BUCKET)
    assert sorted(actual_file_list) == sorted(['datafiles/test.txt',
                                               'datafiles/test2.txt',
                                               'datafiles/subfolder1/test3.txt'])
    assert cont_token is None

    actual_file_list, cont_token = s3.get_file_list(BUCKET, 'datafiles', max_keys=2)
    assert sorted(actual_file_list) == sorted(['datafiles/test.txt',
                                               'datafiles/subfolder1/test3.txt'])
    assert cont_token is not None

    actual_file_list, cont_token = s3.get_file_list(BUCKET, 'datafiles', continuation_token=cont_token)
    assert actual_file_list == ['datafiles/test2.txt']
    assert cont_token is None

    actual_file_list, cont_token = s3.get_file_list(BUCKET, 'dummy', continuation_token=cont_token)
    assert actual_file_list == []
    assert cont_token is None


def test_yield_folder_list():
    assert sorted(list(s3.yield_folder_list(BUCKET, 'datafiles'))) == ['datafiles/subfolder1/',
                                                                       'datafiles/subfolder2/']
    assert list(s3.yield_folder_list(BUCKET)) == ['datafiles/']
    assert list(s3.yield_folder_list(BUCKET, 'dummy')) == []


def test_get_recursive_folder_list():
    assert sorted(s3.get_recursive_folder_list(BUCKET, 'datafiles')) == ['datafiles/subfolder1/',
                                                                         'datafiles/subfolder2/']
    assert sorted(s3.get_recursive_folder_list(BUCKET)) == ['datafiles/subfolder1/', 'datafiles/subfolder2/']
    assert s3.get_recursive_folder_list(BUCKET, 'dummy') == []


def test_get_folder_list():
    actual_folder_list, cont_token = s3.get_folder_list(BUCKET)
    assert actual_folder_list == ['datafiles/']
    assert cont_token is None

    actual_folder_list, cont_token = s3.get_folder_list(BUCKET, 'datafiles')
    assert sorted(actual_folder_list) == ['datafiles/subfolder1/', 'datafiles/subfolder2/']
    assert cont_token is None

    actual_folder_list, cont_token = s3.get_folder_list(BUCKET, 'datafiles', max_keys=1)
    assert actual_folder_list == ['datafiles/subfolder1/']
    assert cont_token is not None

    actual_folder_list, cont_token = s3.get_folder_list(BUCKET, 'datafiles', continuation_token=cont_token)
    assert actual_folder_list == ['datafiles/subfolder2/']
    assert cont_token is None

    actual_folder_list, cont_token = s3.get_folder_list(BUCKET, 'dummy', continuation_token=cont_token)
    assert actual_folder_list == []
    assert cont_token is None


def test_is_folder_empty():
    assert not s3.is_folder_empty(BUCKET)
    assert not s3.is_folder_empty(BUCKET, 'datafiles/')
    assert not s3.is_folder_empty(BUCKET, 'datafiles/subfolder1/')
    assert s3.is_folder_empty(BUCKET, 'datafiles/subfolder2/')
    assert s3.is_folder_empty(BUCKET, 'dummy/')
