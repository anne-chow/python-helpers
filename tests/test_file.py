import pytest
import os
from helpers import file


def test_local_path_exists():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    assert file.local_path_exists(os.path.join(base_dir, 'datafiles'))
    assert not file.local_path_exists(os.path.join(base_dir, 'dummy'))


def test_ensure_local_path_exists():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dummy')
    assert not file.local_path_exists(test_dir)

    try:
        file.ensure_local_path_exists(test_dir)
        assert file.local_path_exists(test_dir)
    finally:
        os.rmdir(test_dir)

    assert not file.local_path_exists(test_dir)


def test_format_folder_name():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dummy')
    assert file.format_folder_name(test_dir) == f'{test_dir}/'

    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dummy/')
    assert file.format_folder_name(test_dir) == f'{test_dir}'


def test_list_files_recursively():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datafiles')
    expected = [
        os.path.join(test_dir, 'test.txt'),
        os.path.join(test_dir, 'test2.txt'),
        os.path.join(test_dir, 'subfolder2/.empty_folder'),
        os.path.join(test_dir, 'subfolder1/test3.txt'),
    ]
    assert sorted(file.list_files_recursively(test_dir)) == sorted(expected)

    assert file.list_files_recursively(os.path.join(test_dir, 'dummy')) == []


def test_get_file_size():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datafiles')
    assert file.get_file_size(os.path.join(test_dir, 'test.txt')) == 38
    assert file.get_file_size(os.path.join(test_dir, 'test2.txt')) == 0

    with pytest.raises(Exception):
        file.get_file_size(os.path.join(test_dir, 'test3.txt'))


def test_peek_file_line():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datafiles')
    with open(os.path.join(test_dir, 'test.txt'), 'r') as fh:
        assert file.peek_file_line(fh) == 'This is line 1\n'

    with open(os.path.join(test_dir, 'test2.txt'), 'r') as fh:
        assert file.peek_file_line(fh) == ''


def test_delete_local_path():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dummy')

    try:
        file.ensure_local_path_exists(test_dir)
        assert file.local_path_exists(test_dir)
        file.delete_local_path(test_dir)
    finally:
        try:
            os.rmdir(test_dir)
        except:
            pass

    assert not file.local_path_exists(test_dir)


def test_compress_file():
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'datafiles')

    orig_file_name = os.path.join(test_dir, 'test.txt')
    zipped_name = None
    try:
        zipped_name = file.compress_file(orig_file_name)
        assert zipped_name == f"{orig_file_name}.gz"
        assert file.get_file_size(zipped_name) == 61
    finally:
        if zipped_name is not None:
            os.remove(zipped_name)


