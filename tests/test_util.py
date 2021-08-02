import pytest
from helpers import util


def test_is_date():
    assert util.is_date('2021-01-01')
    assert util.is_date('2021-01-01 07:00')
    assert not util.is_date(1)
    assert not util.is_date(1.1)
    assert not util.is_date('ac')


def test_is_number():
    assert util.is_number(1)
    assert util.is_number(1.1)
    assert not util.is_number('ac')


def test_is_integer():
    assert util.is_integer(1)
    assert not util.is_integer(1.1)
    assert not util.is_integer('ac')


def test_is_empty():
    assert util.is_empty(None)
    assert util.is_empty('')
    assert util.is_empty('   ')
    assert not util.is_empty(0)
    assert not util.is_empty(1.1)
    assert not util.is_empty('ac')
    assert not util.is_empty(' ac  ')


def test_normalize():
    default_value = 'default'

    assert util.normalize(None, default_value) == default_value
    assert util.normalize('', default_value) == ''
    assert util.normalize(0, default_value) == 0
    assert util.normalize('ac') == 'ac'


def test_normalize_int():
    assert util.normalize_int(None) == 0
    assert util.normalize_int(1.1) == 1
    with pytest.raises(Exception):
        util.normalize_int('ac')


def test_convert_string_to_json():
    assert util.convert_string_to_json(0) == 0
    assert util.convert_string_to_json('ac') == 'ac'
    assert util.convert_string_to_json('{"key": "value"}') == {'key': 'value'}
    assert util.convert_string_to_json('{"key": [{"key2": "value"}]}') == {'key': [{'key2': 'value'}]}


def test_convert_json_to_string():
    assert util.convert_json_to_string(0) == '0'
    assert util.convert_json_to_string('0') == '"0"'
    assert util.convert_json_to_string('ac') == '"ac"'
    assert util.convert_json_to_string({'key': 'value'}) == '{"key": "value"}'
    assert util.convert_json_to_string({'key': [{'key2': 'value'}]}) == '{"key": [{"key2": "value"}]}'
