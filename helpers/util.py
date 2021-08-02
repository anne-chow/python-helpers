import base64
import hashlib
from dateutil.parser import parse
from datetime import date, datetime
import simplejson as json
from . import log


# validators
def is_date(string):
    try:
        parse(string)
        return True
    except (ValueError, OverflowError, TypeError):
        return False


def is_number(string):
    try:
        float(string)
        return True
    except (ValueError, TypeError):
        return False


def is_integer(string):
    try:
        return int(string) - float(string) == 0
    except (ValueError, TypeError):
        return False


def is_empty(value):
    return True if value is None or len(str(value).strip()) == 0 else False


def normalize(value, default_value=None):
    return value if value is not None else default_value


def normalize_int(value):
    return int(normalize(value, default_value=0))


# converters
def convert_string_to_json(str1):
    try:
        return json.loads(str1, encoding='utf-8')
    except (ValueError, TypeError) as e:
        # not parseable json
        log.get_logger().warning("Warning: Cannot parse string, returning original value as is. Error={}. "
                                 "String='{}'".format(e, str1))

    return str1


def _json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return str(obj)
    raise TypeError("Type {} is not serializable" . format(type(obj)))


def convert_json_to_string(json_record):
    return json.dumps(json_record, ensure_ascii=False, default=_json_serial)


def get_md5sum(string_content):
    return base64.b64encode(hashlib.md5(string_content).digest()).decode('utf-8')


def get_md5sum_dict(dict_content):
    return hashlib.md5(json.dumps(dict_content, sort_keys=True, ensure_ascii=False,
                                  default=_json_serial).encode('utf-8')).hexdigest()
