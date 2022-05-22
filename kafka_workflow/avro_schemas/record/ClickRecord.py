# -*- coding: utf-8 -*-

""" avro python class for file: ClickRecord """

import json
from typing import Union


class ClickRecord(object):

    schema = """
    {
        "type": "record",
        "name": "ClickRecord",
        "fields": [
            {
                "name": "session_id",
                "type": "string"
            },
            {
                "name": "browser",
                "type": [
                    "string",
                    "null"
                ]
            },
            {
                "name": "campaign",
                "type": [
                    "string",
                    "null"
                ]
            },
            {
                "name": "channel",
                "type": "string"
            },
            {
                "name": "referrer",
                "type": [
                    "string",
                    "null"
                ],
                "default": "None"
            },
            {
                "name": "ip",
                "type": [
                    "string",
                    "null"
                ]
            }
        ],
        "namespace": ""
    }
    """

    def __init__(self, obj: Union[str, dict, 'ClickRecord']=None) -> None:
        if isinstance(obj, str):
            obj = json.loads(obj)

        elif isinstance(obj, type(self)):
            obj = obj.__dict__

        elif not isinstance(obj, dict):
            raise TypeError(
                f"{type(obj)} is not in ('str', 'dict', 'ClickRecord')"
            )

        self.set_session_id(obj.get('session_id', None))

        self.set_browser(obj.get('browser', None))

        self.set_campaign(obj.get('campaign', None))

        self.set_channel(obj.get('channel', None))

        self.set_referrer(obj.get('referrer', 'None'))

        self.set_ip(obj.get('ip', None))

    def dict(self):
        return todict(self)

    def set_session_id(self, value: str) -> None:

        if isinstance(value, str):
            self.session_id = value
        else:
            raise TypeError("field 'session_id' should be type str")

    def get_session_id(self) -> str:

        return self.session_id

    def set_browser(self, value: Union[str, None]) -> None:
        if isinstance(value, str):
            self.browser = str(value)

        elif isinstance(value, type(None)):
            self.browser = None
        else:
            raise TypeError("field 'browser' should be in (str, None)")

    def get_browser(self) -> Union[str, None]:
        return self.browser

    def set_campaign(self, value: Union[str, None]) -> None:
        if isinstance(value, str):
            self.campaign = str(value)

        elif isinstance(value, type(None)):
            self.campaign = None
        else:
            raise TypeError("field 'campaign' should be in (str, None)")

    def get_campaign(self) -> Union[str, None]:
        return self.campaign

    def set_channel(self, value: str) -> None:

        if isinstance(value, str):
            self.channel = value
        else:
            raise TypeError("field 'channel' should be type str")

    def get_channel(self) -> str:

        return self.channel

    def set_referrer(self, value: Union[str, None]) -> None:
        if isinstance(value, str):
            self.referrer = str(value)

        elif isinstance(value, type(None)):
            self.referrer = None
        else:
            raise TypeError("field 'referrer' should be in (str, None)")

    def get_referrer(self) -> Union[str, None]:
        return self.referrer

    def set_ip(self, value: Union[str, None]) -> None:
        if isinstance(value, str):
            self.ip = str(value)

        elif isinstance(value, type(None)):
            self.ip = None
        else:
            raise TypeError("field 'ip' should be in (str, None)")

    def get_ip(self) -> Union[str, None]:
        return self.ip

    def serialize(self) -> bytes:
        return json.dumps(self, default=default_json_serialize).encode()




""" helper functions for avro serialization """

from enum import Enum, EnumMeta


def default_json_serialize(obj):
    """ Wrapper for serializing enum types """
    if isinstance(obj, Enum):
        return obj.name
    else:
        return obj.__dict__


def todict(obj, classkey=None):
    """ helper function to convert nested objects to dicts """
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = todict(v, classkey)
        return data
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "_ast"):
        return todict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [todict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, todict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj


class DefaultEnumMeta(EnumMeta):
    default = object()

    def __call__(cls, value=default, *args, **kwargs):
        if value is DefaultEnumMeta.default:
            # Assume the first enum is default
            return next(iter(cls))
        return super().__call__(value, *args, **kwargs)
