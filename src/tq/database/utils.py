import datetime
import json
from enum import Enum
from typing import Any, Optional
from uuid import UUID

import redis
from dataclasses_json import DataClassJsonMixin
from marshmallow import Schema


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)

        if isinstance(obj, datetime.datetime):
            return obj.timestamp()

        if issubclass(type(obj), Enum):
            return str(obj.value)

        if issubclass(type(obj), DataClassJsonMixin):
            return obj.to_dict()

        return json.JSONEncoder.default(self, obj)


def to_json(obj: Any) -> bytes:
    return json.dumps(obj, cls=CustomJSONEncoder).encode()


def from_json(_json: Optional[bytes]) -> Optional[Any]:
    return json.loads(_json.decode()) if _json else None


# TODO: Transactional
