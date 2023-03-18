from dataclasses import dataclass
from typing import Optional, Union
from click import UUID

from dataclasses_json import DataClassJsonMixin


@dataclass
class BaseEntity(DataClassJsonMixin):
    id: Optional[Union[UUID, str]]
