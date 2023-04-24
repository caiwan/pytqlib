import abc
from dataclasses import dataclass
from typing import Iterator, List, Optional, Type, Union

from click import UUID
from dataclasses_json import DataClassJsonMixin
from marshmallow import Schema


@dataclass
class BaseEntity(DataClassJsonMixin):
    id: Optional[Union[UUID, str]]


class AbstractDao(abc.ABC):
    def __init__(self, schema: Type[Schema], key_prefix: str = ""):
        self._key_prefix: str = key_prefix
        self._schema: Type[Schema] = schema

    @abc.abstractmethod
    def create_or_update(self, obj: BaseEntity, *args, **kwargs) -> UUID:
        pass

    @abc.abstractmethod
    def get_entity(self, id: Optional[Union[UUID, str]], *args, **kwargs) -> BaseEntity:
        pass

    @abc.abstractmethod
    def get_all(self, *args, **kwargs) -> List[BaseEntity]:
        pass

    @abc.abstractmethod
    def iterate_all(self, *args, **kwargs) -> Iterator[BaseEntity]:
        pass

    @abc.abstractmethod
    def iterate_all_keys(self, *args, **kwargs) -> Iterator[BaseEntity]:
        pass

    @abc.abstractmethod
    def delete(self, id: Optional[Union[UUID, str]], *args, **kwargs):
        pass

    @property
    def schema(self):
        return self._schema


class AbstractFsDao(abc.ABC):
    @abc.abstractmethod
    def store(self, data, filename):
        pass

    @abc.abstractmethod
    def load(self, file_id):
        pass

    @abc.abstractmethod
    def delete(self, file_id):
        pass
