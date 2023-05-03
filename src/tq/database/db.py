import abc
from contextlib import contextmanager
from dataclasses import dataclass
from io import IOBase
from typing import Any, Iterable, Iterator, List, Optional, Type, Union
from uuid import UUID

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
    def store(self, filename: str, data: Any) -> str:
        pass

    @abc.abstractmethod
    def load(self, filename: str) -> bytes:
        pass

    @abc.abstractmethod
    def delete(self, filename: str) -> bool:
        pass

    @abc.abstractmethod
    def load_by_id(self, file_id: str) -> bytes:
        pass

    @abc.abstractmethod
    def delete_by_id(self, file_id: str) -> bool:
        pass

    @abc.abstractmethod
    def iterate_filenames(self) -> Iterable[str]:
        pass

    @abc.abstractmethod
    def find_file_id(self, filename: str) -> str:
        pass

    @abc.abstractmethod
    def open(self, filename: str, mode: str = "rb") -> IOBase:
        pass
