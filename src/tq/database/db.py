import abc
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from io import IOBase
from typing import Any, Callable, Iterable, Iterator, List, Optional, Type, Union
from uuid import UUID

from dataclasses_json import DataClassJsonMixin
from marshmallow import Schema


# TODO: id becomes _id in [mongo]
@dataclass
class BaseEntity(DataClassJsonMixin):
    id: Optional[UUID]


class BaseContext(abc.ABC):
    @abc.abstractmethod
    def _run_transaction(
        self, fn: Callable, is_subcontext: bool = False
    ) -> Optional[Any]:
        pass


def transactional(fn: Callable) -> Callable:
    @wraps(fn)
    def tansaction_wrapper(*args, **kwargs):
        obj_self = args[0]
        ctx: BaseContext = obj_self._create_context(kwargs.get("ctx"))

        if "ctx" in kwargs:
            del kwargs["ctx"]

        return ctx._run_transaction(
            lambda: fn(obj_self, *args[1:], ctx=ctx, **kwargs),
            is_subcontext=kwargs.get("ctx", None) is None,
        )

    return tansaction_wrapper


class AbstractDao(abc.ABC):
    def __init__(self, schema: Type[Schema], key_prefix: str = ""):
        self._key_prefix: str = key_prefix
        self._schema: Type[Schema] = schema

    @abc.abstractmethod
    def create_or_update(self, obj: BaseEntity, *args, **kwargs) -> UUID:
        pass

    @abc.abstractmethod
    def bulk_create_or_update(
        self, obj: Iterable[BaseEntity], *args, **kwargs
    ) -> List[UUID]:
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

    @property
    def key_prefix(self):
        return self._key_prefix

    @abc.abstractmethod
    def _create_context(self, ctx: Optional[BaseContext] = None) -> BaseContext:
        pass


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
