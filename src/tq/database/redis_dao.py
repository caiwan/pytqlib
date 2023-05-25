from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Type, Union
from uuid import UUID, uuid4

import redis
from marshmallow import Schema

from tq.database.db import AbstractDao, BaseContext, BaseEntity, transactional
from tq.database.utils import from_json, to_json

# https://github.com/redis/redis-py
# https://redis.io/commands

DEFAULT_BUCKET_SIZE = 10


# TODO: Unfuck names [type] _ [what_operation] like hash_get_keys() or the other way aorund, make it uniform at least


# TODO: Add db maintainer - aka save after X amount of db commits
@dataclass
class RedisDaoContext(BaseContext):
    # TODO: Properties + use proper setters
    shard_hint: Optional[str] = None
    watches: Optional[Set[str]] = None
    value_from_callable: bool = True
    watch_delay: Optional[float] = None

    def __init__(self, db: redis.Redis, name_prefix: str):
        self._db = db
        self._name_prefix: redis.ConnectionPool = name_prefix

    @contextmanager
    def create_sub_context(self, name_prefix):
        # TODO: When creating a new sub-ctx make it able to set certain flags for the transaction in the root context itself
        yield RedisDaoContext(self._db, name_prefix)

    def table(self, name_prefix):
        return RedisDaoContext(self._db, name_prefix)

    @property
    def db(self):
        return self._db

    @property
    def wildcard(self) -> str:
        return f"{self._name_prefix}:*"

    def _name(self, id: Optional[Union[UUID, str]]) -> str:
        return f"{self._name_prefix}:{str(id)}" if id else self._name_prefix

    def is_exists(self, id: Optional[Union[UUID, str]]) -> bool:
        return self._db.exists(self._name(id))

    def set(self, id: Optional[Union[UUID, str]], data: bytes) -> UUID:
        id = id if id else uuid4()
        name = self._name(id)
        self._db.set(name, data)
        return id

    def create_or_update(
        self, obj: Dict, id: Optional[Union[UUID, str]] = None
    ) -> UUID:
        return self.set(
            id if id else uuid4(),
            to_json(obj),
        )

    def get(self, id: Optional[Union[UUID, str]]) -> Optional[bytes]:
        name = self._name(id)
        if not self._db.exists(name):
            return None
        return self._db.get(name)

    def get_entity(self, id: Optional[Union[UUID, str]]) -> Optional[Dict]:
        item = self.get(id)
        return from_json(item)

    def iterate_all_keys(self) -> Iterator[UUID]:
        for db_key in self._db.scan_iter(self.wildcard):
            name = db_key.decode("utf-8").split(":")[-1]
            yield UUID(name)

    def iterate_all_entities(self) -> Iterator[Dict]:
        for db_key in self._db.scan_iter(self.wildcard):
            item = self._db.get(db_key)
            yield from_json(item)

    def delete(self, id: Optional[Union[UUID, str]]):
        self._db.delete(self._name(id))

    def list_has(self, id: Optional[Union[UUID, str]], data: Union[str, bytes]) -> bool:
        return len(self.list_find_all(id, data)) != 0

    def list_find_all(
        self, id: Optional[Union[UUID, str]], data: Union[str, bytes]
    ) -> List[int]:
        name = self._name(id)
        return self._db.lpos(name, data, count=0)

    def list_set(
        self, id: Optional[Union[UUID, str]], index: int, data: Union[str, bytes]
    ) -> bool:
        name = self._name(id)
        return self._db.lset(name, index, data)

    def list_push(self, id: Optional[Union[UUID, str]], data: Union[str, bytes]) -> int:
        name = self._name(id)
        return self._db.lpush(name, data)

    def list_push_entity(self, id: Optional[Union[UUID, str]], obj: Dict) -> int:
        return self.list_push(id, to_json(obj))

    def list_pop(self, id: Optional[Union[UUID, str]]) -> Optional[bytes]:
        name = self._name(id)
        return self._db.lpop(name)

    def list_pop_entity(self, id: Optional[Union[UUID, str]]) -> Optional[Dict]:
        return from_json(self.list_pop(id))

    def get_list_length(self, id: Optional[Union[UUID, str]]) -> int:
        name = self._name(id)
        if not self._db.exists(name):
            return 0

        return self._db.llen(name)

    def iter_all_from_list(
        self,
        id: Optional[Union[UUID, str]],
        fetch_bucket_size: int = DEFAULT_BUCKET_SIZE,
    ) -> Iterator[Any]:
        # TODO: The list has to be inverted
        fetch_bucket_size = (
            fetch_bucket_size if fetch_bucket_size > 0 else DEFAULT_BUCKET_SIZE
        )
        name = self._name(id)
        count = self.get_list_length(id)
        if count > 0:
            cursor = 0
            while cursor < count:
                delta = min(fetch_bucket_size, count - cursor)
                items = self._db.lrange(name, -cursor, -(cursor + delta))
                for item in items:
                    yield item
                cursor += delta

    def iter_all_entities_from_list(
        self,
        id: Optional[Union[UUID, str]],
        fetch_bucket_size: int = DEFAULT_BUCKET_SIZE,
    ) -> Iterator[Any]:
        for item in self.iter_all_from_list(id, fetch_bucket_size=fetch_bucket_size):
            yield from_json(item)

    def remove_from_list(self, id: Optional[Union[UUID, str]], obj: Any):
        name = self._name(id)
        return self._db.lrem(name, 1, to_json(obj))

    def remove_from_list_by_id(self, id: Optional[Union[UUID, str]], index: int):
        placeholder = str(uuid4())
        name = self._name(id)
        self._db.lset(name, index, placeholder)
        return self._remove_from_list(name, placeholder)

    def bulk_remove_from_list_by_id(
        self, id: Optional[Union[UUID, str]], indices: List[int]
    ):
        placeholders: List[str] = []
        name = self._name(id)
        for index in indices:
            random_uuid = str(uuid4())
            self._db.lset(name, index, random_uuid)
            placeholders.append(random_uuid)
        count = 0
        for placeholder in placeholders:
            count += self._remove_from_list(name, placeholder)
        return count

    def get_hash(self, id: Optional[Union[UUID, str]], key: str) -> Optional[bytes]:
        name = self._name(id)
        return self._db.hget(name, key)

    def set_hash(self, id: Optional[Union[UUID, str]], key: str, value: bytes) -> int:
        name = self._name(id)
        return self._db.hset(name, key, value)

    def delete_hash(self, id: Optional[Union[UUID, str]], key: str) -> int:
        name = self._name(id)
        return self._db.hdel(name, key)

    def has_hash_key(self, id: Optional[Union[UUID, str]], key: str) -> bool:
        return self._db.hexists(self._name(id), key)

    def get_hash_entity(
        self, id: Optional[Union[UUID, str]], key: str
    ) -> Optional[Any]:
        return from_json(self.get_hash(id, key))

    def set_hash_entity(
        self, id: Optional[Union[UUID, str]], key: str, value: Any
    ) -> int:
        return self.set_hash(id, key, to_json(value))

    def iterate_hash_keys(self, id: Optional[Union[UUID, str]]) -> Iterator[str]:
        for key in self._db.hkeys(self._name(id)):
            yield key.decode()

    def cleanup(self) -> int:
        counter = 0
        for name in self._db.scan_iter(self._wildcard):
            counter += self._db.delete(name)
        return counter

    def add_to_set(self, id: Optional[Union[UUID, str]], obj: Any):
        name = self._name(id)
        self._db.sadd(name, to_json(obj))

    def iterate_all_entities_from_set(
        self, id: Optional[Union[UUID, str]]
    ) -> Iterator[Any]:
        name = self._name(id)
        for item in self._db.smembers(name):
            yield from_json(item)

    def get_set_length(self, id: Optional[Union[UUID, str]]):
        name = self._name(id)
        return self._db.scard(name) if self._db.exists(name) else 0

    def set_expiration_time(self, id: Optional[Union[UUID, str]], seconds: int):
        name = self._name(id)
        self._db.expire(name, seconds)

    def trigger_db_cleanup(self):
        try:
            self._db.bgsave()
            self._db.bgrewriteaof()  # AOF Should be disabled anyways
        except Exception:
            # Ignore if already in progress
            pass

    def _run_transaction(self, fn: Callable, is_subcontext: bool = False) -> Any:
        if is_subcontext:
            return fn()
        else:
            return self._db.transaction(
                fn,
                value_from_callable=self.value_from_callable,
                shard_hint=self.shard_hint,
                watches=self.watches,
                watch_delay=self.watch_delay,
            )


class BaseRedisDao(AbstractDao):
    def __init__(
        self,
        db_pool: redis.ConnectionPool,
        schema: Type[Schema],
        key_prefix: str = "",
    ):
        super().__init__(schema, key_prefix)
        self._db_pool: redis.ConnectionPool = db_pool

    @transactional
    def create_or_update(self, obj: BaseEntity, ctx: RedisDaoContext) -> UUID:
        return ctx.create_or_update(obj.to_dict(), obj.id)

    @transactional
    def get_entity(
        self, id: Optional[Union[UUID, str]], ctx: RedisDaoContext
    ) -> BaseEntity:
        data = ctx.get_entity(id)
        return self._schema.load(data) if data else None

    @transactional
    def get_all(self, ctx: RedisDaoContext) -> List[BaseEntity]:
        return list([entity for entity in self.iterate_all(ctx)])

    @transactional
    def iterate_all(self, ctx: RedisDaoContext) -> Iterator[BaseEntity]:
        for entity in ctx.iterate_all_entities():
            yield self._schema.load(entity) if entity else None

    @transactional
    def iterate_all_keys(self, ctx: RedisDaoContext) -> Iterator[BaseEntity]:
        for key in ctx.iterate_all_keys():
            yield key

    @transactional
    def delete(self, id: Optional[Union[UUID, str]], ctx: RedisDaoContext):
        return ctx.delete(id)

    def _create_context(self, ctx: Optional[BaseContext] = None) -> BaseContext:
        if ctx is not None:
            return ctx.create_sub_context(self._key_prefix)
        else:
            return RedisDaoContext(
                redis.Redis(connection_pool=self._db_pool), self._key_prefix
            )
