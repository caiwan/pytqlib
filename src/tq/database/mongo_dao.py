from contextlib import ExitStack, contextmanager
from copy import deepcopy
from functools import wraps
from typing import Callable, Iterator, List, Optional, Type, Union
from uuid import UUID

import bson
from marshmallow import Schema
from pymongo import MongoClient, client_session, collection, errors

from tq.database.db import AbstractDao, BaseEntity
from tq.database.utils import from_json, to_json


class MongoDaoContext:
    def __init__(self, client: MongoClient, key_prefix: str):
        self._client = client
        self._database = self._client.get_default_database()
        self._collection = self._database.get_collection(key_prefix)

        self._key_prefix = key_prefix

    def __enter__(self):
        self._session = self.client.start_session()
        self._session.start_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.end_session()
        pass

    @property
    def session(self) -> client_session.ClientSession:
        return self._session

    @property
    def client(self) -> MongoClient:
        return self._client

    @property
    def collection(self) -> collection.Collection:
        return self._collection

    def create_sub_context(self, key_prefix: str) -> "MongoDaoContext":
        return MongoDaoContext(self._connection_pool_manager, key_prefix)

    def create_or_update(self, obj: BaseEntity) -> UUID:
        data = obj.to_dict()
        result = self.collection.update_one(
            {"_id": bson.Binary.from_uuid(obj.id)},
            {"$set": from_json(to_json(data))},
            upsert=True,
        )
        if result.upserted_id:
            obj.id = bson.Binary.as_uuid(result.upserted_id)

        return obj.id

    def get_entity(self, id: Optional[Union[UUID, str]]) -> BaseEntity:
        result = self.collection.find_one({"_id": bson.Binary.from_uuid(id)})
        if result:
            result["id"] = bson.Binary.as_uuid(result["_id"])
            del result["_id"]
        return result

    def iterate_entities(self) -> Iterator[BaseEntity]:
        for item in self.collection.find({}):
            if item:
                item["id"] = bson.Binary.as_uuid(item["_id"])
                del item["_id"]
                yield item

    def iterate_all_keys(self) -> Iterator[UUID]:
        for item in self.collection.find({}, {"_id": 1}):
            if item:
                yield bson.Binary.as_uuid(item["_id"])

    def delete_entity(self, id: Optional[Union[UUID, str]]):
        self.collection.delete_one({"_id": bson.Binary.from_uuid(id)})


def transactional(fn: Callable) -> Callable:
    @wraps(fn)
    def tansaction_wrapper(*args, **kwargs):
        obj_self = args[0]
        key_prefix = obj_self._key_prefix
        ctx: MongoDaoContext = kwargs.get("ctx")
        client = obj_self._client

        if isinstance(ctx, MongoDaoContext):
            ctx = ctx.create_sub_context(obj_self._key_prefix)
            # TODO add params from ctx
            return fn(obj_self, *args[1:], ctx=ctx, **kwargs)
        else:
            # TODO: set retry attempts
            for attempt in range(3):
                try:
                    ctx = MongoDaoContext(client, key_prefix)
                    with ctx:
                        # TODO add params from ctx
                        return fn(obj_self, *args[1:], ctx=ctx, **kwargs)

                except errors.PyMongoError as e:
                    if attempt == 2:
                        raise e

    return tansaction_wrapper


class BaseMongoDao(AbstractDao):
    def __init__(
        self,
        client: MongoClient,
        schema: Type[Schema],
        key_prefix: str,
    ):
        super().__init__(schema, key_prefix)
        self._client = client

    @transactional
    def create_or_update(self, obj: BaseEntity, ctx: MongoDaoContext) -> UUID:
        return ctx.create_or_update(obj)

    @transactional
    def get_entity(
        self, id: Optional[Union[UUID, str]], ctx: MongoDaoContext
    ) -> BaseEntity:
        result = ctx.get_entity(id)
        return self.schema.from_dict(result) if result else None

    @transactional
    def get_all(self, ctx: MongoDaoContext) -> List[BaseEntity]:
        return [self.schema.from_dict(item) for item in ctx.iterate_entities()]

    @transactional
    def iterate_all(self, ctx: MongoDaoContext) -> Iterator[BaseEntity]:
        for item in ctx.iterate_entities():
            yield self.schema.from_dict(item)

    @transactional
    def iterate_all_keys(self, ctx: MongoDaoContext) -> Iterator[BaseEntity]:
        for key in ctx.iterate_all_keys():
            yield key

    @transactional
    def delete(self, id: Optional[Union[UUID, str]], ctx: MongoDaoContext):
        ctx.delete_entity(id)
