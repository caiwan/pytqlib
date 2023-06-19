from typing import Any, Callable, Iterator, List, Optional, Type, Union
from uuid import UUID, uuid4

import bson
from marshmallow import Schema
from pymongo import MongoClient, client_session, collection, errors

from tq.database.db import AbstractDao, BaseContext, BaseEntity, transactional
from tq.database.utils import from_json, to_json


class MongoDaoContext(BaseContext):
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
        obj.id = obj.id or uuid4()
        result = self.collection.update_one(
            {
                "_id": bson.Binary.from_uuid(
                    UUID(obj.id) if isinstance(obj.id, str) else obj.id
                )
            },
            {"$set": from_json(to_json(data))},
            upsert=True,
        )
        if result.upserted_id:
            obj.id = bson.Binary.as_uuid(result.upserted_id)

        return obj.id

    def get_entity(self, id: Optional[Union[UUID, str]]) -> BaseEntity:
        result = self.collection.find_one(
            {"_id": bson.Binary.from_uuid(UUID(id) if isinstance(id, str) else id)}
        )
        if result:
            result["id"] = bson.Binary.as_uuid(result["_id"])
            del result["_id"]
        return result

    def find_one_entity(self, query: dict) -> BaseEntity:
        result = self.collection.find_one(query)
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

    def _run_transaction(
        self, fn: Callable, is_subcontext: bool = False
    ) -> Optional[Any]:
        if is_subcontext:
            return fn()
        else:
            for attempt in range(3):
                try:
                    with self:
                        return fn()
                except errors.PyMongoError as e:
                    if attempt == 2:
                        raise e


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

    def _create_context(self, ctx: Optional[BaseContext] = None) -> BaseContext:
        if ctx:
            return ctx.create_sub_context(self.key_prefix)
        else:
            return MongoDaoContext(self._client, self.key_prefix)
