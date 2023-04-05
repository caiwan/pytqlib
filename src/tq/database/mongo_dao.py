from ast import List
from ctypes import Union
from functools import wraps
from typing import Callable, Iterator, Optional, Type
from uuid import UUID

import pymongo
from marshmallow import Schema

from tq.database.db import AbstractDao, BaseEntity


class MongoDaoContext:
    pass


# TODO - id is `_id` in mongo


def transactional(fn: Callable) -> Callable:
    @wraps(fn)
    def tansaction_wrapper(*args, **kwargs):
        obj_self = args[0]
        client: pymongo.MongoClient = obj_self._db_client

        # TODO: Write AN helper for this

        # with client.start_session() as session:
        #     with session.start_transaction():
        #         return fn(obj_self, *args[1:], session=session, **kwargs)

        ctx: MongoDaoContext = kwargs.get("ctx")
        # Allow nesting
        if isinstance(ctx, MongoDaoContext):
            ctx = ctx.create_sub_context(obj_self._key_prefix)
            # TODO add params from ctx
            return fn(obj_self, *args[1:], ctx=ctx, **kwargs)
        else:
            ctx = MongoDaoContext(client)
            with ctx:
                return fn(obj_self, *args[1:], ctx=ctx, **kwargs)
                # https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html
                # Use with_transacton

    return tansaction_wrapper


class BaseMongoDao(AbstractDao):
    def __init__(
        self,
        client: pymongo.MongoClient,
        schema: Type[Schema],
        key_prefix: str,
    ):
        super().__init__(schema, key_prefix)
        self._db_client = client

    def create_or_update(self, obj: BaseEntity, *args, **kwargs) -> UUID:
        pass

    def get_entity(self, id: Optional[Union[UUID, str]], *args, **kwargs) -> BaseEntity:
        pass

    def get_all(self, *args, **kwargs) -> List[BaseEntity]:
        pass

    def iterate_all(self, *args, **kwargs) -> Iterator[BaseEntity]:
        pass

    def iterate_all_keys(self, *args, **kwargs) -> Iterator[BaseEntity]:
        pass

    def delete(self, id: Optional[Union[UUID, str]], *args, **kwargs):
        pass

    # def __init__(
    #     self, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"
    # ):
    #     self.client = pymongo.MongoClient(mongo_uri)
    #     self.db = self.client[db_name]
    #     self.collection = self.db[collection_name]

    # def create(self, entity):
    #     entity_json = entity.to_json()
    #     self.collection.insert_one(entity_json)

    # def read(self, entity_id):
    #     entity_json = self.collection.find_one({"_id": entity_id})
    #     if entity_json:
    #         return Entity.from_json(entity_json)
    #     return None

    # def update(self, entity_id, entity):
    #     entity_json = entity.to_json()
    #     self.collection.update_one({"_id": entity_id}, {"$set": entity_json})

    # def delete(self, entity_id):
    #     self.collection.delete_one({"_id": entity_id})

    # def count(self):
    #     return self.collection.count_documents({})

    # def get_keys(self):
    #     return self.collection.distinct("_id")

    # def start_transaction(self):
    #     return self.client.start_session()

    # def with_transaction(self, callback):
    #     with self.client.start_session() as session:
    #         with session.start_transaction():
    #             callback(session)
