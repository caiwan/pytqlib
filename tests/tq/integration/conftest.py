import logging
import uuid

import pytest
import redis
from pymongo import MongoClient

LOGGER = logging.getLogger(__name__)

DB_PORT = 6379
DB_HOST = "localhost"


MONGO_CONNECTION_STRING = "mongodb://root:toor@localhost:27017/test_{}?authSource=admin"


@pytest.fixture(scope="function")
def db_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(host=DB_HOST, port=DB_PORT, db=0)


@pytest.fixture(scope="function")
def db_connection(db_pool) -> redis.Redis:
    db = redis.Redis(connection_pool=db_pool)
    yield db
    db.flushall()


@pytest.fixture(scope="function")
def mongodb_client() -> MongoClient:
    random_id = str(uuid.uuid4())
    client = MongoClient(MONGO_CONNECTION_STRING.format(random_id))

    yield client

    # TODO: Unable to clean up after tests

    # for db_name in client.list_database_names():
    #     if db_name not in ("admin", "local"):
    #         database = client[db_name]
    #         for collection_name in database.list_collection_names():
    #             database.drop_collection(collection_name)

    # client.close()
