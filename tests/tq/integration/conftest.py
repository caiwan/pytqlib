import logging
import uuid

import pytest
import redis
from pymongo import MongoClient

LOGGER = logging.getLogger(__name__)

DB_PORT = 6379
DB_HOST = "localhost"


MONGO_HOST = "localhost:27017"
MONGO_CONNECTION_STRING = "mongodb://root:toor@" + MONGO_HOST
PLACEHOLDER = "_placeholder"


@pytest.fixture(scope="function")
def redis_db() -> redis.ConnectionPool:
    return redis.ConnectionPool(host=DB_HOST, port=DB_PORT, db=0)


@pytest.fixture(scope="function")
def redis_db_connection(redis_db) -> redis.Redis:
    db = redis.Redis(connection_pool=redis_db)
    yield db
    db.flushall()


@pytest.fixture(scope="session")
def mongo_db():
    test_user = "test_user"
    test_password = "test_password"
    test_db = "test_db"

    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client.admin
    if not bool(
        db.system.users.find_one(
            {
                "user": test_user,
            }
        )
    ):
        db.command(
            "createUser",
            test_user,
            pwd=test_password,
            roles=[{"role": "readWrite", "db": test_db}],
        )

    if PLACEHOLDER not in list(client[test_db].list_collection_names()):
        client[test_db].create_collection(PLACEHOLDER)

    client.close()

    yield (test_user, test_password, test_db)

    client = MongoClient(MONGO_CONNECTION_STRING)
    client.drop_database(test_db)
    client.admin.command("dropUser", test_user)
    client.close()


@pytest.fixture(scope="function")
def mongodb_client(mongo_db) -> MongoClient:
    test_user, test_password, test_db = mongo_db
    client = MongoClient(
        f"mongodb://{test_user}:{test_password}@{MONGO_HOST}/{test_db}?authSource=admin"
    )

    client[test_db].create_collection(str(uuid.uuid4()))

    yield client

    db = client.get_database(test_db)

    for collection_name in db.list_collection_names():
        if collection_name not in [PLACEHOLDER]:
            collection = db.get_collection(collection_name)
            collection.drop()

    client.close()
