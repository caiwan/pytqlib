import logging
import uuid

import pytest

import redis
import pymongo

# import py2neo.client
from py2neo import Graph

LOGGER = logging.getLogger(__name__)

DB_PORT = 6379
DB_HOST = "localhost"

MONGO_HOST = "localhost:27017"
MONGO_CONNECTION_STRING = "mongodb://root:toor@" + MONGO_HOST
PLACEHOLDER = "_placeholder"

NEO4J_HOST = "localhost"
NEO4J_PORT = 7687
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"


@pytest.fixture(scope="function", autouse=True)
def setup_logs(caplog):
    caplog.set_level(logging.WARNING, logger="py2neo")


@pytest.fixture(scope="session")
def cache_db_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(host=DB_HOST, port=DB_PORT, db=0)


@pytest.fixture(scope="function")
def cache_db_connection(cache_db_pool) -> redis.Redis:
    db = redis.Redis(connection_pool=cache_db_pool)
    yield db
    db.flushall()


@pytest.fixture(scope="session")
def mongo_db():
    test_user = "test_user"
    test_password = "test_password"
    test_db = "test_db"

    client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
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

    client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    client.drop_database(test_db)
    client.admin.command("dropUser", test_user)
    client.close()


@pytest.fixture(scope="function")
def mongodb_client(mongo_db) -> pymongo.MongoClient:
    test_user, test_password, test_db = mongo_db
    client = pymongo.MongoClient(
        f"mongodb://{test_user}:{test_password}@{MONGO_HOST}/{test_db}?authSource=admin"
    )

    yield client

    db = client.get_database(test_db)

    for collection_name in db.list_collection_names():
        if collection_name not in [PLACEHOLDER]:
            collection = db.get_collection(collection_name)
            collection.drop()

    client.close()


@pytest.fixture(scope="function")
def graph_db():
    # # Pool: https://py2neo.org/2021.0/client/index.html

    # pool = py2neo.client.ConnectionPool(
    #     {
    #         "host": NEO4J_HOST,
    #         "port": NEO4J_PORT,
    #         "user": NEO4J_USER,
    #         "password": NEO4J_PASSWORD,
    #     },
    #     user_agent="tq.tests/1.0",
    #     max_size=10,
    #     max_age=3600,
    # )

    # connection = pool.acquire()

    # yield pool

    # pool.release(connection)
    # pool.close()

    graph = Graph(
        uri=f"bolt://{NEO4J_HOST}:{NEO4J_PORT}",
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )

    yield graph

    graph.delete_all()
