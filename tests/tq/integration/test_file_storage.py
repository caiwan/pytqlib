from uuid import uuid4

import pytest
from gridfs.errors import NoFile

from tq.database.gridfs_dao import GridFsDao
from tq.database.redis_file_cache import RedisFileFsDao


@pytest.fixture(scope="function")
def test_data(generate_random_data) -> bytes:
    # return b"This is some test data."
    return generate_random_data(256)


@pytest.fixture(scope="function")
def test_file_name() -> str:
    return f"test_file_{str(uuid4())}.txt"


@pytest.fixture
def gridfs_dao(mongodb_client):
    yield GridFsDao(mongodb_client)


@pytest.fixture
def redis_dao(redis_db):
    yield RedisFileFsDao(redis_db)


def do_store_and_load(dao, test_data, test_file_name):
    file_id = dao.store(test_file_name, test_data)
    assert file_id
    assert dao.find_file_id(test_file_name)
    loaded_data = dao.load(test_file_name)
    assert loaded_data == test_data


def do_delete(dao, test_data, test_file_name):
    file_id = dao.store(test_file_name, test_data)
    assert file_id
    assert dao.load_by_id(file_id) == test_data

    assert dao.delete(test_file_name)

    with pytest.raises(NoFile):
        dao.load(test_file_name)

    assert not dao.delete(test_file_name)


def do_load_nonexistent_file(dao):
    assert dao.find_file_id("Nonexistent_file") is None


def do_open_read(dao, test_data, test_file_name):
    dao.store(test_file_name, test_data)
    with dao.open(test_file_name, "rb") as f:
        assert f.read() == test_data


def do_open_write(dao, test_file_name):
    test_data = b"test data"

    with dao.open(test_file_name, "w") as f:
        f.write(test_data)
    assert dao.load(test_file_name) == test_data


def do_open_append_text(dao, test_data, test_file_name):
    dao.store(test_file_name, test_data)
    with dao.open(test_file_name, "a") as f:
        f.write(test_data)
    assert dao.load(test_file_name) == test_data + test_data


@pytest.mark.mongo
def test_mongodb_store_and_load(gridfs_dao, test_data, test_file_name):
    do_store_and_load(gridfs_dao, test_data, test_file_name)


@pytest.mark.mongo
def test_mongodb_delete(gridfs_dao, test_data, test_file_name):
    do_delete(gridfs_dao, test_data, test_file_name)


@pytest.mark.mongo
def test_mongodb_load_nonexistent_file(gridfs_dao):
    do_load_nonexistent_file(gridfs_dao)


@pytest.mark.mongo
def test_mongodb_open_read(gridfs_dao, test_data, test_file_name):
    do_open_read(gridfs_dao, test_data, test_file_name)


@pytest.mark.mongo
def test_mongodb_open_write(gridfs_dao, test_file_name):
    do_open_write(gridfs_dao, test_file_name)


@pytest.mark.mongo
@pytest.mark.skip("Not implemented")
def test_mongodb_open_append_text(gridfs_dao, test_data, test_file_name):
    do_open_append_text(gridfs_dao, test_data, test_file_name)


###


@pytest.mark.redis
def test_redis_store_and_load(redis_dao, test_data, test_file_name):
    do_store_and_load(redis_dao, test_data, test_file_name)


@pytest.mark.redis
def test_redis_delete(redis_dao, test_data, test_file_name):
    do_delete(redis_dao, test_data, test_file_name)


@pytest.mark.redis
def test_redis_load_nonexistent_file(redis_dao):
    do_load_nonexistent_file(redis_dao)


@pytest.mark.redis
def test_redis_open_read(dao_fredis_daoixture, test_data, test_file_name):
    do_open_read(redis_dao, test_data, test_file_name)


@pytest.mark.redis
def test_redis_open_write(redis_dao, test_file_name):
    do_open_write(redis_dao, test_file_name)


@pytest.mark.redis
@pytest.mark.skip("Not implemented")
def test_redis_open_append_text(redis_dao, test_data, test_file_name):
    do_open_append_text(redis_dao, test_data, test_file_name)
