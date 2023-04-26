from uuid import uuid4

import pytest
from gridfs.errors import NoFile

from tq.database.gridfs_dao import GridFsDao


@pytest.fixture(scope="function")
def test_data(generate_random_data):
    # return b"This is some test data."
    return generate_random_data(256)


@pytest.fixture(scope="function")
def test_file_name():
    return f"test_file_{str(uuid4())}.txt"


@pytest.fixture
def gridfs_dao(mongodb_client):
    # set up test database and return GridFsDao instance
    dao = GridFsDao(mongodb_client)
    yield dao
    # clean up test database
    # mongodb_client.drop_database(dao._db)


@pytest.mark.mongo
def test_store_and_load(gridfs_dao, test_data, test_file_name):
    # test storing and loading a file
    file_id = gridfs_dao.store(test_data, test_file_name)
    loaded_data = gridfs_dao.load(file_id)
    assert loaded_data == test_data


@pytest.mark.mongo
def test_delete(gridfs_dao, test_data, test_file_name):
    # test deleting a file

    file_id = gridfs_dao.store(test_data, test_file_name)
    assert gridfs_dao.load(file_id) == test_data
    gridfs_dao.delete(file_id)
    with pytest.raises(NoFile):
        gridfs_dao.load(file_id)


@pytest.mark.mongo
def test_load_nonexistent_file(gridfs_dao):
    # test loading a nonexistent file
    with pytest.raises(NoFile):
        gridfs_dao.load("nonexistent_file_id")


def test_open_read(gridfs_dao, test_data, test_file_name):
    gridfs_dao.store(test_file_name, test_data)
    with gridfs_dao.open(test_file_name, "rb") as f:
        assert f.read() == test_data


def test_open_read_text(gridfs_dao, test_data, test_file_name):
    test_data_str = test_data.decode()
    gridfs_dao.store(test_file_name, test_data)
    with gridfs_dao.open(test_file_name, "r") as f:
        assert f.read() == test_data_str


def test_open_write(gridfs_dao, test_data, test_file_name):
    with gridfs_dao.open(test_file_name, "wb") as f:
        f.write(test_data)
    assert gridfs_dao.load(test_file_name) == test_data


def test_open_write_text(gridfs_dao, test_data, test_file_name):
    test_data_str = test_data.decode()
    with gridfs_dao.open(test_file_name, "w") as f:
        f.write(test_data_str)
    assert gridfs_dao.load(test_file_name) == test_data


def test_open_append(gridfs_dao, test_data, test_file_name):
    gridfs_dao.store(test_file_name, test_data)
    with gridfs_dao.open(test_file_name, "ab") as f:
        f.write(test_data)
    assert gridfs_dao.load(test_file_name) == test_data + test_data


def test_open_append_text(gridfs_dao, test_data, test_file_name):
    test_data_str = test_data.decode()
    gridfs_dao.store(test_file_name, test_data)
    with gridfs_dao.open(test_file_name, "a") as f:
        f.write(test_data_str)
    assert gridfs_dao.load(test_file_name) == test_data + test_data
