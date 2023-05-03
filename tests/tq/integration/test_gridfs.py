from uuid import uuid4

import pytest
from gridfs.errors import NoFile

from tq.database.gridfs_dao import GridFsDao


@pytest.fixture(scope="function")
def test_data(generate_random_data) -> bytes:
    # return b"This is some test data."
    return generate_random_data(256)


@pytest.fixture(scope="function")
def test_file_name() -> str:
    return f"test_file_{str(uuid4())}.txt"


@pytest.fixture
def gridfs_dao(mongodb_client):
    dao = GridFsDao(mongodb_client)
    yield dao


@pytest.mark.mongo
def test_store_and_load(gridfs_dao, test_data, test_file_name):
    # test storing and loading a file
    file_id = gridfs_dao.store(test_file_name, test_data)
    assert file_id
    assert gridfs_dao.find_file_id(test_file_name)
    loaded_data = gridfs_dao.load(test_file_name)
    assert loaded_data == test_data


@pytest.mark.mongo
def test_delete(gridfs_dao, test_data, test_file_name):
    # test deleting a file

    file_id = gridfs_dao.store(test_file_name, test_data)
    assert file_id
    assert gridfs_dao.load_by_id(file_id) == test_data

    assert gridfs_dao.delete(test_file_name)

    with pytest.raises(NoFile):
        gridfs_dao.load(test_file_name)

    assert not gridfs_dao.delete(test_file_name)


@pytest.mark.mongo
def test_load_nonexistent_file(gridfs_dao):
    # test loading a nonexistent file
    assert gridfs_dao.find_file_id("Nonexistent_file") is None


@pytest.mark.mongo
def test_open_read(gridfs_dao, test_data, test_file_name):
    gridfs_dao.store(test_file_name, test_data)
    with gridfs_dao.open(test_file_name, "rb") as f:
        assert f.read() == test_data


@pytest.mark.mongo
def test_open_write(gridfs_dao, test_file_name):
    test_data = b"test data"

    with gridfs_dao.open(test_file_name, "w") as f:
        f.write(test_data)
    assert gridfs_dao.load(test_file_name) == test_data


@pytest.mark.skip("Not implemented")
def test_open_append_text(gridfs_dao, test_data, test_file_name):
    gridfs_dao.store(test_file_name, test_data)
    with gridfs_dao.open(test_file_name, "a") as f:
        f.write(test_data)
    assert gridfs_dao.load(test_file_name) == test_data + test_data
