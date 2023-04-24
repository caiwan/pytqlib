import pytest
from gridfs.errors import FileExists, NoFile

from tq.database.gridfs_dao import GridFsDao


@pytest.fixture
def gridfs_dao(mongodb_client):
    # set up test database and return GridFsDao instance
    dao = GridFsDao(mongodb_client)
    yield dao
    # clean up test database
    mongodb_client.drop_database(dao._db)


@pytest.mark.mongo
def test_store_and_load(gridfs_dao):
    # test storing and loading a file
    data = b"test data"
    filename = "test.txt"
    file_id = gridfs_dao.store(data, filename)
    loaded_data = gridfs_dao.load(file_id)
    assert loaded_data == data


@pytest.mark.mongo
def test_delete(gridfs_dao):
    # test deleting a file
    data = b"test data"
    filename = "test.txt"
    file_id = gridfs_dao.store(data, filename)
    assert gridfs_dao.load(file_id) == data
    gridfs_dao.delete(file_id)
    with pytest.raises(NoFile):
        gridfs_dao.load(file_id)


@pytest.mark.mongo
def test_load_nonexistent_file(gridfs_dao):
    # test loading a nonexistent file
    with pytest.raises(NoFile):
        gridfs_dao.load("nonexistent_file_id")
