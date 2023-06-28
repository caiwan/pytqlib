import uuid
from dataclasses import dataclass

import pytest

from tq.database.db import BaseEntity
from tq.database.mongo_dao import BaseMongoDao, MongoDaoContext, transactional


@dataclass
class MyEntity(BaseEntity):
    name: str = ""


class MyDao(BaseMongoDao):
    def __init__(self, connection):
        super().__init__(connection, MyEntity.schema(), "test")


@pytest.mark.mongo
def test_create_or_update(mongodb_client):
    dao = MyDao(mongodb_client)
    entities = [
        MyEntity(
            id=None,
            name="Alice",
        ),
        MyEntity(
            id=uuid.uuid4(),
            name="Bob",
        ),
        MyEntity(
            id=uuid.uuid4(),
            name="Charlie",
        ),
    ]
    ids = [dao.create_or_update(entity) for entity in entities]
    assert all(isinstance(id, uuid.UUID) for id in ids)


@pytest.mark.mongo
def test_get_entity(mongodb_client):
    dao = MyDao(mongodb_client)
    entity = MyEntity(
        id=None,
        name="Alice",
    )
    id = dao.create_or_update(entity)
    retrieved_entity = dao.get_entity(id)
    assert retrieved_entity is not None
    assert retrieved_entity.id == id
    assert retrieved_entity.name == "Alice"


@pytest.mark.mongo
def test_get_all(mongodb_client):
    dao = MyDao(mongodb_client)
    entity1 = MyEntity(
        id=None,
        name="Alice",
    )
    entity2 = MyEntity(
        id=None,
        name="Bob",
    )
    dao.create_or_update(entity1)
    dao.create_or_update(entity2)
    entities = dao.get_all()
    assert len(entities) == 2


@pytest.mark.mongo
def test_iterate_all(mongodb_client):
    dao = MyDao(mongodb_client)
    entity1 = MyEntity(
        id=None,
        name="Alice",
    )
    entity2 = MyEntity(
        id=uuid.uuid4(),
        name="Bob",
    )
    dao.create_or_update(entity1)
    dao.create_or_update(entity2)
    names = [entity.name for entity in dao.iterate_all()]
    assert set(names) == set(["Alice", "Bob"])


@pytest.mark.mongo
def test_iterate_all_keys(mongodb_client):
    dao = MyDao(mongodb_client)
    entity1 = MyEntity(
        id=None,
        name="Alice",
    )
    entity2 = MyEntity(
        id=None,
        name="Bob",
    )
    id1 = dao.create_or_update(entity1)
    id2 = dao.create_or_update(entity2)
    ids = [id for id in dao.iterate_all_keys()]
    assert set(ids) == set([id1, id2])


@pytest.mark.mongo
def test_delete(mongodb_client):
    dao = MyDao(mongodb_client)
    entity = MyEntity(
        id=None,
        name="Alice",
    )
    id = dao.create_or_update(entity)
    dao.delete(id)
    assert dao.get_entity(id) is None
