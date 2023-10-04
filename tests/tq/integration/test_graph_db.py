from dataclasses import dataclass
import itertools
import random
import more_itertools
import pytest
from uuid import UUID
from tq.database.graph_dao import Neo4jDao, graph_node
from tq.database.db import BaseEntity


@graph_node
@dataclass
class DummyNode(BaseEntity):
    name: str
    age: int
    weight: float


@pytest.fixture(scope="function")
def dummy_node_dao(graph_db):
    return Neo4jDao(DummyNode, graph_db, key_prefix="test_graph")


@pytest.mark.neo4j
def test_create_or_update(dummy_node_dao):
    entity = DummyNode(
        id=None,
        name="Alice",
        age=20,
        weight=50.0,
    )
    # Create
    result_id = dummy_node_dao.create_or_update(entity)
    assert isinstance(result_id, UUID)

    # Read
    retrieved_entity = dummy_node_dao.get_entity(result_id)
    assert retrieved_entity is not None
    assert retrieved_entity.to_dict() == entity.to_dict()

    # Update
    entity.name = "Bob"
    entity.age = 30
    entity.weight = 60.0
    dummy_node_dao.create_or_update(entity)

    # Read
    retrieved_entity = dummy_node_dao.get_entity(result_id)
    assert retrieved_entity is not None
    assert retrieved_entity.to_dict() == entity.to_dict()

    # Delete
    dummy_node_dao.delete(result_id)
    retrieved_entity = dummy_node_dao.get_entity(result_id)
    assert retrieved_entity is None


@pytest.mark.neo4j
def test_bulk_create_or_update(dummy_node_dao):
    entities = [
        DummyNode(
            id=None,
            name="Person {}".format(_),
            age=random.randint(0, 100),
            weight=random.uniform(0, 100),
        )
        for _ in range(10)
    ]
    results = dummy_node_dao.bulk_create_or_update(entities)
    assert all(isinstance(res, UUID) for res in results)

    for key in dummy_node_dao.iterate_all_keys():
        assert key in results

    for retrieved_entity in dummy_node_dao.iterate_all():
        assert retrieved_entity is not None

        # This does not finds the entity because the retrieved entity is not the same for some reason
        entity = more_itertools.first_true(
            entities, pred=lambda x: str(x.id) == str(retrieved_entity.id)
        )

        assert retrieved_entity.to_dict() == entity.to_dict()
