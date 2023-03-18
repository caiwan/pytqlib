from typing import Optional
from uuid import UUID, uuid4

from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin

from tq.database import BaseEntity
from tq.database.redis_dao import BaseDao, transactional, DaoContext

import random


@dataclass
class MyData(DataClassJsonMixin):
    id: UUID = None
    integer: int = 42
    string: str = "test_str"


class MyDataDao(BaseDao):
    def __init__(self, db_pool):
        super().__init__(db_pool, MyData.schema(), key_prefix="my_data")

    @transactional
    def save_raw_data(self, ctx: DaoContext, id: UUID, data: bytes) -> UUID:
        return ctx.set(id, data)

    @transactional
    def load_raw_data(self, ctx: DaoContext, id: UUID) -> Optional[bytes]:
        return ctx.get(id)

    @transactional
    def push(self, ctx: DaoContext, id: UUID, obj: MyData):
        ctx.list_push_entity(id, obj.to_dict())

    @transactional
    def pop(self, ctx: DaoContext, id: UUID) -> MyData:
        return self._schema.load(ctx.list_pop_entity(id))

    @transactional
    def size(self, ctx: DaoContext, id: UUID) -> int:
        return ctx.get_list_length(id)


def test_create_read_delete(db_pool):
    dao = MyDataDao(db_pool)
    data = MyData()
    obj_id = dao.create_or_update(data)
    assert obj_id is not None

    read = dao.get_entity(obj_id)
    assert read is not None
    assert read.integer == data.integer
    assert read.string == data.string

    dao.delete(obj_id)

    read = dao.get_entity(obj_id)
    assert read is None


def test_create_read_delete_raw_data(db_pool):
    dao = MyDataDao(db_pool)
    data = bytes(bytearray([random.randint(0, 255) for _ in range(1024**2)]))

    obj_id = dao.save_raw_data(None, data)
    assert obj_id is not None

    read = dao.load_raw_data(obj_id)
    assert read is not None
    assert len(read) == len(data)
    assert all(a == b for a, b in zip(read, data))

    dao.delete(obj_id)

    read = dao.load_raw_data(obj_id)
    assert read is None


def test_stack_push_pop(db_pool):
    dao = MyDataDao(db_pool)
    test_objects = [MyData(integer=random.randint(0, 65536)) for _ in range(256)]

    list_id = uuid4()

    for obj in test_objects:
        dao.push(list_id, obj)

    assert dao.size(list_id) == 256

    def fetch():
        item = dao.pop(list_id)
        if item:
            yield item

    for obj in fetch():
        assert test_objects.pop() == obj


# TODO: Test hashes
