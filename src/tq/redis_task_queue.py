import base64
import logging
import pickle
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from tq.database import BaseEntity
from tq.database.redis_dao import (
    BaseEntity,
    BaseRedisDao,
    RedisDaoContext,
    transactional,
)
from tq.task_dispacher import BaseTaskQueue, Task

logger = logging.getLogger(__name__)


@dataclass
class TaskEntity(BaseEntity):
    payload: str = ""


class RedisTaskQueueDao(BaseRedisDao):
    def __init__(self, db_pool, task_queue_id=None):
        super().__init__(db_pool, TaskEntity.schema(), key_prefix="task_queue")
        self.task_queue_id = task_queue_id if task_queue_id else uuid.uuid4()

    def push(self, task: Task):
        payload = base64.b64encode(pickle.dumps(task)).decode()
        self.push_raw(payload)

    @transactional
    def pop(self, ctx: RedisDaoContext) -> TaskEntity:
        task_serialized = ctx.list_pop_entity(self.task_queue_id)
        if task_serialized:
            task_entity: TaskEntity = self._schema.from_dict(task_serialized)
            return pickle.loads(base64.b64decode(task_entity.payload.encode()))
        return None

    @transactional
    def push_raw(self, payload: str, ctx: RedisDaoContext):
        task_entity = TaskEntity(id=uuid.uuid4(), payload=payload)
        ctx.list_push_entity(self.task_queue_id, task_entity.to_dict())


class RedisTaskQueue(BaseTaskQueue):
    def __init__(self, db_pool):
        self._dao = RedisTaskQueueDao(db_pool)

    def put(self, task: Task):
        self._dao.push(task)

    @contextmanager
    def fetch_task(self) -> Iterator[Task]:
        yield self._dao.pop()
