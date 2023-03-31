import logging
from contextlib import ExitStack
from dataclasses import dataclass
from unittest.mock import Mock

import pytest
import waiting

from tq.job_system import JobManager
from tq.redis_task_queue import RedisTaskQueue
from tq.task_dispacher import Task, TaskDispatcher, task_handler

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def task_dispatcher_with_redis(db_pool):
    task_queue = RedisTaskQueue(db_pool)
    with ExitStack() as stack:
        job_manager = stack.enter_context(JobManager())
        dispatcher = stack.enter_context(TaskDispatcher(task_queue, job_manager))
        yield dispatcher


@dataclass
class DummyTaskOne(Task):
    pass


@dataclass
class DummyTaskTwo(Task):
    pass


@dataclass
class DummyTaskThree(Task):
    pass


class DummyTaskHandler:
    def __init__(self) -> None:
        self.mocks = [Mock() for _ in range(3)]

    @task_handler(DummyTaskOne)
    def task_one(self, *a, **w):
        LOGGER.debug("Handling dummy task 1")
        self.mocks[0]()

    @task_handler(DummyTaskTwo)
    def task_two(self, *a, **w):
        LOGGER.debug("Handling dummy task 2")
        self.mocks[1]()

    @task_handler(DummyTaskOne, DummyTaskTwo, DummyTaskThree)
    def task_three(self, *a, **w):
        LOGGER.debug("Handling dummy task 3")
        self.mocks[2]()


@pytest.mark.integration
def test_task_dipatcher_dispatches_tasks_with_redis(
    task_dispatcher_with_redis: TaskDispatcher,
):
    dummy_handler = DummyTaskHandler()
    task_dispatcher_with_redis.register_task_handler(dummy_handler)

    all(
        [
            task_dispatcher_with_redis.post_task(t)
            for t in [DummyTaskOne(), DummyTaskTwo(), DummyTaskThree()]
        ]
    )

    waiting.wait(
        lambda: all(mock.called for mock in dummy_handler.mocks),
        sleep_seconds=0.1,
        timeout_seconds=30,
    )
