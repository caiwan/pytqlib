import logging
from contextlib import ExitStack

import pytest
import redis

from tq.job_system import JobManager
from tq.task_dispacher import LocalTaskQueue, TaskDispatcher

LOGGER = logging.getLogger(__name__)

DB_PORT = 6379
DB_HOST = "localhost"


def pytest_addoption(parser):
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="run slow tests",
    )

    parser.addoption(
        "--unittest_only",
        action="store_true",
        help="run only unittests",
    )

    parser.addoption(
        "--integration_only",
        action="store_true",
        help="run only integration tests",
    )


# USE @pytest.mark.integration


@pytest.fixture(scope="function", autouse=True)
def setup_logs(caplog):
    caplog.set_level(logging.INFO, logger="tq.job_system")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--unittest_only"):
        # filter out integration tests
        skip_integration = pytest.mark.skip(
            reason="need --integration_only option to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    elif config.getoption("--integration_only"):
        # filter out unittests
        skip_unittest = pytest.mark.skip(reason="need --unittest_only option to run")
        for item in items:
            if "unittest" in item.keywords:
                item.add_marker(skip_unittest)

    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(scope="function")
def db_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(host=DB_HOST, port=DB_PORT, db=0)


@pytest.fixture(scope="function")
def db_connection(db_pool) -> redis.Redis:
    db = redis.Redis(connection_pool=db_pool)
    yield db
    db.flushall()


@pytest.fixture(scope="function")
def task_dispatcher():
    task_queue = LocalTaskQueue()
    with ExitStack() as stack:
        job_manager = stack.enter_context(JobManager())
        dispatcher = stack.enter_context(TaskDispatcher(task_queue, job_manager))
        yield dispatcher
        dispatcher.terminate()
