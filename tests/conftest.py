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
        "--unittest",
        action="store_true",
        help="run only unittests",
    )

    parser.addoption(
        "--integration",
        choices=["nodb", "redis", "mongo", "all"],
        default=None,
        help="run only integration tests",
    )

    parser.addoption(
        "--all",
        action="store_true",
        help="run all tests",
    )


# USE @pytest.mark.integration


@pytest.fixture(scope="function", autouse=True)
def setup_logs(caplog):
    caplog.set_level(logging.INFO, logger="tq.job_system")


def pytest_collection_modifyitems(config, items):
    # Mark db tests as integration first
    for item in items:
        if any(k in item.keywords for k in {"mongo", "redis"}):
            item.keywords.append("integration")

    # Disable slow tests
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if config.getoption("--all"):
        return

    # Select which tests to run
    if config.getoption("--unittest"):
        # filter out integration tests
        skip_integration = pytest.mark.skip(
            reason="need --integration option to run",
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    elif config.getoption("--integration"):
        choice = config.getvalue("--integration")

        # filter out unittests
        skip_unittest = pytest.mark.skip(
            reason="need --unittest_only or --all option to run",
        )

        # filter out db tests
        skip_redis = pytest.mark.skip(
            reason="need --integration all or redis to run",
        )

        skip_mongo = pytest.mark.skip(
            reason="need --integration all or mongo to run",
        )

        for item in items:
            if "unittest" in item.keywords:
                item.add_marker(skip_unittest)

            if choice == "redis" and "mongo" in item.keywords:
                item.add_marker(skip_mongo)
            elif choice == "mongo" and "redis" in item.keywords:
                item.add_marker(skip_redis)
            elif choice != "all" and any(
                k in item.keywords for k in {"mongo", "redis"}
            ):
                item.add_marker(skip_mongo)
                item.add_marker(skip_redis)


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
