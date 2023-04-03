import logging
from unittest.mock import Mock

import pytest
from waiting import wait

from tq.job_system import Job, JobManager

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def job_manager():
    with JobManager() as job_manager:
        yield job_manager


@pytest.mark.slow
@pytest.mark.parametrize("job_count", [1, 10, 100, 1000])
def test_job_system_single_task(job_manager, job_count):
    def log_fn(*a, **w) -> None:
        # LOGGER.debug(f"Job called: {a}, {w}")
        pass

    job_fns = [
        [Mock(side_effect=lambda *a, **w: log_fn(*a, **w)), None]
        for _ in range(job_count)
    ]

    for index, (job_fn, _) in enumerate(job_fns):
        # LOGGER.debug(f"New job {index}")
        job_fns[index][1] = job_manager.create_job(job_fn)

    for _, job in job_fns:
        # LOGGER.debug(f"Schedule {_}")
        job_manager.schedule_job(job)

    wait(
        lambda: all(fn.called for fn, _ in job_fns),
        sleep_seconds=0.1,
        timeout_seconds=5,
    )


@pytest.mark.slow
@pytest.mark.parametrize("job_count", [1, 10, 100, 1000])
def test_job_system_subtasks(job_manager, job_count):
    def log_fn(*a, **w) -> None:
        # LOGGER.debug(f"Child Job called: {a}, {w}")
        return True

    def test_fn(job: Job, manager: JobManager):
        # LOGGER.debug(f"job called: {job}")

        mock = Mock(side_effect=log_fn)
        new_job = manager.create_child_job(job, mock)
        manager.schedule_job(new_job)
        manager.wait(new_job)

        assert mock.called
        return True

    job_fns = [[Mock(side_effect=test_fn), None] for _ in range(job_count)]

    for index, (job_fn, _r) in enumerate(job_fns):
        job_fns[index][1] = job_manager.create_job(job_fn)

    for _f, job in job_fns:
        job_manager.schedule_job(job)

    wait(
        lambda: all(mock.called for mock, _j in job_fns),
        sleep_seconds=0.1,
        timeout_seconds=1,
    )


# TODO Test factorial?
# TODO Test iteration? Add iteration tools?
# TODO Test for error recovery? (When the call stack becomes way too deep, the worker has to be recovered)
