import abc
import collections
import contextlib
import heapq
import logging
import multiprocessing
import queue
import random
import threading
import time
from typing import Any, Callable, List

LOGGER = logging.getLogger(__name__)


class Job:
    def __init__(self, fn, parent, manager, *argv, **kwargs):
        self._fn = fn
        self._data = (argv, kwargs)
        self._parent = parent
        self._result = None
        self._manager = manager

        self._job_counter = collections.Counter(unfinished=1)

    def __call__(self):
        argv, kwargs = self._data
        self._result = self._fn(*argv, **kwargs, job=self, manager=self._manager)

    def job_created(self):
        self._job_counter.update({"unfinished": 1})
        if self.is_finished and self._parent:
            self._parent.job_created()

    def job_finished(self):
        self._job_counter.update({"unfinished": -1})
        if self._parent:
            self._parent.job_finished()

    @property
    def unfinished_jobs(self) -> int:
        return self._job_counter["unfinished"]

    @property
    def is_finished(self) -> bool:
        return self.unfinished_jobs == 0


class Worker(threading.Thread):
    def __init__(self, manager: "JobManager", *argv, **kwargs):
        threading.Thread.__init__(self, *argv, **kwargs)
        self._jobs = queue.Queue()
        self._manager = manager
        self._is_terminated = multiprocessing.Event()
        self._ident = threading.get_ident()

    def add_job(self, job: Job):
        self._jobs.put_nowait(job)

    def run(self):
        try:
            LOGGER.debug(f"Worker {self} starts")
            self._execute()
        except:
            LOGGER.error(
                "Unhandled Exception had occurred in thread {self}", exc_info=True
            )
            raise

    def _execute(self):
        while not self._is_terminated.is_set():
            job: Job = self._manager._get_job()

            if callable(job):
                try:
                    LOGGER.debug(f"Worker {self} execute job {job}")
                    job()

                except Exception as e:
                    LOGGER.error(
                        "Unhandled Exception had occurred in thread {self} while execute {job}",
                        exc_info=e,
                    )
                finally:
                    job.job_finished()

            elif job is not None:
                LOGGER.warning(f"Job {job} was not scheduled properly")

    def terminate(self):
        self._is_terminated.set()


class JobManager(contextlib.AbstractContextManager):
    def __init__(self, num_of_workers=0):
        super().__init__()
        self.num_of_workers = (
            num_of_workers if num_of_workers > 0 else multiprocessing.cpu_count() - 1
        )
        self.workers: List[Worker] = []

    def _spawn_workers(self):
        LOGGER.debug(f"Creating {self.num_of_workers} workers")
        self.workers = [Worker(self, daemon=True) for _ in range(self.num_of_workers)]
        for worker in self.workers:
            worker.start()

    def __enter__(self):
        self._spawn_workers()
        return self

    def __exit__(self, *argv, **kwargs):
        LOGGER.debug("Terminating job system")
        self.join()

    def create_job(self, fn: Callable, *argv, **kwargs) -> Job:
        return Job(fn, None, self, *argv, **kwargs)

    def create_child_job(self, parent: Job, fn: Callable, *argv, **kwargs) -> Job:
        parent.job_created()
        return Job(fn, parent, self, *argv, **kwargs)

    def schedule_job(self, job: Job):
        if job.unfinished_jobs > 0:
            worker: Worker = random.choice(self.workers)
            LOGGER.debug(f"Job Scheduled {job}")
            worker.add_job(job)

    def wait(self, job: Job):
        while not job.is_finished:
            another_job: Job = self._get_job()
            if callable(another_job):
                try:
                    another_job()
                except Exception as e:
                    LOGGER.error("Exception occurred", exc_info=e)
                    another_job.result = None
                another_job.job_finished()

    def join(self, timeout: float = None):
        for worker in self.workers:
            worker.terminate()
        for worker in self.workers:
            worker.join(timeout=timeout)

    def _steal(self) -> Worker:
        return random.choice(self.workers)

    def _find_worker(self):
        current_thread_ident = threading.get_ident()
        for worker in self.workers:
            if worker.ident == current_thread_ident:
                return worker
        return None

    def _get_job(self) -> Job:
        worker = self._find_worker()
        if not worker:
            return None

        if worker._jobs.empty():
            steal_from_worker = self._steal()
            if steal_from_worker == worker or steal_from_worker._jobs.empty():
                time.sleep(0.3)
                return None
            stolen_job = steal_from_worker._jobs.get_nowait()
            return stolen_job
        else:
            job = worker._jobs.get_nowait()
            return job
