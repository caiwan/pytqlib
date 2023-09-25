import abc
import logging
import queue
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing import Event
from typing import Any, Callable, Dict, Iterator, List, Optional, Type, TypeVar
from uuid import UUID, uuid4

from tq import bind_function
from tq.job_system import Job, JobManager

LOGGER = logging.getLogger(__name__)


@dataclass
class Task:
    @property
    def task_id(self) -> Optional[UUID]:
        return getattr(self, "_task_id", None)


@dataclass
class TaskResult(Task):
    task: Task

    @property
    def task_id(self) -> Optional[UUID]:
        return self.task.task_id if self.task else None

    def failed(self, reason: Any):
        setattr(self, "_failed", True)
        setattr(self, "_failure_reason", reason)
        return self

    @property
    def is_failed(self) -> bool:
        return getattr(self, "_failed", False)

    @property
    def failure_reason(self) -> Any:
        return getattr(self, "_failure_reason", None)


TaskType = TypeVar("TaskType", bound=Task)
TaskResultType = TypeVar("TaskResultType", bound=TaskResult)


class TerminateDispatcherLoop(Task):
    pass


def task_handler(*task_type_list):
    """
    Decorator for task handler functions.
    Example:
        @task_handler(MyTask)
        def fetch_user_page(self, task: MyTask, job: Job, manager: JobManager, dispatcher: TaskDispatcher):
            pass

    """

    def decorator(f):
        f.task_hanlder_type_list = task_type_list
        return f

    return decorator


class BaseTaskQueue(abc.ABC):
    @abc.abstractmethod
    def put(self, task: Task):
        pass

    @contextmanager
    @abc.abstractmethod
    def fetch_task(self) -> Iterator[Task]:
        yield


class LocalTaskQueue(BaseTaskQueue):
    def __init__(self) -> None:
        super().__init__()
        self.queue = queue.Queue()

    def put(self, task: Task):
        self.queue.put(task)

    @contextmanager
    def fetch_task(self) -> Iterator[Task]:
        yield self.queue.get()
        self.queue.task_done()


class TaskDispatcher:
    def __init__(self, task_queue: BaseTaskQueue, job_manager: JobManager) -> None:
        # TODO: Use defaultdict
        self.task_handlers: Dict[Type, List[Callable]] = {}
        self.task_queque: BaseTaskQueue = task_queue
        self.tasks = []
        self.job_manager = job_manager
        self._exit_event = Event()

    @property
    def is_exit(self):
        return self._exit_event.is_set()

    def __enter__(self):
        LOGGER.debug("Dispatch loop starting.")
        self._schedule_dispatch_job()
        return self

    def __exit__(self, *a, **w):
        LOGGER.debug("Dispatch loop terminating.")
        self._exit_event.set()

    def terminate(self):
        # TODO: This does not work the way intended
        self.post_task(TerminateDispatcherLoop())

    @staticmethod
    def _get_task_hanlders(clazz: Type):
        for func in clazz.__dict__.values():
            if hasattr(func, "task_hanlder_type_list"):
                yield func, func.task_hanlder_type_list

    def register_task_handler_callback(self, task_type: Type, handler_func: Callable):
        self.task_handlers[task_type].append(handler_func)

    def register_task_handler(self, dispatcher: Any):
        for handler_func, handling_type_list in TaskDispatcher._get_task_hanlders(
            type(dispatcher)
        ):
            for task_type in handling_type_list:
                # TODO: Use defaultdict
                if task_type not in self.task_handlers:
                    self.task_handlers[task_type] = []

                self.task_handlers[task_type].append(
                    bind_function(handler_func, dispatcher)
                )
                LOGGER.debug(f"{task_type} had been registered to {dispatcher}")

    # TODO: Add unregister

    def post_task(self, task: Task) -> UUID:
        if not task.task_id:
            setattr(task, "_task_id", uuid4())
        LOGGER.debug(f"Task posted: {task}")
        self.task_queque.put(task)
        return task.task_id

    def _schedule_dispatch_job(self):
        job = self.job_manager.create_job(
            bind_function(TaskDispatcher._dispatch_loop, self)
        )
        self.job_manager.schedule_job(job)

    def _dispatch_loop(self, job: Job, manager: JobManager):
        LOGGER.debug("dispatch loop_tick")
        is_continue = True
        with self.task_queque.fetch_task() as task:
            if task:
                LOGGER.info(f"Dispatch task: {task}")
                is_continue = self._dispatch_task(task, job, manager)
                if is_continue:
                    LOGGER.debug("schedule dispatch next tick")
                    self._schedule_dispatch_job()
                else:
                    LOGGER.info("Dispatcher terminated")

    def _dispatch_task(self, task: Task, job: Job, manager: JobManager):
        if not isinstance(task, TerminateDispatcherLoop):
            task_type = type(task)
            handler_jobs = []
            if task_type in self.task_handlers:
                for handler in self.task_handlers[task_type]:
                    LOGGER.debug(f"Create job = {handler} for task = {task}")
                    handler_job = manager.create_child_job(
                        job, handler, task, dispatcher=self
                    )
                    manager.schedule_job(handler_job)
                    handler_jobs.append(handler_job)

            for handler_job in handler_jobs:
                manager.wait(handler_job)
                # TODO: IF handler returns with result dispatch it.

            for handler_job in handler_jobs:
                if handler_job.result and isinstance(handler_job.result, TaskResult):
                    self.post_task(handler_job.result)

            return True

        self._exit_event.set()
        return False
