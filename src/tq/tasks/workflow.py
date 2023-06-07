import abc
import enum
import logging
from datetime import datetime
from typing import Optional, TypeVar
from uuid import UUID

from statemachine import State, StateMachine

from tq.task_dispacher import TaskResultType

LOGGER = logging.getLogger(__name__)


class VerificationResult(enum.Enum):
    INCOMPLETE = 0
    SUCCESS = 1
    FAILED = 2


class FlowStateMachine(StateMachine):
    NEW = State("new", initial=True)
    PENDING = State("pending")
    DONE = State("done")
    ERROR = State("error")
    TIMEOUT = State("timeout")

    # TODO: use validators and guards

    task_created = NEW.to(PENDING)
    task_done = DONE.from_(PENDING, NEW)
    task_failed = ERROR.from_(NEW, PENDING)
    reset = NEW.from_(PENDING, ERROR, TIMEOUT)
    timeout = TIMEOUT.from_(PENDING)

    def __init__(self, name: str) -> None:
        super().__init__()
        self._name = name
        self._dirty = True

    def on_enter_state(self, event: str, state: State):
        self._dirty = True
        LOGGER.info(f"Step {self.name} changed to {state.name} by event {event}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_dirty(self) -> bool:
        return self._dirty

    def clear_dirty(self):
        self._dirty = False


class AbstractFlowStep(abc.ABC):
    def __init__(self, name: str, timeout: int = 0) -> None:
        super().__init__()

        self._state_macine = FlowStateMachine(name)

        self._name = name

        self._task_id: Optional[UUID] = None
        self._task_result: Optional[TaskResultType] = None

        self._timeout_seconds: int = timeout
        self._task_created_timestamp = datetime.min

        self._result = None

    def poll(self, *args, **kwargs):
        if self._state_macine.current_state == FlowStateMachine.NEW:
            if self.verify_done(*args, **kwargs):
                self._state_macine.task_done()
                return

            self._task_id = self.create_task(*args, **kwargs)

            if self._task_id is not None:
                self._task_created_timestamp = datetime.now()
                self._state_macine.task_created()
            else:
                LOGGER.error(f"Step {self.name} failed to create task")
                self._state_macine.task_failed()

        elif self._state_macine.current_state == FlowStateMachine.PENDING:
            if (
                self._timeout_seconds
                and datetime.now() - self._task_created_timestamp
                > self._timeout_seconds
            ):
                self._state_macine.timeout()
                return

            if self._result is not None:
                if self.verify_done(*args, **kwargs):
                    self.post_step(*args, **kwargs)
                    self._state_macine.task_done()
                else:
                    # TODO: Add failure reason
                    self._state_macine.task_failed()

    @abc.abstractmethod
    def create_task(self, *args, **kwargs) -> Optional[UUID]:
        pass

    @abc.abstractmethod
    def verify_done(self, *args, **kwargs) -> bool:
        pass

    def set_task_result(self, result):
        self._result = result

    def post_step(self, *args, **kwargs):
        pass

    @property
    def is_done(self) -> bool:
        return self._state_macine.current_state == FlowStateMachine.DONE

    @property
    def is_pending(self) -> bool:
        return any(
            [
                self._state_macine.current_state == FlowStateMachine.NEW,
                self._state_macine.current_state == FlowStateMachine.PENDING,
            ]
        )

    @property
    def is_failed(self) -> bool:
        return any(
            [
                self._state_macine.current_state == FlowStateMachine.ERROR,
                self._state_macine.current_state == FlowStateMachine.TIMEOUT,
            ]
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def task_id(self) -> Optional[UUID]:
        return self._task_id

    @property
    def is_dirty(self) -> bool:
        return self._state_macine._dirty

    def clear_dirty(self) -> bool:
        self._state_macine.clear_dirty()


FlowStepType = TypeVar("FlowStepType", bound=AbstractFlowStep)
