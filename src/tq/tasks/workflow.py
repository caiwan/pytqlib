import abc
import enum
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from uuid import UUID

from statemachine import State, StateMachine

from tq.task_dispacher import TaskResult, TaskResultType, TaskType, task_handler

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


@dataclass
class WorkflowNode:
    step: Optional[FlowStepType] = None
    children: List["WorkflowNode"] = field(default_factory=list)

    def add_child(self, child):
        self.children.append(child)


class Workflow:
    def __init__(self) -> None:
        self._root: WorkflowNode = WorkflowNode()
        self._args: List[Any] = []
        self._kwargs: Dict[str, Any] = defaultdict(lambda: None)

    # TODO: Workflow id?

    def iterate_nodes(self) -> Iterator[WorkflowNode]:
        q = [self._root]
        while q:
            node = q.pop()
            yield node
            for c in node.children:
                q.append(c)

    def iterate_steps(self) -> Iterator[FlowStepType]:
        for node in self.iterate_nodes():
            if node.step:
                yield node.step

    def iterate_incomplete_steps(self) -> Iterator[FlowStepType]:
        q = [self._root]
        while q:
            node = q.pop()
            if node.step and not node.step.is_done and node.step.is_pending:
                yield node.step
            elif not node.step or node.step.is_done:
                for child in node.children:
                    q.append(child)

    def poll(self, max_count: int = 0) -> int:
        flow_count = 0
        for flow in self.iterate_incomplete_steps():
            flow.poll(*self._args, **self._kwargs)
            flow_count += 1
            if max_count > 0 and flow_count == max_count:
                return flow_count
        return flow_count

    def is_done(self) -> bool:
        return all([step.is_done for step in self.iterate_steps()])

    def is_pending(self) -> bool:
        return any([step.is_pending for step in self.iterate_steps()])

    def is_failed(self) -> bool:
        return any([step.is_failed for step in self.iterate_steps()])


class WorkflowBuilder:
    def __init__(self, workflow: Workflow) -> None:
        self._workflow = workflow

        LOGGER.debug(f"building workflow={workflow}")

        self._node_map: Dict[str, WorkflowNode] = dict(
            [
                (node.step.name if node.step else None, node)
                for node in workflow.iterate_nodes()
            ]
        )

    def then_do(self, step: FlowStepType, after: str = None) -> "WorkflowBuilder":
        if not after in self._node_map:
            raise ValueError(
                f"No such exist step exists '{after}' to insert '{step.name}' as child"
            )

        LOGGER.debug(f"inserting step={step.name} after step={after}")

        if step.name in self._node_map:
            raise ValueError(f"Step with name '{step.name}' already exists")

        child_node = WorkflowNode(
            step=step,
        )

        self._node_map[after].add_child(child_node)
        self._node_map[step.name] = child_node

        return self

    def with_params(self, *args, **kwargs) -> "WorkflowBuilder":
        self._workflow._args = args
        self._workflow._kwargs = kwargs
        return self

    @property
    def workflow(self) -> Workflow:
        return self._workflow


class WorkflowManager:
    def __init__(self, max_concurrent_steps: int = 0) -> None:
        self._workflows: List[Workflow] = []
        self._max_concurrent_steps: int = max_concurrent_steps

    def create(self) -> WorkflowBuilder:
        workflow = Workflow()
        LOGGER.debug(f"creating workflow={workflow}")
        self._workflows.append(workflow)
        return WorkflowBuilder(workflow)

    def poll(self):
        step_count = 0
        for workflow in self._workflows:
            if self.max_concurrent_steps > 0:
                remaining_steps = self.max_concurrent_steps - step_count
                if remaining_steps > 0:
                    workflow.poll(remaining_steps)
            else:
                workflow.poll()

    @property
    def all_done(self):
        return all(workflow.is_done() for workflow in self._workflows)

    @property
    def max_concurrent_steps(self) -> int:
        return self._max_concurrent_steps

    @task_handler(TaskResult)
    def handle_task_result(self, task_result: TaskResult, *a, **w):
        task_id = task_result.task_id
        # Any better idea ???
        for workflow in self._workflows:
            for step in workflow.iterate_incomplete_steps():
                if step.task_id == task_id:
                    LOGGER.info(f"Task {task_id} result returned, updating {step.name}")
                    step.set_task_result(task_result)

    def reset_steps_with_timeout(self):
        for workflow in self._workflows:
            for step in workflow.iterate_steps():
                if step.is_timeout:
                    step.reset()

    # TODO: Get workflow states
    # TODO: Get errors / timeouts
    def get_workflow():
        pass

    def persist():
        # TODO: DB
        pass

    def restore():
        # tODO: DB
        pass

    # TODO: Get all workflow/steps which had changed their state since last query
    # or had been created -> step.is_dirty and clear_dirty()

    # TODO: CXreate a DB message channel/DB in which this information is sotred [for frontend]
