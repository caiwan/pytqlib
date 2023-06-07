import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

from tq.task_dispacher import TaskResult, task_handler
from tq.tasks.workflow import FlowStepType

LOGGER = logging.getLogger(__name__)


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
