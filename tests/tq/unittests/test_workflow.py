import logging
from typing import Callable, Optional
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
import waiting

from tq.task_dispacher import Task, TaskDispatcher, TaskResult, task_handler
from tq.tasks.workflow import AbstractFlowStep
from tq.tasks.workflow_manager import WorkflowManager

LOGGER = logging.getLogger(__name__)


class MokcedFlowStep(AbstractFlowStep):
    def __init__(self, name: str, timeout: int = 0, success: bool = True) -> None:
        super().__init__(name, timeout)
        self.dummy_result: Optional[TaskResult] = None
        self.success = success

    def create_task(self) -> Optional[UUID]:
        random_uuid = uuid4()
        if self.success:
            self.dummy_result = TaskResult(task=Task())
        else:
            self.dummy_result = TaskResult(task=Task()).failed("Failed")
        setattr(self.dummy_result.task, "_task_id", random_uuid)
        return random_uuid

    def verify_done(self) -> bool:
        return self._result is not None and not self._result.is_failed


class DummyTaskHandler:
    def __init__(self, accept=True) -> None:
        self.accept = accept

    @task_handler(Task)
    def handle_task(self, task: Task, dispatcher: TaskDispatcher, *a, **w):
        if self.accept:
            dispatcher.post_task(TaskResult(task=task))
        else:
            dispatcher.post_task(TaskResult(task=task).failed("Failed"))


class DummyFlowStep(AbstractFlowStep):
    def __init__(self, name: str, class_=Task, timeout: int = 0) -> None:
        super().__init__(name, timeout)
        self._cls = class_

    def create_task(self, task_dispatcher: TaskDispatcher, *a, **k) -> Optional[UUID]:
        return task_dispatcher.post_task(self._cls())

    def verify_done(self, *a, **k) -> bool:
        return self._result is not None


def test_create_and_execute_workflow():
    workflow_manager: WorkflowManager = WorkflowManager()

    workflow = (
        workflow_manager.create()
        .then_do(MokcedFlowStep("step1"))
        .then_do(MokcedFlowStep("step2"), after="step1")
        .then_do(MokcedFlowStep("step3"), after="step1")
    ).workflow

    def _poll():
        workflow_manager.poll()

        for flow in workflow.iterate_steps():
            if flow.is_pending and isinstance(flow, MokcedFlowStep):
                workflow_manager.handle_task_result(flow.dummy_result)

        return workflow_manager.all_finished

    waiting.wait(_poll, timeout_seconds=1, sleep_seconds=0.1)

    assert all(flow.is_done for flow in workflow.iterate_steps())


@pytest.mark.slow
@pytest.mark.parametrize(
    "task_count",
    [1, 10, 100],
)
@pytest.mark.parametrize(
    "max_concurrent_steps",
    [0, 10, 100],
)
def test_create_workflow_with_task_dispatcher(
    task_dispatcher: TaskDispatcher, task_count: int, max_concurrent_steps: int
):
    workflow_manager: WorkflowManager = WorkflowManager(
        max_concurrent_steps=max_concurrent_steps
    )

    task_dispatcher.register_task_handler(workflow_manager)

    dummy_handler = DummyTaskHandler()
    task_dispatcher.register_task_handler(dummy_handler)

    for _ in range(task_count):
        (
            workflow_manager.create()
            .then_do(DummyFlowStep("step1"))
            .then_do(DummyFlowStep("step2"), after="step1")
            .then_do(DummyFlowStep("step3"), after="step1")
            .with_params(task_dispatcher)
        )

    def _poll():
        workflow_manager.poll()
        return workflow_manager.all_done

    waiting.wait(_poll, timeout_seconds=240)

    assert all(flow.is_done for flow in workflow_manager.iterate_workflows())


# TODO: Add erorr handling
def test_workflow_fails():
    workflow_manager: WorkflowManager = WorkflowManager()

    workflow = (
        workflow_manager.create()
        .then_do(MokcedFlowStep("step1"))
        .then_do(MokcedFlowStep("step2", success=False), after="step1")
        .then_do(MokcedFlowStep("step3"), after="step1")
        .then_do(MokcedFlowStep("step4"), after="step2")
    ).workflow

    def _poll():
        workflow_manager.poll()

        for flow in workflow.iterate_steps():
            if flow.is_pending and isinstance(flow, MokcedFlowStep):
                workflow_manager.handle_task_result(flow.dummy_result)

        return workflow_manager.all_finished

    waiting.wait(_poll, timeout_seconds=1, sleep_seconds=0.1)

    assert all([flow.is_finished for flow in workflow.iterate_steps()])

    failed = filter(lambda flow: flow.is_failed, workflow.iterate_steps())
    assert len(list(failed)) == 2


# TODO: Add timeout

# TODO: Add queries
