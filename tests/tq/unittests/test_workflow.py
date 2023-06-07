import logging
from typing import Optional
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
import waiting

from tq.task_dispacher import Task, TaskDispatcher, TaskResult, task_handler
from tq.tasks.workflow import AbstractFlowStep
from tq.tasks.workflow_manager import WorkflowManager

LOGGER = logging.getLogger(__name__)


# @pytest.fixture(scope="function", autouse=True)
# def workflow_manager():
#     yield WorkflowManager()


class MokcedFlowStep(AbstractFlowStep):
    def create_task(self) -> Optional[UUID]:
        return uuid4()

    def verify_done(self) -> bool:
        return True


class DummyTaskHandler:
    def __init__(self) -> None:
        self.mocks = [Mock() for _ in range(3)]

    @task_handler(Task)
    def handle_task(self, task: Task, dispatcher: TaskDispatcher, *a, **w):
        dispatcher.post_task(TaskResult(task=task))


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
        return workflow_manager.all_done

    waiting.wait(_poll, timeout_seconds=1)

    for flow in workflow.iterate_steps():
        assert flow.is_done


def test_create_workflow_with_task_dispatcher(task_dispatcher: TaskDispatcher):
    workflow_manager0: WorkflowManager = WorkflowManager()

    task_dispatcher.register_task_handler(workflow_manager0)

    dummy_handler = DummyTaskHandler()
    task_dispatcher.register_task_handler(dummy_handler)

    LOGGER.info(f"workflows={workflow_manager0._workflows}")

    workflow = (
        workflow_manager0.create()
        .then_do(DummyFlowStep("step1"))
        .then_do(DummyFlowStep("step2"), after="step1")
        .then_do(DummyFlowStep("step3"), after="step1")
        .with_params(task_dispatcher)
    ).workflow

    def _poll():
        workflow_manager0.poll()
        return workflow_manager0.all_done

    waiting.wait(_poll, timeout_seconds=10)

    for flow in workflow.iterate_steps():
        assert flow.is_done


# TODO: Add erorr handling

# TODO: Add timeout

# TODO: Add queries
