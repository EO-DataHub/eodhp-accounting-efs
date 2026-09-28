import subprocess
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample

from accounting_efs.sampler.messager import EFSSamplerMessager, SampleRequestMsg


@pytest.fixture
def sampler_messager() -> EFSSamplerMessager:
    return EFSSamplerMessager()


def test_no_workspaces_produces_no_messages(sampler_messager: EFSSamplerMessager) -> None:
    actions = sampler_messager.process_msg([])
    assert list(actions) == []


def test_consume_no_workspaces_produces_no_failures(sampler_messager: EFSSamplerMessager) -> None:
    failures = sampler_messager.consume([])

    assert not failures.any_temporary()
    assert not failures.any_permanent()


def test_workspace_sample_produces_correct_message(
    test_dir: tuple[Path, float], sampler_messager: EFSSamplerMessager
) -> None:
    # This is a parameterized fixture so this test will run with a variety of different sizes
    # and cases.
    test_dir_path, test_dir_size = test_dir

    actions = list(sampler_messager.process_msg([SampleRequestMsg("workspace0", test_dir_path)]))

    assert len(actions) == 1
    assert isinstance(actions[0], Messager.PulsarMessageAction)
    assert isinstance(actions[0].payload, BillingResourceConsumptionRateSample)

    sample = actions[0].payload
    uuid.UUID(sample.uuid)  # pyright: ignore[reportArgumentType]
    datetime.fromisoformat(sample.sample_time)  # pyright: ignore[reportArgumentType]
    assert sample.sku == "EFS-STORAGE-STD"
    assert sample.user is None
    assert sample.workspace == "workspace0"
    assert sample.rate == test_dir_size


def test_du_timeout_fails_only_that_workspace(
    monkeypatch: pytest.MonkeyPatch, sampler_messager: EFSSamplerMessager, tmp_path: Path
) -> None:
    def raise_timeout(*args: object, **kwargs: object) -> None:
        raise subprocess.TimeoutExpired(cmd="du", timeout=1800)

    monkeypatch.setattr(subprocess, "run", raise_timeout)

    actions = list(
        sampler_messager.process_msg(
            [
                SampleRequestMsg("slow-workspace", tmp_path),
                SampleRequestMsg("other-workspace", tmp_path),
            ]
        )
    )

    assert len(actions) == 2
    assert isinstance(actions[0], Messager.FailureAction)
    assert actions[0].permanent
    assert isinstance(actions[1], Messager.FailureAction)
    assert actions[1].permanent
