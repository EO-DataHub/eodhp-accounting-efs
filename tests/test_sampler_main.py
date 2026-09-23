from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from click.testing import CliRunner
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample

from accounting_efs.sampler.__main__ import cli, main


def test_main_scans_dirs(block_size: int) -> None:
    with (
        mock.patch("accounting_efs.sampler.__main__.get_pulsar_client") as mock_getclient,
        TemporaryDirectory() as tmpdir_str,
    ):
        ############## Setup
        tmpdir = Path(tmpdir_str)

        (tmpdir / "workspace0").mkdir()
        w1 = tmpdir / "workspace1"
        w1.mkdir()

        with open(w1 / "500-byte-file", "wb") as fh:
            fh.write(b"1234" * 125)

        ############## Invoke sample
        main(tmpdir_str, verbose=2, once=True)

        ############## Check results
        calls = (
            mock_getclient()
            .create_producer(topic="billing-events-consumption-rate-samples", producer_name=any, schema=any)
            .send.call_args_list
        )

        be0: BillingResourceConsumptionRateSample = calls[0].args[0]
        be1: BillingResourceConsumptionRateSample = calls[1].args[0]

        if be0.workspace == "workspace1":
            tmp = be0
            be0 = be1
            be1 = tmp

        assert be0.sku == "EFS-STORAGE-STD"
        assert be0.workspace == "workspace0"
        assert be0.rate == block_size / (1024.0**3)

        assert be1.sku == "EFS-STORAGE-STD"
        assert be1.workspace == "workspace1"
        assert be1.rate == block_size * 2 / (1024.0**3)


@pytest.mark.parametrize(
    ("topic", "expected_topic"),
    [
        pytest.param(None, "billing-events-consumption-rate-samples", id="default"),
        pytest.param(
            "persistent://public/billing/billing-events-consumption-rate-samples",
            "persistent://public/billing/billing-events-consumption-rate-samples",
            id="given",
        ),
    ],
)
def test_main_sends_to_one_topic(topic: str | None, expected_topic: str) -> None:
    with (
        mock.patch("accounting_efs.sampler.__main__.get_pulsar_client") as mock_getclient,
        TemporaryDirectory() as tmpdir_str,
    ):
        if topic is None:
            main(tmpdir_str, once=True)
        else:
            main(tmpdir_str, once=True, topic=topic)

    mock_getclient.return_value.create_producer.assert_called_once()
    assert mock_getclient.return_value.create_producer.call_args.kwargs["topic"] == expected_topic


@pytest.mark.parametrize(
    ("env", "expected_topic"),
    [
        pytest.param({}, "billing-events-consumption-rate-samples", id="default"),
        pytest.param(
            {"PULSAR_TOPIC": "persistent://public/billing/billing-events-consumption-rate-samples"},
            "persistent://public/billing/billing-events-consumption-rate-samples",
            id="from PULSAR_TOPIC",
        ),
    ],
)
def test_cli_takes_topic_from_environment(env: dict[str, str], expected_topic: str) -> None:
    with (
        mock.patch("accounting_efs.sampler.__main__.setup_logging"),
        mock.patch("accounting_efs.sampler.__main__.main") as mock_main,
        TemporaryDirectory() as tmpdir_str,
    ):
        result = CliRunner().invoke(cli, [tmpdir_str, "--interval", "60", "--once"], env={"PULSAR_TOPIC": None} | env)

    assert result.exit_code == 0, result.output
    assert mock_main.call_args.args[-1] == expected_topic
