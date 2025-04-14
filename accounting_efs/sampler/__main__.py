import logging
import time
from pathlib import Path

import click
from eodhp_utils.pulsar.messages import (
    generate_billingresourceconsumptionratesample_schema,
)
from eodhp_utils.runner import get_pulsar_client, log_component_version, setup_logging

from accounting_efs.sampler.messager import EFSSamplerMessager, SampleRequestMsg


def generate_sample_requests(parent: Path):
    assert parent.is_dir()

    for subdir in parent.iterdir():
        if subdir.is_dir():
            yield SampleRequestMsg(workspace=subdir.name, path=subdir)


@click.command
@click.option("-v", "--verbose", count=True)
@click.option("--pulsar-url")
@click.option("--interval", type=int)
@click.option("--once", is_flag=True)
@click.argument("dir")
def cli(dir: str, verbose: int = 1, interval: int = 3600, pulsar_url=None, once=False):
    setup_logging(verbosity=verbose)
    log_component_version("eodhp-accounting-efs")

    logging.info("Monitoring %s with target interval %i seconds")

    main(dir, verbose, interval, pulsar_url, once)


def main(dir: str, verbose: int = 1, interval: int = 3600, pulsar_url=None, once=False):
    pulsar_client = get_pulsar_client(pulsar_url=pulsar_url)

    producer = pulsar_client.create_producer(
        topic="billing-events-consumption-rate-samples",
        producer_name=f"efs-monitor-{dir}",
        schema=generate_billingresourceconsumptionratesample_schema(),
    )

    messager = EFSSamplerMessager(producer=producer)

    while True:
        scan_start = time.time()

        logging.info("Scanning all workspace dirs in %s", dir)
        failures = messager.consume(generate_sample_requests(Path(dir)))

        scan_end = time.time()
        scan_time = scan_end - scan_start
        logging.info("Scan took %i seconds", scan_time)

        if failures.any_permanent():
            logging.fatal("Got permanent error from EFSSamplerMessager - exiting")
            exit(1)

        if once:
            return

        wait_time = interval - scan_time
        if wait_time > 0:
            time.sleep(wait_time)


if __name__ == "__main__":
    cli()
