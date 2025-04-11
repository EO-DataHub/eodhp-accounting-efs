import logging
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample


@dataclass
class SampleRequestMsg:
    workspace: str
    path: Path


class EFSSamplerMessager(Messager[Iterable[str], BillingResourceConsumptionRateSample]):
    def process_msg(self, msg: Iterable[SampleRequestMsg]) -> Iterable[Messager.Action]:
        actions = []

        for sample_request in msg:
            start_time = datetime.now(timezone.utc)
            size = self.count_size(sample_request.path)

            if size is None:
                actions.append(Messager.FailureAction(permanent=True))
            else:
                sample_msg = BillingResourceConsumptionRateSample(
                    uuid=str(uuid.uuid4()),
                    sample_time=start_time.isoformat(),
                    sku="EFS-STORAGE-STD",
                    user=None,
                    workspace=sample_request.workspace,
                    rate=size,
                )

                actions.append(Messager.PulsarMessageAction(payload=sample_msg))

        return actions

    @classmethod
    def count_size(cls, path: Path) -> Optional[int]:
        # This uses `du` because we expect there to be a lot of files, with the efficiency fain
        # from `du` being written in C and being very mature outweighing the cost of running
        # a process.
        du_result = subprocess.run(
            ["du", "--apparent-size", "-b", "-P", "-s", str(path)],
            capture_output=True,
            text=True,
            timeout=1800,
        )

        if du_result.returncode != 0:
            logging.error("Failed to calculate size of %s: %s", str(path), du_result.stderr)
            return None

        # The result should be a number in bytes in the form `<number>\t<path>\n`.
        # Anything else will be an error reported by du.
        try:
            size_str = du_result.stdout.split("\t")[0]
            size = int(size_str)
            logging.debug("Size of %s was %i", str(path), size)
            return size
        except (ValueError, IndexError):
            logging.exception(
                "Failed to calculate size of %s (output not int): %s", str(path), du_result.stdout
            )
            return None

    def gen_empty_catalogue_message(self, msg: Iterable[str]) -> dict:
        return {}
