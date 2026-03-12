"""ContentsPal routing logic for FNOL messages.

Checks parsed FNOL data for specific dataset and job_type values and
produces a side-effect message to the contentspal_delivery topic when
criteria are met.

Matching criteria:
    - dataset (carrierId) == "3372667"
    - job_type (rotationTrade) in ("INVENTORY", "PRICING TASK")
"""

import logging
from datetime import UTC, datetime

from pipeline.verisk.handlers.base import FileHandlerSideEffect
from pipeline.verisk.schemas.fnol import FnolMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)

CONTENTSPAL_DATASET = "3372667"
CONTENTSPAL_JOB_TYPES = frozenset({"INVENTORY", "PRICING TASK"})
CONTENTSPAL_TOPIC_KEY = "contentspal_delivery"


def build_contentspal_side_effect(
    task: DownloadTaskMessage,
    parsed_data: dict,
) -> FileHandlerSideEffect | None:
    """Return a side effect for contentspal_delivery if criteria match, else None."""
    dataset = parsed_data.get("dataset")
    job_type = parsed_data.get("job_type")

    if dataset != CONTENTSPAL_DATASET or job_type not in CONTENTSPAL_JOB_TYPES:
        return None

    logger.info(
        "FNOL matched ContentsPal routing criteria",
        extra={
            "trace_id": task.trace_id,
            "dataset": dataset,
            "job_type": job_type,
        },
    )

    return FileHandlerSideEffect(
        topic_key=CONTENTSPAL_TOPIC_KEY,
        message=FnolMessage.from_handler_data(
            task_message=task,
            parsed_data=parsed_data,
            produced_at=datetime.now(UTC),
        ),
    )
