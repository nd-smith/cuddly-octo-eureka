"""Handler for estimatePackageReceived XML attachments.

Handles: estimatePackageReceived + xml
Parses REINSPECTION_FORM.XML and extracts structured fields plus the
full raw payload for downstream consumption.
"""

import asyncio
import logging
from pathlib import Path
from xml.etree import ElementTree

from pipeline.common.logging import extract_log_context
from pipeline.verisk.handlers.base import (
    DownloadFileHandler,
    FileHandlerResult,
    FileHandlerSideEffect,
    register_handler,
)
from pipeline.verisk.schemas.grd import GrdMessage
from pipeline.verisk.schemas.reinspection import ReinspectionMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)

REINSPECTION_FILENAME = "REINSPECTION_FORM.XML"
GRD_FILENAME_SUFFIX = "_GENERIC_ROUGHDRAFT.XML"


def _opt_str(value: str | None) -> str | None:
    """Return stripped string, or None if empty/missing."""
    return value.strip() if value else None


def _opt_int(value: str | None) -> int | None:
    """Parse integer, or None if empty/missing/invalid."""
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _opt_float(value: str | None) -> float | None:
    """Parse float, or None if empty/missing/invalid."""
    if not value:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


@register_handler
class EstimatePackageXmlHandler(DownloadFileHandler):
    """Parses XML attachments from estimatePackageReceived events.

    Parses REINSPECTION_FORM.XML to extract structured reinspection data.
    Other XML attachments for this subtype are silently passed through
    with no parsed data.
    """

    status_subtypes = ["estimatePackageReceived"]
    file_types = ["xml"]

    async def handle(
        self,
        task: DownloadTaskMessage,
        file_path: Path,
    ) -> FileHandlerResult:
        attachment_filename = file_path.name.upper()

        logger.info(
            "Running download file handler",
            extra={
                "handler_name": self.name,
                "attachment_filename": file_path.name,
                **extract_log_context(task),
            },
        )

        try:
            if attachment_filename.endswith("REINSPECTION_FORM.XML"):
                parsed_data = await asyncio.to_thread(
                    _parse_reinspection_form_sync, file_path
                )
                from datetime import UTC, datetime
                side_effects = [
                    FileHandlerSideEffect(
                        topic_key="reinspections",
                        message=ReinspectionMessage.from_handler_data(
                            task_message=task,
                            parsed_data={**parsed_data, "blob_url": f"{task.blob_path}/{file_path.name}"},
                            produced_at=datetime.now(UTC),
                        ),
                    ),
                ]
            elif attachment_filename.endswith(GRD_FILENAME_SUFFIX):
                parsed_data = _parse_grd_filename(file_path.name)
                parsed_data["generic_roughdraft_data"] = await asyncio.to_thread(
                    file_path.read_text, encoding="utf-8"
                )
                from datetime import UTC, datetime
                side_effects = [
                    FileHandlerSideEffect(
                        topic_key="verisk_grd",
                        message=GrdMessage.from_handler_data(
                            task_message=task,
                            parsed_data={**parsed_data, "blob_url": f"{task.blob_path}/{file_path.name}"},
                            produced_at=datetime.now(UTC),
                        ),
                    ),
                ]
            else:
                # No specific parser for this XML file; nothing to extract.
                parsed_data = {}
                side_effects = []

            logger.info(
                "Download file handler complete",
                extra={
                    "handler_name": self.name,
                    "attachment_filename": file_path.name,
                    "parsed_fields": list(parsed_data.keys()),
                    **extract_log_context(task),
                },
            )
            return FileHandlerResult(
                success=True,
                parsed_data={**parsed_data, "blob_url": f"{task.blob_path}/{file_path.name}"},
                side_effects=side_effects,
            )

        except Exception as e:
            logger.error(
                "Download file handler failed",
                extra={
                    "handler_name": self.name,
                    "attachment_filename": file_path.name,
                    "error": str(e),
                    **extract_log_context(task),
                },
                exc_info=True,
            )
            return FileHandlerResult(success=False, error=str(e))


def _parse_reinspection_form_sync(file_path: Path) -> dict:
    """Parse REINSPECTION_FORM.XML synchronously and return extracted fields.

    Fields are snake_cased. The full XML payload is stored as a string
    under ``reinspection_form_data`` for raw downstream consumption.
    """
    tree = ElementTree.parse(file_path)  # noqa: S314 - file is locally cached, not from network
    root = tree.getroot()  # <XACTDOC>

    result: dict = {"is_reinspection": True}

    # --- XACTNET_INFO --------------------------------------------------------
    xact_info = root.find("XACTNET_INFO")
    if xact_info is not None:
        a = xact_info.attrib
        result.update({
            "transaction_id":         _opt_str(a.get("transactionId")),
            "claim_number":           _opt_str(a.get("assignmentId")),
            "assignment_type":        _opt_str(a.get("assignmentType")),
            "recipients_xn_address":  _opt_str(a.get("recipientsXNAddress")),
            "recipients_xm8_user_id": _opt_str(a.get("recipientsXM8UserId") or a.get("recipientsXm8UserId")),
            "recipients_name":        _opt_str(a.get("recipientsName")),
            "dataset":                _opt_str(a.get("carrierId")),
            "job_type":               _opt_str(a.get("rotationTrade")),
            "office_description_1":   _opt_str(a.get("officeDescription1")),
            "carrier_org_level_1":    _opt_str(a.get("carrierOrgLevel1")),
            "carrier_org_level_2":    _opt_str(a.get("carrierOrgLevel2")),
            "carrier_org_level_3":    _opt_str(a.get("carrierOrgLevel3")),
            "carrier_org_level_4":    _opt_str(a.get("carrierOrgLevel4")),
            "carrier_org_level_5":    _opt_str(a.get("carrierOrgLevel5")),
            "carrier_org_level_6":    _opt_str(a.get("carrierOrgLevel6")),
        })

    # --- ADM / COVERAGE_LOSS -------------------------------------------------
    coverage_loss = root.find(".//COVERAGE_LOSS")
    if coverage_loss is not None:
        result["cat_code"] = _opt_str(coverage_loss.attrib.get("catastrophe"))
        tol = coverage_loss.find("TOL")
        if tol is not None:
            result["type_of_loss"] = _opt_str(tol.attrib.get("code"))

    # --- REINSPECTION_INFO / FILTERED_TOTALS ---------------------------------
    reinspection_info = root.find(".//REINSPECTION_INFO")
    if reinspection_info is not None:
        result["estimate_version"] = _opt_int(reinspection_info.attrib.get("reinspectedEstimateCount"))

    filtered_totals = reinspection_info.find("FILTERED_TOTALS") if reinspection_info is not None else None
    if filtered_totals is None:
        filtered_totals = root.find(".//FILTERED_TOTALS")
    if filtered_totals is not None:
        ft = filtered_totals.attrib
        result.update({
            "filtered_overwrite_count":    _opt_int(ft.get("filteredOverwriteCount")),
            "filtered_underwrite_count":   _opt_int(ft.get("filteredUnderwriteCount")),
            "filtered_overhead_acv":       _opt_float(ft.get("filteredOverheadAcv")),
            "filtered_overhead_dep":       _opt_float(ft.get("filteredOverheadDep")),
            "filtered_overhead_rcv":       _opt_float(ft.get("filteredOverheadRcv")),
            "filtered_overhead_reason":    _opt_str(ft.get("filteredOverheadReason")),
            "filtered_profit_acv":         _opt_float(ft.get("filteredProfitAcv")),
            "filtered_profit_dep":         _opt_float(ft.get("filteredProfitDep")),
            "filtered_profit_rcv":         _opt_float(ft.get("filteredProfitRcv")),
            "filtered_profit_reason":      _opt_str(ft.get("filteredProfitReason")),
            "filtered_over_acv":           _opt_float(ft.get("filteredOverAcv")),
            "filtered_over_dep":           _opt_float(ft.get("filteredOverDep")),
            "filtered_over_rcv":           _opt_float(ft.get("filteredOverRcv")),
            "filtered_under_acv":          _opt_float(ft.get("filteredUnderAcv")),
            "filtered_under_dep":          _opt_float(ft.get("filteredUnderDep")),
            "filtered_under_rcv":          _opt_float(ft.get("filteredUnderRcv")),
            "filtered_total_acv":          _opt_float(ft.get("filteredTotalAcv")),
            "filtered_total_dep":          _opt_float(ft.get("filteredTotalDep")),
            "filtered_total_rcv":          _opt_float(ft.get("filteredTotalRcv")),
            "filtered_orig_acv":           _opt_float(ft.get("filteredOrigAcv")),
            "filtered_orig_dep":           _opt_float(ft.get("filteredOrigDep")),
            "filtered_orig_rcv":           _opt_float(ft.get("filteredOrigRcv")),
            "filtered_error_percent_acv":  _opt_float(ft.get("filteredErrorPercentAcv")),
            "filtered_error_percent_dep":  _opt_float(ft.get("filteredErrorPercentDep")),
            "filtered_error_percent_rcv":  _opt_float(ft.get("filteredErrorPercentRcv")),
        })

    # --- Full raw payload ----------------------------------------------------
    result["reinspection_form_data"] = ElementTree.tostring(root, encoding="unicode")

    return result


def _parse_grd_filename(filename: str) -> dict:
    """Extract assignment_id and version from a GENERIC_ROUGHDRAFT filename.

    Expected format: ``{assignment_id}_{version}_GENERIC_ROUGHDRAFT.xml``
    or ``{assignment_id}_{version}__GENERIC_ROUGHDRAFT.xml`` (double underscore).

    Example: ``055379P_2__GENERIC_ROUGHDRAFT.xml`` → assignment_id=055379P, version=2
    """
    # Strip the suffix to get the prefix part
    name_upper = filename.upper()
    prefix = filename[: name_upper.index("_GENERIC_ROUGHDRAFT")]

    # Strip trailing underscores (handles double-underscore separator)
    prefix = prefix.rstrip("_")

    # Split on last underscore to separate assignment_id and version
    if "_" in prefix:
        assignment_id, version = prefix.rsplit("_", 1)
    else:
        assignment_id = prefix
        version = "0"

    return {"assignment_id": assignment_id, "version": version}


