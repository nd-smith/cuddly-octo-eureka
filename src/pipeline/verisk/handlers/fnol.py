"""Handler for firstNoticeOfLossReceived XML attachments.

Handles: firstNoticeOfLossReceived + xml
Parses FNOL_XACTDOC.XML and extracts structured fields plus the
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
from pipeline.verisk.handlers.contentspal_router import build_contentspal_side_effect
from pipeline.verisk.schemas.fnol import FnolMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)

FNOL_FILENAME_SUFFIX = "_FNOL_XACTDOC.XML"


def _opt_str(value: str | None) -> str | None:
    """Return stripped string, or None if empty/missing."""
    return value.strip() if value else None


@register_handler
class FnolXactdocXmlHandler(DownloadFileHandler):
    """Parses XML attachments from firstNoticeOfLossReceived events.

    Parses FNOL_XACTDOC.XML to extract structured FNOL data.
    Other XML attachments for this subtype are silently passed through
    with no parsed data.
    """

    status_subtypes = ["firstNoticeOfLossReceived"]
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
            if attachment_filename.endswith(FNOL_FILENAME_SUFFIX):
                parsed_data = await asyncio.to_thread(
                    _parse_fnol_xactdoc_sync, file_path
                )
                merged = {**parsed_data, "blob_url": f"{task.blob_path}/{file_path.name}"}
                from datetime import UTC, datetime
                side_effects = [
                    FileHandlerSideEffect(
                        topic_key="fnol",
                        message=FnolMessage.from_handler_data(
                            task_message=task,
                            parsed_data=merged,
                            produced_at=datetime.now(UTC),
                        ),
                    ),
                ]

                contentspal_se = build_contentspal_side_effect(task, merged)
                if contentspal_se is not None:
                    side_effects.append(contentspal_se)
            else:
                # No specific parser for this XML file; nothing to extract.
                parsed_data = {}
                merged = {}
                side_effects = []

            logger.info(
                "Download file handler complete",
                extra={
                    "handler_name": self.name,
                    "attachment_filename": file_path.name,
                    "parsed_fields": list(parsed_data.keys()),
                    "has_side_effect": len(side_effects) > 0,
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


def _parse_fnol_xactdoc_sync(file_path: Path) -> dict:
    """Parse FNOL_XACTDOC.XML synchronously and return extracted fields.

    Fields are snake_cased. The full XML payload is stored as a string
    under ``fnol_xactdoc_data`` for raw downstream consumption.
    """
    tree = ElementTree.parse(file_path)  # noqa: S314 - file is locally cached, not from network
    root = tree.getroot()  # <XACTDOC>

    result: dict = {}

    # --- XACTNET_INFO --------------------------------------------------------
    xact_info = root.find("XACTNET_INFO")
    if xact_info is not None:
        a = xact_info.attrib
        result.update({
            "transaction_id":          _opt_str(a.get("transactionId")),
            "original_transaction_id": _opt_str(a.get("originalTransactionId")),
            "assignment_type":         _opt_str(a.get("assignmentType")),
            "business_unit":           _opt_str(a.get("businessUnit")),
            "dataset":                 _opt_str(a.get("carrierId")),
            "job_type":                _opt_str(a.get("rotationTrade")),
            "senders_xn_address":      _opt_str(a.get("sendersXNAddress")),
            "recipients_xn_address":   _opt_str(a.get("recipientsXNAddress")),
        })

    # --- ADM -----------------------------------------------------------------
    adm = root.find("ADM")
    if adm is not None:
        result.update({
            "date_of_loss":  _opt_str(adm.attrib.get("dateOfLoss")),
            "date_received": _opt_str(adm.attrib.get("dateReceived")),
        })

        # --- COVERAGE_LOSS ---------------------------------------------------
        coverage_loss = adm.find("COVERAGE_LOSS")
        if coverage_loss is not None:
            cl = coverage_loss.attrib
            result.update({
                "claim_number":  _opt_str(cl.get("claimNumber")),
                "date_init_cov": _opt_str(cl.get("dateInitCov")),
                "policy_number": _opt_str(cl.get("policyNumber")),
            })

            tol = coverage_loss.find("TOL")
            if tol is not None:
                result.update({
                    "type_of_loss":      _opt_str(tol.attrib.get("code")),
                    "type_of_loss_desc": _opt_str(tol.attrib.get("desc")),
                })

    # --- CONTACTS ------------------------------------------------------------
    for contact in root.findall(".//CONTACTS/CONTACT"):
        if contact.attrib.get("type") == "Client":
            result["insured_name"] = _opt_str(contact.attrib.get("name"))
            email = contact.find("CONTACTMETHODS/EMAIL")
            if email is not None:
                result["insured_email"] = _opt_str(email.attrib.get("address"))
            # Prefer address types in priority order; fall back to first available
            addresses = contact.findall("ADDRESSES/ADDRESS")
            address_by_type = {a.attrib.get("type"): a for a in addresses}
            address = next(
                (address_by_type[t] for t in ("Property", "Home", "Mailing") if t in address_by_type),
                addresses[0] if addresses else None,
            )
            if address is not None:
                result.update({
                    "insured_street": _opt_str(address.attrib.get("street")),
                    "insured_city":   _opt_str(address.attrib.get("city")),
                    "insured_state":  _opt_str(address.attrib.get("state")),
                    "insured_postal": _opt_str(address.attrib.get("postal")),
                })
            break

    # --- Full raw payload ----------------------------------------------------
    result["fnol_xactdoc_data"] = ElementTree.tostring(root, encoding="unicode")

    return result
