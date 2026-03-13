"""Parse functions extracted from Verisk download file handlers.

Each parser is a plain function that takes a file path (or task) and returns
a dict of parsed fields. The PARSERS lookup maps (status_subtype, file_type,
filename_pattern) to the appropriate parser.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from pipeline.verisk.schemas.tasks import XACTEnrichmentTask

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# FNOL XACTDOC parser
# ---------------------------------------------------------------------------

FNOL_FILENAME_SUFFIX = "_FNOL_XACTDOC.XML"


def _parse_fnol_xactdoc_sync(file_path: Path) -> dict[str, Any]:
    """Parse FNOL_XACTDOC.XML synchronously and return extracted fields.

    Fields are snake_cased. The full XML payload is stored as a string
    under ``fnol_xactdoc_data`` for raw downstream consumption.
    """
    tree = ElementTree.parse(file_path)  # noqa: S314 - file is locally cached, not from network
    root = tree.getroot()  # <XACTDOC>

    result: dict[str, Any] = {}

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


async def parse_fnol_xactdoc(file_path: Path) -> dict[str, Any]:
    """Async wrapper for FNOL XACTDOC XML parsing."""
    return await asyncio.to_thread(_parse_fnol_xactdoc_sync, file_path)


# ---------------------------------------------------------------------------
# Reinspection form parser
# ---------------------------------------------------------------------------

REINSPECTION_FILENAME = "REINSPECTION_FORM.XML"


def _parse_reinspection_form_sync(file_path: Path) -> dict[str, Any]:
    """Parse REINSPECTION_FORM.XML synchronously and return extracted fields.

    Fields are snake_cased. The full XML payload is stored as a string
    under ``reinspection_form_data`` for raw downstream consumption.
    """
    tree = ElementTree.parse(file_path)  # noqa: S314 - file is locally cached, not from network
    root = tree.getroot()  # <XACTDOC>

    result: dict[str, Any] = {"is_reinspection": True}

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


async def parse_reinspection_form(file_path: Path) -> dict[str, Any]:
    """Async wrapper for reinspection form XML parsing."""
    return await asyncio.to_thread(_parse_reinspection_form_sync, file_path)


# ---------------------------------------------------------------------------
# GRD filename parser
# ---------------------------------------------------------------------------

GRD_FILENAME_SUFFIX = "_GENERIC_ROUGHDRAFT.XML"


def parse_grd_filename(filename: str) -> dict[str, Any]:
    """Extract assignment_id and version from a GENERIC_ROUGHDRAFT filename.

    Expected format: ``{assignment_id}_{version}_GENERIC_ROUGHDRAFT.xml``
    or ``{assignment_id}_{version}__GENERIC_ROUGHDRAFT.xml`` (double underscore).

    Example: ``055379P_2__GENERIC_ROUGHDRAFT.xml`` -> assignment_id=055379P, version=2
    """
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


async def parse_grd(file_path: Path) -> dict[str, Any]:
    """Parse GRD: extract filename metadata + read raw XML content."""
    parsed = parse_grd_filename(file_path.name)
    parsed["generic_roughdraft_data"] = await asyncio.to_thread(
        file_path.read_text, encoding="utf-8"
    )
    return parsed


# ---------------------------------------------------------------------------
# Event JSON parser (for status-only events)
# ---------------------------------------------------------------------------

def parse_event_json(task: XACTEnrichmentTask) -> dict[str, Any]:
    """Parse JSON data payload from a status-only event."""
    raw_data = task.raw_event.get("data", "{}")
    data: dict[str, Any] = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
    return data
