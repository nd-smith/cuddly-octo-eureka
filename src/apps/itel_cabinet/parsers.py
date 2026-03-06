"""
Form parsing for iTel Cabinet Repair forms.

Extracts structured data from ClaimX task responses using the DataBuilder pattern.
Integrated from parse.py with improved column mapping and value extraction.
"""

import json
import logging
import math
import re
import types
from collections import defaultdict
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import UTC, datetime
from typing import Union
from .models import CabinetAttachment, CabinetSubmission

logger = logging.getLogger(__name__)


# ==========================================
# UTILITY FUNCTIONS
# ==========================================


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def parse_date(date_str: str) -> datetime | None:
    """Parse ISO format date string to datetime."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def _convert_field_value(field_type, value):
    """Convert a single field value to its target type, handling lists and optionals."""
    origin = getattr(field_type, "__origin__", None)
    args = getattr(field_type, "__args__", [])
    if origin is list:
        item_type = args[0]
        return [from_dict(item_type, item) for item in value]
    if (origin is Union or isinstance(field_type, types.UnionType)) and type(None) in args:
        non_none_type = next(t for t in args if t is not type(None))
        return from_dict(non_none_type, value)
    return from_dict(field_type, value)


def from_dict(data_class, data):
    """
    Recursively convert dict to dataclass instance.
    Handles camelCase to snake_case conversion.
    Float values assigned to int fields are rounded up (ceiling).
    """
    if data is None:
        return None
    if data_class is datetime:
        return parse_date(data)
    if data_class is int and isinstance(data, float):
        return round(data)
    if not is_dataclass(data_class):
        return data

    field_types = {f.name: f.type for f in fields(data_class)}
    kwargs = {}
    for key, value in data.items():
        snake_key = camel_to_snake(key)
        if snake_key in field_types:
            kwargs[snake_key] = _convert_field_value(field_types[snake_key], value)
    return data_class(**kwargs)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


# ==========================================
# SCHEMA CLASSES (from parse.py)
# ==========================================


@dataclass
class OptionAnswer:
    name: str


@dataclass
class NumberAnswer:
    value: float


@dataclass
class ResponseAnswerExport:
    type: str
    text: str | None = None
    option_answer: OptionAnswer | None = None
    number_answer: NumberAnswer | None = None
    claim_media_ids: list[int] | None = None


@dataclass
class FormControl:
    id: str


@dataclass
class QuestionAndAnswer:
    question_text: str
    component: str
    response_answer_export: ResponseAnswerExport
    form_control: FormControl


@dataclass
class Group:
    name: str
    group_id: str
    question_and_answers: list[QuestionAndAnswer] = field(default_factory=list)


@dataclass
class ResponseData:
    groups: list[Group] = field(default_factory=list)


@dataclass
class ExternalLinkData:
    url: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None


@dataclass
class ApiResponse:
    assignment_id: int
    task_id: int
    task_name: str
    project_id: int
    form_id: str
    status: str
    assignor_email: str | None = None
    form_response_id: str | None = None
    date_assigned: datetime | None = None
    date_completed: datetime | None = None
    external_link_data: ExternalLinkData | None = None
    response: ResponseData | None = None


# ==========================================
# COLUMN MAPPING CONFIGURATION
# ==========================================

# Maps (group_name, question_text) tuples to database column names
# Keys are stripped of trailing spaces to ensure matching works
COLUMN_MAP = {
    # Measurements
    ("Linear Feet Capture", "Countertops (linear feet)"): "countertops_lf",
    ("Linear Feet Capture", "Lower Cabinets (linear feet)"): "lower_cabinets_lf",
    ("Linear Feet Capture", "Upper Cabinets (linear feet)"): "upper_cabinets_lf",
    (
        "Linear Feet Capture",
        "Full-Height Cabinets (linear feet)",
    ): "full_height_cabinets_lf",
    ("Linear Feet Capture", "Island Cabinets (linear feet)"): "island_cabinets_lf",
    # Lower Cabinets
    ("Cabinet Types Damaged", "Lower Cabinets Damaged?"): "lower_cabinets_damaged",
    (
        "Lower Cabinets",
        "Enter Number of Damaged Lower Boxes",
    ): "num_damaged_lower_boxes",
    ("Lower Cabinets", "Are The Lower Cabinets Detached?"): "lower_cabinets_detached",
    (
        "Lower Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "lower_face_frames_doors_drawers_available",
    (
        "Lower Cabinets",
        "Are there damages to the face frames, doors, and/or drawer fronts?",
    ): "lower_face_frames_doors_drawers_damaged",
    (
        "Lower Cabinets",
        "Are The Lower Finished End Panels Damaged?",
    ): "lower_finished_end_panels_damaged",
    (
        "Capture Lower End Panel",
        "Is there lower cabinet end panel damage?",
    ): "lower_end_panel_damage_present",
    (
        "Lower Cabinet Counter Type",
        "Select Lower Cabinet Counter Type",
    ): "lower_counter_type",
    # Upper Cabinets
    ("Cabinet Types Damaged", "Upper Cabinets Damaged?"): "upper_cabinets_damaged",
    (
        "Upper Cabinets",
        "Enter Number of Damaged Upper Boxes",
    ): "num_damaged_upper_boxes",
    ("Upper Cabinets", "Are The Upper Cabinets Detached?"): "upper_cabinets_detached",
    (
        "Upper Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "upper_face_frames_doors_drawers_available",
    (
        "Upper Cabinets",
        "Are there damages to the face frames, doors, and/or drawer fronts?",
    ): "upper_face_frames_doors_drawers_damaged",
    (
        "Upper Cabinets",
        "Are The Upper Finished End Panels Damaged?",
    ): "upper_finished_end_panels_damaged",
    (
        "Capture Upper End Panel",
        "Is there upper cabinet end panel damage?",
    ): "upper_end_panel_damage_present",
    # Full Height
    (
        "Cabinet Types Damaged",
        "Full Height Cabinets Damaged?",
    ): "full_height_cabinets_damaged",
    (
        "Full Height/Pantry Cabinets",
        "Enter Number of Damaged Full Height Boxes",
    ): "num_damaged_full_height_boxes",
    (
        "Full Height/Pantry Cabinets",
        "Are The Full Height Cabinets Detached?",
    ): "full_height_cabinets_detached",
    (
        "Full Height/Pantry Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "full_height_face_frames_doors_drawers_available",
    (
        "Full Height/Pantry Cabinets",
        "Are there damages to the face frames, doors, and/or drawer fronts?",
    ): "full_height_face_frames_doors_drawers_damaged",
    (
        "Full Height/Pantry Cabinets",
        "Are the Full Height Finished End Panels Damaged?",
    ): "full_height_finished_end_panels_damaged",
    # Island
    ("Cabinet Types Damaged", "Island Cabinets Damaged?"): "island_cabinets_damaged",
    (
        "Island Cabinets",
        "Enter Number of Damaged Island Boxes",
    ): "num_damaged_island_boxes",
    (
        "Island Cabinets",
        "Are The Island Cabinets Detached?",
    ): "island_cabinets_detached",
    (
        "Island Cabinets",
        "Are All The Face Frames, Doors, And Drawers Available?",
    ): "island_face_frames_doors_drawers_available",
    (
        "Island Cabinets",
        "Are there damages to the face frames, doors, and/or drawer fronts?",
    ): "island_face_frames_doors_drawers_damaged",
    (
        "Island Cabinets",
        "Are the Island Finished End Panels Damaged?",
    ): "island_finished_end_panels_damaged",
    (
        "Capture Island End Panel",
        "Is there island cabinet end panel damage?",
    ): "island_end_panel_damage_present",
    (
        "Island Cabinet Counter Type",
        "Select Island Cabinet Counter Type",
    ): "island_counter_type",
    # General
    ("Damage Description", "Enter Damaged Description"): "damage_description",
    (
        "Other Details and Information",
        "Now that you have been through the process, please provide any other details that you think would be relevant to the cabinet damage",
    ): "additional_notes",
    (
        "Other Details and Information",
        "Additional details/information",
    ): "additional_notes",
}


# ==========================================
# DATA BUILDER
# ==========================================


class DataBuilder:
    """
    Core parsing logic for transforming ClaimX API response into structured data.
    Produces three outputs: form_row (Delta), attachments_rows (Delta), readable_report (API).
    """

    # Fields that must be stored as "yes"/"no" strings (not booleans) to match the Delta schema.
    # These are "available?" questions whose option answers go through _normalize_boolean and come
    # out as bool; we convert them back to strings before writing.
    _AVAILABLE_FIELDS = {
        "lower_face_frames_doors_drawers_available",
        "upper_face_frames_doors_drawers_available",
        "full_height_face_frames_doors_drawers_available",
        "island_face_frames_doors_drawers_available",
    }

    @staticmethod
    def _extract_raw_value(export_data: ResponseAnswerExport):
        """Extract the raw value from a response answer by type."""
        if export_data.type == "text":
            return export_data.text
        if export_data.type == "number":
            return export_data.number_answer.value if export_data.number_answer else None
        if export_data.type == "option":
            return export_data.option_answer.name if export_data.option_answer else None
        if export_data.type == "image":
            return export_data.claim_media_ids if export_data.claim_media_ids else []
        return None

    @staticmethod
    def _normalize_boolean(val: str):
        """Normalize Yes/No string variants to True/False, or return None if not boolean."""
        clean = val.lower().strip()
        if clean == "yes" or clean.startswith("yes,"):
            return True
        if clean == "no" or clean.startswith("there is no") or clean == "not available":
            return False
        return None

    @staticmethod
    def extract_value(export_data: ResponseAnswerExport):
        """
        Extracts and normalizes value from response answer.
        Handles Yes/No -> True/False conversion for boolean fields.
        """
        if not export_data:
            return None
        val = DataBuilder._extract_raw_value(export_data)
        if isinstance(val, str):
            normalized = DataBuilder._normalize_boolean(val)
            if normalized is not None:
                return normalized
        return val

    _TOPIC_KEYWORDS = [
        ("island", "Island Cabinets"),
        ("lower", "Lower Cabinets"),
        ("upper", "Upper Cabinets"),
        ("full height", "Full Height / Pantry"),
        ("pantry", "Full Height / Pantry"),
        ("countertop", "Countertops"),
    ]

    @staticmethod
    def get_topic_category(group_name: str, question_text: str) -> str:
        """Determine topic category for organizing attachments and readable reports."""
        text = (group_name + " " + question_text).lower()
        for keyword, category in DataBuilder._TOPIC_KEYWORDS:
            if keyword in text:
                return category
        return "General"

    @staticmethod
    def _extract_external_link_fields(ext_data: ExternalLinkData | None) -> dict:
        """Extract external link data fields, returning None for each if absent."""
        if not ext_data:
            return {
                "external_link_url": None,
                "customer_first_name": None,
                "customer_last_name": None,
                "customer_email": None,
                "customer_phone": None,
            }
        return {
            "external_link_url": ext_data.url,
            "customer_first_name": ext_data.first_name,
            "customer_last_name": ext_data.last_name,
            "customer_email": ext_data.email,
            "customer_phone": ext_data.phone,
        }

    @staticmethod
    def _process_response_groups(groups: list[Group]) -> tuple[dict, dict]:
        """Process response groups, returning (column_values, raw_data_flat)."""
        column_values = {}
        raw_data_flat = defaultdict(list)
        for group in groups:
            clean_group_name = group.name.strip()
            for qa in group.question_and_answers:
                clean_question_text = qa.question_text.strip()
                answer_val = DataBuilder.extract_value(qa.response_answer_export)
                map_key = (clean_group_name, clean_question_text)
                if map_key in COLUMN_MAP:
                    column_values[COLUMN_MAP[map_key]] = answer_val
                raw_data_flat[clean_group_name].append(
                    {"q": clean_question_text, "a": answer_val, "id": qa.form_control.id}
                )
        return column_values, dict(raw_data_flat)

    @staticmethod
    def build_form_row(api_obj: ApiResponse, event_id: str) -> dict:
        """Build form row for database insertion."""
        if not api_obj:
            return {}

        now = datetime.now(UTC)
        form_row = {
            "assignment_id": api_obj.assignment_id,
            "task_id": api_obj.task_id,
            "task_name": api_obj.task_name,
            "project_id": str(api_obj.project_id),
            "form_id": api_obj.form_id,
            "form_response_id": api_obj.form_response_id,
            "status": api_obj.status,
            "event_id": event_id,
            "date_assigned": api_obj.date_assigned,
            "date_completed": api_obj.date_completed,
            "ingested_at": now,
            "assignor_email": api_obj.assignor_email,
            **DataBuilder._extract_external_link_fields(api_obj.external_link_data),
            **dict.fromkeys(COLUMN_MAP.values()),
        }

        groups = api_obj.response.groups if api_obj.response and api_obj.response.groups else []
        column_values, raw_data_flat = DataBuilder._process_response_groups(groups)
        form_row.update(column_values)

        # Ensure *_available fields are stored as "yes"/"no" strings (not booleans) to match the
        # Delta table schema.  _normalize_boolean converts Yes/No/Not Available → True/False/False;
        # we convert those booleans back to lowercase strings here.
        for field in DataBuilder._AVAILABLE_FIELDS:
            val = form_row.get(field)
            if isinstance(val, bool):
                form_row[field] = "yes" if val else "no"

        form_row["raw_data"] = json.dumps(raw_data_flat, cls=DateTimeEncoder)
        form_row["created_at"] = now
        form_row["updated_at"] = now
        return form_row

    @staticmethod
    def _build_attachment_rows(
        api_obj: ApiResponse,
        qa: QuestionAndAnswer,
        media_ids: list,
        topic: str,
        event_id: str,
        now: datetime,
        media_url_map: dict[int, str],
    ) -> list[dict]:
        """Build attachment row dicts for a single image question."""
        clean_question_text = qa.question_text.strip()
        return [
            {
                "assignment_id": api_obj.assignment_id,
                "project_id": api_obj.project_id,
                "event_id": event_id,
                "control_id": qa.form_control.id,
                "question_key": _get_question_key(clean_question_text),
                "question_text": clean_question_text,
                "topic_category": topic,
                "media_id": media_id,
                "display_order": idx + 1,
                "created_at": now,
                "is_active": True,
                "media_type": "image/jpeg",
                "url": media_url_map.get(media_id),
            }
            for idx, media_id in enumerate(media_ids)
        ]

    @staticmethod
    def extract_attachments(
        api_obj: ApiResponse, event_id: str, media_url_map: dict[int, str] = None
    ) -> list[dict]:
        """Extract attachment rows from API response."""
        if media_url_map is None:
            media_url_map = {}
        if not api_obj:
            return []

        now = datetime.now(UTC)
        attachments_rows = []
        groups = api_obj.response.groups if api_obj.response and api_obj.response.groups else []

        for group in groups:
            clean_group_name = group.name.strip()
            for qa in group.question_and_answers:
                answer_val = DataBuilder.extract_value(qa.response_answer_export)
                if qa.response_answer_export.type != "image" or not isinstance(answer_val, list):
                    continue
                topic = DataBuilder.get_topic_category(clean_group_name, qa.question_text.strip())
                attachments_rows.extend(
                    DataBuilder._build_attachment_rows(
                        api_obj, qa, answer_val, topic, event_id, now, media_url_map
                    )
                )

        return attachments_rows

    @staticmethod
    def _build_report_meta(api_obj: ApiResponse) -> dict:
        """Build the meta section for a readable report."""
        return {
            "task_id": api_obj.task_id,
            "assignment_id": api_obj.assignment_id,
            "project_id": str(api_obj.project_id),
            "status": api_obj.status,
            "dates": {
                "assigned": api_obj.date_assigned.isoformat() if api_obj.date_assigned else None,
                "completed": (
                    api_obj.date_completed.isoformat() if api_obj.date_completed else None
                ),
            },
        }

    @staticmethod
    def _enrich_answer(answer_val, export_type: str, media_url_map: dict[int, str]):
        """Enrich image answers with URLs, pass through others."""
        if export_type == "image" and isinstance(answer_val, list):
            return [
                {"media_id": media_id, "url": media_url_map.get(media_id)}
                for media_id in answer_val
            ]
        return answer_val

    @staticmethod
    def build_readable_report(
        api_obj: ApiResponse, _event_id: str, media_url_map: dict[int, str] = None
    ) -> dict:
        """Build readable report with categorized topics."""
        if media_url_map is None:
            media_url_map = {}
        if not api_obj:
            return {}

        topics = defaultdict(list)
        groups = api_obj.response.groups if api_obj.response and api_obj.response.groups else []

        for group in groups:
            clean_group_name = group.name.strip()
            for qa in group.question_and_answers:
                clean_question_text = qa.question_text.strip()
                answer_val = DataBuilder.extract_value(qa.response_answer_export)
                topic = DataBuilder.get_topic_category(clean_group_name, clean_question_text)
                topics[topic].append({
                    "question": clean_question_text,
                    "answer": DataBuilder._enrich_answer(
                        answer_val, qa.response_answer_export.type, media_url_map
                    ),
                    "type": qa.response_answer_export.type,
                    "control_id": qa.form_control.id,
                })

        return {
            "meta": DataBuilder._build_report_meta(api_obj),
            "topics": dict(topics),
        }


# ==========================================
# HELPER FUNCTIONS
# ==========================================


def _get_question_key(question_text: str) -> str:
    """
    Generate question_key from question text.
    Maps known media questions to consistent keys.
    """
    # Known media question mappings
    media_mapping = {
        "Upload Overview Photo(s)": "overview_photos",
        "Captured Lower Cabinet Box": "lower_cabinet_box",
        "Captured Lower Face Frames, Doors, and Drawers": "lower_face_frames_doors_drawers",
        "Captured Lower Cabinet End Panels": "lower_cabinet_end_panels",
        "Captured Upper Cabinet Box": "upper_cabinet_box",
        "Captured Upper Face Frames, Doors, and Drawers": "upper_face_frames_doors_drawers",
        "Captured Upper Cabinet End Panels": "upper_cabinet_end_panels",
        "Captured Full Height/Pantry Cabinets": "full_height_cabinet_box",
        "Captured Full Height/Pantry Face Frames, Doors, and Drawers": "full_height_face_frames_doors_drawers",
        "Captured Full Height/Pantry End Panels": "full_height_end_panels",
        "Captured Island Cabinet Box": "island_cabinet_box",
        "Captured Island Face Frames, Doors, and Drawers": "island_face_frames_doors_drawers",
        "Captured Island Cabinet End Panels": "island_cabinet_end_panels",
    }

    if question_text in media_mapping:
        return media_mapping[question_text]

    return question_text.lower().replace(" ", "_").replace("/", "_").replace("?", "")


# ==========================================
# PUBLIC API FUNCTIONS
# ==========================================


def parse_task_data(task_data: dict) -> ApiResponse:
    """Parse raw ClaimX task dict into typed ApiResponse."""
    return from_dict(ApiResponse, task_data)


def parse_cabinet_form(api_obj: ApiResponse, event_id: str) -> CabinetSubmission:
    """
    Parse ClaimX API response into CabinetSubmission.

    Args:
        api_obj: Parsed API response
        event_id: Event ID for traceability

    Returns:
        Parsed submission data
    """
    logger.debug("Parsing cabinet form for assignment_id=%s", api_obj.assignment_id)
    form_row = DataBuilder.build_form_row(api_obj, event_id)
    return CabinetSubmission(**form_row)


def parse_cabinet_attachments(
    api_obj: ApiResponse,
    event_id: str,
    media_url_map: dict[int, str] = None,
) -> list[CabinetAttachment]:
    """
    Extract media attachments from ClaimX API response.

    Args:
        api_obj: Parsed API response
        event_id: Event ID for traceability
        media_url_map: Optional mapping of media_id to download URL

    Returns:
        List of attachment records with URLs enriched
    """
    logger.debug("Parsing attachments for assignment_id=%s", api_obj.assignment_id)
    attachment_rows = DataBuilder.extract_attachments(api_obj, event_id, media_url_map)
    attachments = [CabinetAttachment(**row) for row in attachment_rows]
    logger.debug("Parsed %s attachments", len(attachments))
    return attachments


def get_readable_report(
    api_obj: ApiResponse, event_id: str, media_url_map: dict[int, str] = None
) -> dict:
    """
    Generate readable report for API consumption.

    Args:
        api_obj: Parsed API response
        event_id: Event ID for traceability
        media_url_map: Optional mapping of media_id to download URL

    Returns:
        Readable report dict with topics organized by category and media URLs enriched
    """
    logger.debug("Generating readable report for assignment_id=%s", api_obj.assignment_id)
    return DataBuilder.build_readable_report(api_obj, event_id, media_url_map)
