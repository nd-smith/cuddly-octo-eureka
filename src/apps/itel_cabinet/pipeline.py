"""iTel Cabinet Processing Pipeline."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pipeline.common.connections import (
    ConnectionManager,
    is_http_error,
)

from .models import CabinetAttachment, CabinetSubmission, ProcessedTask, TaskEvent
from .parsers import (
    get_readable_report,
    parse_cabinet_attachments,
    parse_cabinet_form,
    parse_task_data,
)

if TYPE_CHECKING:
    from .api_sender import ItelApiSender

logger = logging.getLogger(__name__)


class ItelCabinetPipeline:
    """Processing pipeline for iTel Cabinet task events.

    Responsibilities:
    1. Parse and validate incoming events
    2. Enrich completed tasks with ClaimX data
    3. Write to Delta tables (always)
    4. Send completed tasks to iTel API via ItelApiSender
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        submissions_writer,  # ItelSubmissionsDeltaWriter
        attachments_writer,  # ItelAttachmentsDeltaWriter
        config: dict,
        api_sender: ItelApiSender | None = None,
    ):
        self.connections = connection_manager
        self.submissions_writer = submissions_writer
        self.attachments_writer = attachments_writer
        self.config = config
        self.claimx_connection = config.get("claimx_connection", "claimx_api")
        self.api_sender = api_sender

        logger.info(
            "ItelCabinetPipeline initialized",
            extra={
                "claimx_connection": self.claimx_connection,
                "api_sender_enabled": api_sender is not None,
            },
        )

    async def process(self, raw_message: dict) -> ProcessedTask:
        """Main processing flow:
        1. Parse and validate event
        2. Conditionally enrich (COMPLETED tasks only)
        3. Write to Delta (always)
        4. Send to iTel API via ItelApiSender (COMPLETED tasks only)
        """
        event = TaskEvent.from_message(raw_message)
        logger.info(
            "Processing iTel cabinet event",
            extra={
                "event_id": event.event_id,
                "assignment_id": event.assignment_id,
                "task_status": event.task_status,
            },
        )

        self._validate_event(event)
        if event.task_status == "COMPLETED":
            submission, attachments, readable_report, insured_info, adjuster_info, project_number = (
                await self._enrich_completed_task(event)
            )
        else:
            submission, attachments, readable_report, insured_info, adjuster_info, project_number = (
                None, [], None, {}, {}, ""
            )
            logger.debug(
                "Skipping enrichment for non-completed task",
                extra={"task_status": event.task_status},
            )

        await self._write_to_delta(event, submission, attachments)
        if event.task_status == "COMPLETED" and submission and self.api_sender:
            await self._submit_to_api(
                event, submission, attachments, readable_report, insured_info, adjuster_info,
                project_number,
            )

        return ProcessedTask(
            event=event,
            submission=submission,
            attachments=attachments,
            readable_report=readable_report,
        )

    def _validate_event(self, event: TaskEvent) -> None:
        """Validate task_status is a known value."""
        valid_statuses = ["ASSIGNED", "IN_PROGRESS", "COMPLETED"]
        if event.task_status not in valid_statuses:
            raise ValueError(
                f"Invalid task_status: {event.task_status}. Expected one of: {valid_statuses}"
            )

        logger.debug("Event validation passed")

    async def _enrich_completed_task(
        self,
        event: TaskEvent,
    ) -> tuple[CabinetSubmission, list[CabinetAttachment], dict, dict, dict, str]:
        """Fetch and parse completed task:
        1. Fetch task from ClaimX API
        2. Fetch project media for URL lookup
        3. Fetch project export for insured address
        4. Extract adjuster info from task response groups
        5. Parse form, attachments, and readable report
        """
        logger.info("Enriching completed task", extra={"assignment_id": event.assignment_id})

        task_data = await self._fetch_claimx_assignment(event.assignment_id)
        project_id = int(task_data.get("projectId", event.project_id))
        media_url_map = await self._fetch_project_media_urls(project_id, event.media_ids)

        # Fetch insured info + project number from project export
        project_export = await self._fetch_project_export(project_id)
        project_data = project_export.get("data", {}).get("project", {})
        insured_info = project_data.get("customerInformation", {})
        project_number = project_data.get("projectNumber", "")

        # Extract adjuster info from task response
        adjuster_info = self._extract_adjuster_info(task_data)

        api_obj = parse_task_data(task_data)

        submission = parse_cabinet_form(api_obj, event.event_id)
        attachments = parse_cabinet_attachments(api_obj, event.event_id, media_url_map)
        readable_report = get_readable_report(api_obj, event.event_id, media_url_map)

        logger.info(
            "Task enriched successfully",
            extra={
                "assignment_id": event.assignment_id,
                "attachment_count": len(attachments),
            },
        )

        return submission, attachments, readable_report, insured_info, adjuster_info, project_number

    ADJUSTER_GROUP_ID = "group-309019430"

    def _extract_adjuster_info(self, task_data: dict) -> dict:
        """Extract adjuster info from the task response group.

        Looks for group with groupId "group-309019430" and reads:
          - question 0: Vendor Name → adjuster_id
          - question 1: First Name → first_name
          - question 2: Last Name → last_name
          - question 3: Best Contact Number → phone
          - question 4: Best Contact Email → email
        """
        groups = task_data.get("response", {}).get("groups", [])
        adjuster_group = None
        for group in groups:
            if group.get("groupId") == self.ADJUSTER_GROUP_ID:
                adjuster_group = group
                break

        if not adjuster_group:
            logger.warning(
                "Adjuster group not found in task response",
                extra={"expected_group_id": self.ADJUSTER_GROUP_ID},
            )
            return {}

        questions = adjuster_group.get("questionAndAnswers", [])

        def _get_text(index: int) -> str:
            if index < len(questions):
                return questions[index].get("responseAnswerExport", {}).get("text") or ""
            return ""

        return {
            "adjuster_id": _get_text(0),
            "first_name": _get_text(1),
            "last_name": _get_text(2),
            "phone": _get_text(3),
            "email": _get_text(4),
        }

    async def _fetch_project_export(self, project_id: int) -> dict:
        """Fetch project export data from ClaimX API."""
        endpoint = f"/export/project/{project_id}"

        logger.debug("Fetching project export from ClaimX", extra={"project_id": project_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method="GET",
            path=endpoint,
            params={},
        )

        if is_http_error(status):
            logger.warning(
                "Failed to fetch project export: HTTP %s",
                status,
                extra={"project_id": project_id, "status": status},
            )
            return {}

        return response

    async def _fetch_claimx_assignment(self, assignment_id: int) -> dict:
        endpoint = f"/customTasks/assignment/{assignment_id}"

        logger.debug("Fetching assignment from ClaimX", extra={"assignment_id": assignment_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method="GET",
            path=endpoint,
            params={"full": "true"},
        )

        if is_http_error(status):
            raise RuntimeError(f"ClaimX API returned error status {status}: {response}")

        return response

    async def _fetch_project_media_urls(
        self, project_id: int, media_ids: list[str] | None = None
    ) -> dict[int, str]:
        """Build media_id -> download URL lookup map from ClaimX API."""
        endpoint = f"/export/project/{project_id}/media"
        params = {}

        if media_ids:
            params["MediaIds"] = ",".join(media_ids)
            logger.debug(
                "Fetching selective project media",
                extra={"project_id": project_id, "media_id_count": len(media_ids)},
            )
        else:
            logger.debug("Fetching all project media", extra={"project_id": project_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method="GET",
            path=endpoint,
            params=params,
        )

        if is_http_error(status):
            logger.warning(
                "Failed to fetch project media: HTTP %s",
                status,
                extra={"project_id": project_id, "status": status},
            )
            return {}
        if isinstance(response, list):
            media_list = response
        elif isinstance(response, dict):
            if "data" in response:
                media_list = response["data"]
            elif "media" in response:
                media_list = response["media"]
            else:
                media_list = [response]
        else:
            media_list = []
        media_url_map = {}
        for media in media_list:
            media_id = media.get("mediaID")
            download_url = media.get("fullDownloadLink")
            if media_id and download_url:
                media_url_map[media_id] = download_url

        logger.info(
            "Fetched project media URLs",
            extra={
                "project_id": project_id,
                "total_media": len(media_list),
                "with_urls": len(media_url_map),
            },
        )

        # Resolve ClaimX URLs to S3 pre-signed URLs
        resolved_media_url_map = {}
        for media_id, claimx_url in media_url_map.items():
            s3_url = await self._resolve_redirect_url(claimx_url)
            resolved_media_url_map[media_id] = s3_url

        logger.info(
            "Resolved %d media URLs to S3",
            len(resolved_media_url_map),
            extra={"project_id": project_id},
        )

        return resolved_media_url_map

    def _is_s3_url(self, url: str) -> bool:
        """
        Check if URL is already an S3 URL.

        S3 URLs can have various formats:
        - https://s3.amazonaws.com/bucket/key
        - https://bucket.s3.amazonaws.com/key
        - https://bucket.s3.region.amazonaws.com/key

        Args:
            url: URL to check

        Returns:
            True if URL is an S3 URL, False otherwise
        """
        if not url:
            return False

        url_lower = url.lower()
        return (
            "s3.amazonaws.com" in url_lower
            or "s3-" in url_lower  # Handles s3-region.amazonaws.com patterns
        )

    async def _resolve_redirect_url(self, claimx_url: str) -> str:
        """
        Follow 302 redirect from ClaimX URL to get S3 pre-signed URL.

        ClaimX media URLs require auth and redirect to S3 - we need the final S3 URL
        since receivers won't have ClaimX auth.

        Some files (large mp4/mov) already link directly to S3, so we skip redirect
        resolution for those.

        Args:
            claimx_url: ClaimX media download URL or direct S3 URL

        Returns:
            S3 pre-signed URL from Location header, or original URL if already S3 or redirect fails
        """
        if self._is_s3_url(claimx_url):
            logger.debug(
                "URL is already S3, skipping redirect resolution",
                extra={"url_prefix": claimx_url[:80]},
            )
            return claimx_url

        try:
            response = await self.connections.request_url(
                connection_name=self.claimx_connection,
                method="HEAD",
                url=claimx_url,
                allow_redirects=False,
                timeout_override=10,
            )
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if location:
                    logger.debug(
                        "Resolved ClaimX URL to S3",
                        extra={
                            "status": response.status,
                            "claimx_url_prefix": claimx_url[:80],
                            "s3_url_prefix": location[:80],
                        },
                    )
                    return location

            logger.warning(
                "No redirect found for ClaimX URL (status %s)",
                response.status,
                extra={"url_prefix": claimx_url[:80], "status": response.status},
            )
            return claimx_url

        except Exception as e:
            logger.warning(
                "Failed to resolve redirect URL: %s",
                e,
                extra={"url_prefix": claimx_url[:80]},
            )
            return claimx_url

    async def _write_to_delta(
        self,
        event: TaskEvent,
        submission: CabinetSubmission | None,
        attachments: list[CabinetAttachment],
    ) -> None:
        logger.info("Writing to Delta tables", extra={"assignment_id": event.assignment_id})
        submission_row = submission.to_dict() if submission else self._build_metadata_row(event)

        await self.submissions_writer.write(submission_row)
        if attachments:
            attachment_rows = [att.to_dict() for att in attachments]
            await self.attachments_writer.write(attachment_rows)

        logger.info(
            "Delta write complete",
            extra={
                "assignment_id": event.assignment_id,
                "has_submission": submission is not None,
                "attachment_count": len(attachments),
            },
        )

    def _build_metadata_row(self, event: TaskEvent) -> dict:
        """Build minimal submission row for non-completed statuses."""
        now = datetime.now(UTC)

        return {
            "assignment_id": event.assignment_id,
            "project_id": event.project_id,
            "task_id": event.task_id,
            "task_name": event.task_name,
            "task_status": event.task_status,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "event_timestamp": event.event_timestamp,
            "assigned_to_user_id": event.assigned_to_user_id,
            "assigned_by_user_id": event.assigned_by_user_id,
            "task_created_at": event.task_created_at,
            "task_completed_at": event.task_completed_at,
            "updated_at": now.isoformat(),
            "form_id": None,
            "customer_first_name": None,
            "customer_last_name": None,
        }

    async def _submit_to_api(
        self,
        event: TaskEvent,
        submission: CabinetSubmission,
        attachments: list[CabinetAttachment],
        readable_report: dict | None,
        insured_info: dict | None = None,
        adjuster_info: dict | None = None,
        project_number: str = "",
    ) -> None:
        """Send completed task data directly to iTel API via ItelApiSender."""
        logger.info(
            "Submitting to iTel API",
            extra={"assignment_id": event.assignment_id},
        )

        payload = {
            "event_id": event.event_id,
            "event_timestamp": event.event_timestamp,
            "assignment_id": event.assignment_id,
            "project_id": event.project_id,
            "project_number": project_number,
            "task_id": event.task_id,
            "submission": submission.to_dict(),
            "attachments": [att.to_dict() for att in attachments],
            "readable_report": readable_report,
            "insured_info": insured_info or {},
            "adjuster_info": adjuster_info or {},
            "published_at": datetime.now(UTC).isoformat(),
            "source": "itel_cabinet_tracking_worker",
        }

        success = await self.api_sender.submit(payload)
        logger.info(
            "iTel API submission complete",
            extra={"assignment_id": event.assignment_id, "success": success},
        )
