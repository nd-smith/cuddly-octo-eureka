"""
Tests for ClaimX Delta entity writer.

Tests cover:
- Initialization with multiple table paths
- Writing to different entity tables
- Schema validation and type conversion
- Merge operations for upserts
- Append operations for contacts/media
"""

import logging
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from pipeline.claimx.schemas.entities import EntityRowsMessage


@pytest.fixture
def mock_base_writer_factory():
    """Factory to create mock BaseDeltaWriter instances."""
    created_writers = {}

    def create_mock_writer(table_path, **kwargs):
        mock = MagicMock()
        mock.table_path = table_path
        mock.partition_column = kwargs.get("partition_column")
        mock._async_append = AsyncMock(return_value=True)
        mock._async_merge = AsyncMock(return_value=True)
        mock.append_call_count = 0
        mock.merge_call_count = 0
        mock.last_df = None

        # Track call counts

        async def tracking_append(df):
            mock.append_call_count += 1
            mock.last_df = df
            return True

        async def tracking_merge(df, **merge_kwargs):
            mock.merge_call_count += 1
            mock.last_df = df
            return True

        mock._async_append = AsyncMock(side_effect=tracking_append)
        mock._async_merge = AsyncMock(side_effect=tracking_merge)

        created_writers[table_path] = mock
        return mock

    return create_mock_writer, created_writers


@pytest.fixture
def claimx_entity_writer(mock_base_writer_factory):
    """Create a ClaimXEntityWriter with mocked Delta backends."""
    create_mock, created_writers = mock_base_writer_factory

    with patch(
        "pipeline.claimx.writers.delta_entities.BaseDeltaWriter",
        side_effect=create_mock,
    ):
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        writer = ClaimXEntityWriter(
            projects_table_path="abfss://test@onelake/lakehouse/claimx_projects",
            contacts_table_path="abfss://test@onelake/lakehouse/claimx_contacts",
            media_table_path="abfss://test@onelake/lakehouse/claimx_attachment_metadata",
            tasks_table_path="abfss://test@onelake/lakehouse/claimx_tasks",
            task_templates_table_path="abfss://test@onelake/lakehouse/claimx_task_templates",
            external_links_table_path="abfss://test@onelake/lakehouse/claimx_external_links",
            video_collab_table_path="abfss://test@onelake/lakehouse/claimx_video_collab",
        )
        writer._mock_writers = created_writers
        yield writer


class TestClaimXEntityWriterInit:
    """Test suite for ClaimXEntityWriter initialization."""

    def test_initialization(self, claimx_entity_writer):
        """Test writer creates all expected table writers."""
        expected_tables = [
            "projects",
            "contacts",
            "media",
            "tasks",
            "task_templates",
            "external_links",
            "video_collab",
        ]

        for table in expected_tables:
            assert table in claimx_entity_writer._writers, f"Missing writer for {table}"

    def test_projects_writer_has_partition(self, claimx_entity_writer):
        """Test that projects writer has project_id partition."""
        projects_writer = claimx_entity_writer._writers["projects"]
        assert projects_writer.partition_column == "project_id"

    def test_media_writer_has_partition(self, claimx_entity_writer):
        """Test that media writer has project_id partition."""
        media_writer = claimx_entity_writer._writers["media"]
        assert media_writer.partition_column == "project_id"


class TestClaimXEntityWriterWriteAll:
    """Test suite for write_all method."""

    @pytest.mark.asyncio
    async def test_write_all_projects_only(self, claimx_entity_writer, sample_project_row):
        """Test writing only projects."""
        entity_rows = EntityRowsMessage(projects=[sample_project_row])

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("projects") == 1
        # Verify merge was called for projects
        projects_writer = claimx_entity_writer._writers["projects"]
        assert projects_writer.merge_call_count == 1

    @pytest.mark.asyncio
    async def test_write_all_contacts_only(self, claimx_entity_writer, sample_contact_row):
        """Test writing only contacts."""
        entity_rows = EntityRowsMessage(contacts=[sample_contact_row])

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("contacts") == 1
        # Verify append was called for contacts (not merge)
        contacts_writer = claimx_entity_writer._writers["contacts"]
        assert contacts_writer.append_call_count == 1
        assert contacts_writer.merge_call_count == 0

    @pytest.mark.asyncio
    async def test_write_all_media_only(self, claimx_entity_writer, sample_media_row):
        """Test writing only media."""
        entity_rows = EntityRowsMessage(media=[sample_media_row])

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("media") == 1
        # Verify append was called for media (not merge)
        media_writer = claimx_entity_writer._writers["media"]
        assert media_writer.append_call_count == 1
        assert media_writer.merge_call_count == 0

    @pytest.mark.asyncio
    async def test_write_all_tasks_only(self, claimx_entity_writer, sample_task_row):
        """Test writing only tasks."""
        entity_rows = EntityRowsMessage(tasks=[sample_task_row])

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("tasks") == 1
        # Verify merge was called for tasks
        tasks_writer = claimx_entity_writer._writers["tasks"]
        assert tasks_writer.merge_call_count == 1

    @pytest.mark.asyncio
    async def test_write_all_multiple_tables(
        self,
        claimx_entity_writer,
        sample_project_row,
        sample_contact_row,
        sample_media_row,
    ):
        """Test writing to multiple tables in one call."""
        entity_rows = EntityRowsMessage(
            projects=[sample_project_row],
            contacts=[sample_contact_row],
            media=[sample_media_row],
        )

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("projects") == 1
        assert counts.get("contacts") == 1
        assert counts.get("media") == 1

    @pytest.mark.asyncio
    async def test_write_all_runs_tables_concurrently(
        self,
        claimx_entity_writer,
        sample_project_row,
        sample_contact_row,
        sample_task_row,
    ):
        """Test that write_all dispatches table writes concurrently."""
        import asyncio

        call_order: list[str] = []

        async def slow_merge(df, **kwargs):
            call_order.append("merge_start")
            await asyncio.sleep(0.01)
            call_order.append("merge_end")
            return True

        async def slow_append(df):
            call_order.append("append_start")
            await asyncio.sleep(0.01)
            call_order.append("append_end")
            return True

        # Patch writers with slow async ops
        claimx_entity_writer._writers["projects"]._async_merge = AsyncMock(side_effect=slow_merge)
        claimx_entity_writer._writers["contacts"]._async_append = AsyncMock(side_effect=slow_append)
        claimx_entity_writer._writers["tasks"]._async_merge = AsyncMock(side_effect=slow_merge)

        entity_rows = EntityRowsMessage(
            projects=[sample_project_row],
            contacts=[sample_contact_row],
            tasks=[sample_task_row],
        )

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert len(counts) == 3
        assert failed_tables == []
        # With concurrent execution, all starts should happen before all ends
        starts = [i for i, x in enumerate(call_order) if x.endswith("_start")]
        ends = [i for i, x in enumerate(call_order) if x.endswith("_end")]
        # At least 2 starts before the first end (proves concurrency)
        assert starts[1] < ends[0], f"Writes appear sequential: {call_order}"

    @pytest.mark.asyncio
    async def test_write_all_handles_exception_in_gather(
        self,
        claimx_entity_writer,
        sample_project_row,
        sample_contact_row,
    ):
        """Test that write_all handles exceptions from individual table writes."""
        claimx_entity_writer._writers["projects"]._async_merge = AsyncMock(
            side_effect=RuntimeError("storage exploded")
        )

        entity_rows = EntityRowsMessage(
            projects=[sample_project_row],
            contacts=[sample_contact_row],
        )

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert "projects" in failed_tables
        assert counts.get("contacts") == 1

    @pytest.mark.asyncio
    async def test_write_all_empty_entity_rows(self, claimx_entity_writer):
        """Test writing with no data returns empty counts."""
        entity_rows = EntityRowsMessage()

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts == {}

    @pytest.mark.asyncio
    async def test_write_all_multiple_rows_per_table(
        self, claimx_entity_writer, sample_project_row
    ):
        """Test writing multiple rows to a single table."""
        projects = [
            {**sample_project_row, "project_id": "111"},
            {**sample_project_row, "project_id": "222"},
            {**sample_project_row, "project_id": "333"},
        ]
        entity_rows = EntityRowsMessage(projects=projects)

        counts, failed_tables = await claimx_entity_writer.write_all(entity_rows)

        assert counts.get("projects") == 3

        # Verify the DataFrame has 3 rows
        projects_writer = claimx_entity_writer._writers["projects"]
        assert len(projects_writer.last_df) == 3


class TestClaimXEntityWriterSchema:
    """Test schema-related functionality."""

    def test_merge_keys_defined(self):
        """Test that MERGE_KEYS has all required tables."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        expected_tables = [
            "projects",
            "contacts",
            "media",
            "tasks",
            "task_templates",
            "external_links",
            "video_collab",
        ]

        for table in expected_tables:
            assert table in MERGE_KEYS, f"Missing merge keys for {table}"
            assert len(MERGE_KEYS[table]) > 0, f"Empty merge keys for {table}"

    def test_table_schemas_defined(self):
        """Test that TABLE_SCHEMAS has all required tables."""
        from pipeline.claimx.writers.delta_entities import TABLE_SCHEMAS

        expected_tables = [
            "projects",
            "contacts",
            "media",
            "tasks",
            "task_templates",
            "external_links",
            "video_collab",
        ]

        for table in expected_tables:
            assert table in TABLE_SCHEMAS, f"Missing schema for {table}"
            assert len(TABLE_SCHEMAS[table]) > 0, f"Empty schema for {table}"

    def test_projects_schema_fields(self):
        """Test projects schema has required fields."""
        from pipeline.claimx.writers.delta_entities import PROJECTS_SCHEMA

        required_fields = [
            "project_id",
            "project_number",
            "master_file_name",
            "status",
            "trace_id",
            "created_at",
            "updated_at",
            "last_enriched_at",
        ]

        for field in required_fields:
            assert field in PROJECTS_SCHEMA, f"Missing projects field: {field}"

    def test_contacts_schema_fields(self):
        """Test contacts schema has required fields."""
        from pipeline.claimx.writers.delta_entities import CONTACTS_SCHEMA

        required_fields = [
            "project_id",
            "contact_email",
            "contact_type",
            "first_name",
            "last_name",
            "trace_id",
            "created_at",
            "last_enriched_at",
        ]

        for field in required_fields:
            assert field in CONTACTS_SCHEMA, f"Missing contacts field: {field}"

    def test_media_schema_fields(self):
        """Test media schema has required fields."""
        from pipeline.claimx.writers.delta_entities import MEDIA_SCHEMA

        required_fields = [
            "media_id",
            "project_id",
            "file_type",
            "file_name",
            "trace_id",
            "created_at",
            "updated_at",
            "last_enriched_at",
        ]

        for field in required_fields:
            assert field in MEDIA_SCHEMA, f"Missing media field: {field}"


class TestClaimXEntityWriterDataFrameCreation:
    """Test DataFrame creation with schema."""

    def test_create_dataframe_with_schema_projects(self):
        """Test DataFrame creation for projects."""
        from pipeline.claimx.writers.delta_entities import (
            ClaimXEntityWriter,
        )

        # Create a minimal writer instance for testing
        with patch("pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "project_number": "P-123",
                    "master_file_name": "MFN-123",
                    "status": "active",
                }
            ]

            df = writer._create_dataframe_with_schema("projects", rows)

            assert len(df) == 1
            assert df["project_id"][0] == "123"
            assert df.schema["project_id"] == pl.Utf8

    def test_create_dataframe_converts_datetime_strings(self):
        """Test that ISO datetime strings are converted to datetime objects."""
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        with patch("pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00+00:00",
                }
            ]

            df = writer._create_dataframe_with_schema("projects", rows)

            # Should be converted to datetime
            assert df.schema["created_at"] == pl.Datetime("us", "UTC")
            assert df.schema["updated_at"] == pl.Datetime("us", "UTC")

    def test_create_dataframe_converts_date_strings(self):
        """Test that date strings are converted to date objects."""
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        with patch("pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "contact_email": "test@example.com",
                    "contact_type": "primary",
                    "created_date": "2024-01-15",
                }
            ]

            df = writer._create_dataframe_with_schema("contacts", rows)

            # Should be converted to date
            assert df.schema["created_date"] == pl.Date
            assert df["created_date"][0] == date(2024, 1, 15)

    def test_create_dataframe_empty_rows(self):
        """Test DataFrame creation with empty rows returns empty DataFrame."""
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        with patch("pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            df = writer._create_dataframe_with_schema("projects", [])

            assert len(df) == 0


class TestClaimXEntityWriterMergeKeys:
    """Test merge key configurations."""

    def test_projects_merge_key(self):
        """Test projects uses project_id as merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["projects"] == ["project_id"]

    def test_contacts_merge_keys(self):
        """Test contacts uses composite merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert "project_id" in MERGE_KEYS["contacts"]
        assert "contact_email" in MERGE_KEYS["contacts"]
        assert "contact_type" in MERGE_KEYS["contacts"]

    def test_media_merge_key(self):
        """Test media uses media_id as merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["media"] == ["media_id"]

    def test_tasks_merge_key(self):
        """Test tasks uses assignment_id as merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["tasks"] == ["assignment_id"]

    def test_task_templates_merge_key(self):
        """Test task_templates uses task_id as merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["task_templates"] == ["task_id"]

    def test_external_links_merge_key(self):
        """Test external_links uses link_id as merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["external_links"] == ["link_id"]

    def test_video_collab_merge_key(self):
        """Test video_collab uses video_collaboration_id as merge key."""
        from pipeline.claimx.writers.delta_entities import MERGE_KEYS

        assert MERGE_KEYS["video_collab"] == ["video_collaboration_id"]


@pytest.mark.asyncio
async def test_claimx_entity_writer_integration():
    """Integration test with BaseDeltaWriter mocked."""
    mock_writers = {}

    def create_mock_writer(table_path, **kwargs):
        mock = MagicMock()
        mock.table_path = table_path
        mock.partition_column = kwargs.get("partition_column")
        mock._async_append = AsyncMock(return_value=True)
        mock._async_merge = AsyncMock(return_value=True)
        mock_writers[table_path] = mock
        return mock

    with patch(
        "pipeline.claimx.writers.delta_entities.BaseDeltaWriter",
        side_effect=create_mock_writer,
    ):
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        # Create writer
        writer = ClaimXEntityWriter(
            projects_table_path="abfss://test/projects",
            contacts_table_path="abfss://test/contacts",
            media_table_path="abfss://test/media",
            tasks_table_path="abfss://test/tasks",
            task_templates_table_path="abfss://test/task_templates",
            external_links_table_path="abfss://test/external_links",
            video_collab_table_path="abfss://test/video_collab",
        )

        # Verify all 7 writers were created
        assert len(mock_writers) == 7

        # Write some test data
        entity_rows = EntityRowsMessage(
            projects=[
                {
                    "project_id": "int-test-123",
                    "project_number": "P-123",
                    "master_file_name": "MFN-123",
                    "status": "active",
                }
            ],
            contacts=[
                {
                    "project_id": "int-test-123",
                    "contact_email": "test@example.com",
                    "contact_type": "policyholder",
                    "first_name": "Test",
                    "last_name": "User",
                }
            ],
        )

        counts, failed_tables = await writer.write_all(entity_rows)

        assert counts.get("projects") == 1
        assert counts.get("contacts") == 1
        assert failed_tables == []

        # Verify merge was called for projects
        projects_mock = mock_writers["abfss://test/projects"]
        projects_mock._async_merge.assert_called_once()

        # Verify append was called for contacts
        contacts_mock = mock_writers["abfss://test/contacts"]
        contacts_mock._async_append.assert_called_once()


class TestClaimXEntityWriterFailedTables:
    """Test write_all failure reporting (H3)."""

    @pytest.mark.asyncio
    async def test_write_all_reports_failed_tables(self):
        """write_all returns failed table names when writes fail."""
        mock_writers = {}

        def create_mock_writer(table_path, **kwargs):
            mock = MagicMock()
            mock.table_path = table_path
            mock.partition_column = kwargs.get("partition_column")
            # Projects will fail, contacts will succeed
            if "projects" in table_path:
                mock._async_merge = AsyncMock(return_value=False)
            else:
                mock._async_append = AsyncMock(return_value=True)
                mock._async_merge = AsyncMock(return_value=True)
            mock_writers[table_path] = mock
            return mock

        with patch(
            "pipeline.claimx.writers.delta_entities.BaseDeltaWriter",
            side_effect=create_mock_writer,
        ):
            from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

            writer = ClaimXEntityWriter(
                projects_table_path="abfss://test/projects",
                contacts_table_path="abfss://test/contacts",
                media_table_path="abfss://test/media",
                tasks_table_path="abfss://test/tasks",
                task_templates_table_path="abfss://test/task_templates",
                external_links_table_path="abfss://test/external_links",
                video_collab_table_path="abfss://test/video_collab",
            )

            entity_rows = EntityRowsMessage(
                projects=[{"project_id": "123", "status": "active"}],
                contacts=[
                    {
                        "project_id": "123",
                        "contact_email": "a@b.com",
                        "contact_type": "primary",
                    }
                ],
            )

            counts, failed_tables = await writer.write_all(entity_rows)

            assert "projects" in failed_tables
            assert "projects" not in counts
            assert counts.get("contacts") == 1


class TestClaimXEntityWriterColumnWarning:
    """Test unknown column warning (H4)."""

    def test_create_dataframe_warns_on_unknown_columns(self, caplog):
        """_create_dataframe_with_schema warns when columns are not in schema."""
        with patch("pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            rows = [
                {
                    "project_id": "123",
                    "unknown_field": "should be dropped",
                    "another_unknown": 42,
                }
            ]

            with caplog.at_level(logging.WARNING):
                df = writer._create_dataframe_with_schema("projects", rows)

            assert "Dropping columns not in schema" in caplog.text
            assert "project_id" in df.columns
            assert "unknown_field" not in df.columns


class TestClaimXEntityWriterCoercion:
    """Test _coerce_value for numeric and boolean types (M5)."""

    def test_coerce_value_string_to_int(self):
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        assert ClaimXEntityWriter._coerce_value("42", pl.Int32) == 42
        assert ClaimXEntityWriter._coerce_value("12345", pl.Int64) == 12345

    def test_coerce_value_string_to_float(self):
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        assert ClaimXEntityWriter._coerce_value("3.14", pl.Float64) == 3.14

    def test_coerce_value_string_to_boolean(self):
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        assert ClaimXEntityWriter._coerce_value("true", pl.Boolean) is True
        assert ClaimXEntityWriter._coerce_value("True", pl.Boolean) is True
        assert ClaimXEntityWriter._coerce_value("1", pl.Boolean) is True
        assert ClaimXEntityWriter._coerce_value("false", pl.Boolean) is False
        assert ClaimXEntityWriter._coerce_value("0", pl.Boolean) is False

    def test_coerce_value_non_string_passthrough(self):
        from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

        assert ClaimXEntityWriter._coerce_value(42, pl.Int32) == 42
        assert ClaimXEntityWriter._coerce_value(None, pl.Int32) is None


class TestClaimXEntityWriterTimestamp:
    """Test _ensure_timestamp_columns preserves UTC timezone (M6)."""

    def test_ensure_timestamp_preserves_utc(self):
        """pl.lit(now) with explicit cast preserves UTC timezone."""
        with patch("pipeline.claimx.writers.delta_entities.BaseDeltaWriter"):
            from pipeline.claimx.writers.delta_entities import ClaimXEntityWriter

            writer = ClaimXEntityWriter(
                projects_table_path="test",
                contacts_table_path="test",
                media_table_path="test",
                tasks_table_path="test",
                task_templates_table_path="test",
                external_links_table_path="test",
                video_collab_table_path="test",
            )

            df = pl.DataFrame({"project_id": ["123"]})
            result = writer._ensure_timestamp_columns(df)

            assert result.schema["created_at"] == pl.Datetime("us", "UTC")
            assert result.schema["updated_at"] == pl.Datetime("us", "UTC")
