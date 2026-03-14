"""
Tests for Delta table reader.

Covers:
- get_open_file_descriptors on Linux and error paths
- DeltaTableReader: read, scan, read_filtered, read_as_polars, exists
- Resource cleanup: context managers, close, __del__

All external dependencies (polars, deltalake, auth) are mocked.
Since polars is a MagicMock in sys.modules, we test control flow and logic
paths rather than actual DataFrame operations.
"""

import sys
import warnings
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pipeline.common.storage.delta import (
    DELTA_CIRCUIT_CONFIG,
    DELTA_RETRY_CONFIG,
    DeltaTableReader,
    _on_auth_error,
    get_open_file_descriptors,
)


class MockExpr:
    """Mock polars expression that supports all comparison and bitwise operators.

    MagicMock raises TypeError for comparison operators (<, >, <=, >=) when the
    other operand is a non-MagicMock type (like datetime.date or str). This class
    returns itself from every operation so chained expressions work.
    """

    def __getattr__(self, name):
        return MockExpr()

    def __call__(self, *args, **kwargs):
        return MockExpr()

    def __eq__(self, other):
        return MockExpr()

    def __ne__(self, other):
        return MockExpr()

    def __lt__(self, other):
        return MockExpr()

    def __gt__(self, other):
        return MockExpr()

    def __le__(self, other):
        return MockExpr()

    def __ge__(self, other):
        return MockExpr()

    def __and__(self, other):
        return MockExpr()

    def __rand__(self, other):
        return MockExpr()

    def __or__(self, other):
        return MockExpr()

    def __ror__(self, other):
        return MockExpr()

    def __invert__(self):
        return MockExpr()

    def __hash__(self):
        return id(self)


# ---------------------------------------------------------------------------
# get_open_file_descriptors
# ---------------------------------------------------------------------------


class TestGetOpenFileDescriptors:
    def test_returns_integer(self):
        result = get_open_file_descriptors()
        assert isinstance(result, int)

    def test_returns_positive_on_linux(self):
        import sys

        if not sys.platform.startswith("linux"):
            pytest.skip("Linux only")
        result = get_open_file_descriptors()
        assert result > 0

    @patch("os.path.exists", return_value=False)
    def test_returns_negative_one_when_no_proc(self, mock_exists):
        # When /proc/pid/fd doesn't exist, falls back to resource module
        result = get_open_file_descriptors()
        # Should return -1 (fallback path)
        assert result == -1

    @patch("os.listdir", side_effect=PermissionError("denied"))
    def test_returns_negative_one_on_error(self, mock_listdir):
        result = get_open_file_descriptors()
        assert result == -1


# ---------------------------------------------------------------------------
# _on_auth_error
# ---------------------------------------------------------------------------


class TestOnAuthError:
    @patch("pipeline.common.storage.delta._refresh_all_credentials")
    def test_calls_refresh_all_credentials(self, mock_refresh):
        _on_auth_error()
        mock_refresh.assert_called_once()


# ---------------------------------------------------------------------------
# DELTA_RETRY_CONFIG / DELTA_CIRCUIT_CONFIG
# ---------------------------------------------------------------------------


class TestDeltaConfigs:
    def test_retry_config_values(self):
        assert DELTA_RETRY_CONFIG.max_attempts == 3
        assert DELTA_RETRY_CONFIG.base_delay == 1.0
        assert DELTA_RETRY_CONFIG.max_delay == 10.0

    def test_circuit_config_values(self):
        assert DELTA_CIRCUIT_CONFIG.failure_threshold == 5
        assert DELTA_CIRCUIT_CONFIG.timeout_seconds == 30.0


# ---------------------------------------------------------------------------
# DeltaTableReader - initialization
# ---------------------------------------------------------------------------


class TestDeltaTableReaderInit:
    def test_initializes_with_path(self):
        reader = DeltaTableReader("abfss://ws@acct/table")
        assert reader.table_path == "abfss://ws@acct/table"
        assert reader.storage_options is None
        assert reader._delta_table is None
        assert not reader._closed

    def test_initializes_with_storage_options(self):
        opts = {"account_name": "test", "account_key": "key"}
        reader = DeltaTableReader("abfss://ws@acct/table", storage_options=opts)
        assert reader.storage_options == opts


# ---------------------------------------------------------------------------
# DeltaTableReader - context manager and close
# ---------------------------------------------------------------------------


class TestDeltaTableReaderLifecycle:
    async def test_async_context_manager(self):
        async with DeltaTableReader("test://table") as reader:
            assert not reader._closed
        assert reader._closed

    async def test_close_is_idempotent(self):
        reader = DeltaTableReader("test://table")
        await reader.close()
        await reader.close()
        assert reader._closed

    async def test_close_releases_delta_table(self):
        reader = DeltaTableReader("test://table")
        reader._delta_table = MagicMock()
        await reader.close()
        assert reader._delta_table is None

    def test_del_warns_when_not_closed(self):
        reader = DeltaTableReader("test://table")
        reader._closed = False
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            reader.__del__()
            assert len(w) == 1
            assert "was not properly closed" in str(w[0].message)

    async def test_del_no_warning_when_closed(self):
        reader = DeltaTableReader("test://table")
        await reader.close()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", ResourceWarning)
            reader.__del__()
            assert len(w) == 0


# ---------------------------------------------------------------------------
# DeltaTableReader - read
# ---------------------------------------------------------------------------


class TestDeltaTableReaderRead:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.read_delta")
    def test_read_calls_polars_read_delta(self, mock_read, mock_opts):
        mock_opts.return_value = {"key": "val"}
        mock_df = MagicMock()
        mock_read.return_value = mock_df

        reader = DeltaTableReader("abfss://ws@acct/table")
        result = reader.read()

        mock_read.assert_called_once_with(
            "abfss://ws@acct/table",
            storage_options={"key": "val"},
            columns=None,
        )
        assert result is mock_df

    @patch("pipeline.common.storage.delta.pl.read_delta")
    def test_read_uses_provided_storage_options(self, mock_read):
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        opts = {"custom": "option"}

        reader = DeltaTableReader("abfss://ws@acct/table", storage_options=opts)
        reader.read()

        mock_read.assert_called_once_with(
            "abfss://ws@acct/table",
            storage_options=opts,
            columns=None,
        )

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.read_delta")
    def test_read_with_columns(self, mock_read, mock_opts):
        mock_opts.return_value = {}
        mock_read.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read(columns=["id", "name"])

        mock_read.assert_called_once_with(
            "abfss://ws@acct/table",
            storage_options={},
            columns=["id", "name"],
        )


# ---------------------------------------------------------------------------
# DeltaTableReader - scan
# ---------------------------------------------------------------------------


class TestDeltaTableReaderScan:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_scan_returns_lazy_frame(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        result = reader.scan()

        assert result is mock_lf

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_scan_with_columns_selects(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.scan(columns=["id", "name"])

        mock_lf.select.assert_called_once_with(["id", "name"])


# ---------------------------------------------------------------------------
# DeltaTableReader - read_filtered
# ---------------------------------------------------------------------------


class TestDeltaTableReaderReadFiltered:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_applies_filter(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        # Chain all methods to return the same mock
        mock_lf.filter.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        filter_expr = MagicMock()
        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(filter_expr)

        mock_lf.filter.assert_called_once_with(filter_expr)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_columns(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), columns=["id"])

        mock_lf.select.assert_called_once_with(["id"])

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_order_by(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), order_by="created_at", descending=True)

        mock_lf.sort.assert_called_once_with("created_at", descending=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_limit_and_no_order_by(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), limit=10)

        mock_lf.head.assert_called_once_with(10)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_uses_streaming_without_sort(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock())

        mock_lf.collect.assert_called_once_with(streaming=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_disables_streaming_with_sort(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), order_by="ts")

        mock_lf.collect.assert_called_once_with(streaming=False)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_filtered_with_limit_and_order_by(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.head.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_filtered(MagicMock(), limit=5, order_by="ts")

        mock_lf.sort.assert_called_once_with("ts", descending=False)
        mock_lf.head.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# DeltaTableReader - read_as_polars
# ---------------------------------------------------------------------------


class TestDeltaTableReaderReadAsPolars:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_no_filters(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars()

        mock_lf.collect.assert_called_once_with(streaming=True)

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_with_columns(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(columns=["id", "name"])

        mock_lf.select.assert_called_once_with(["id", "name"])

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_raises_on_unsupported_operator(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        with pytest.raises(TypeError, match="Filter error"):
            reader.read_as_polars(filters=[("col", "LIKE", "val")])

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_read_as_polars_applies_eq_filter(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(filters=[("status", "=", "active")])

        # filter should have been called for the "=" operation
        mock_lf.filter.assert_called_once()

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_as_polars_applies_ne_filter(self, mock_col, mock_scan, mock_opts):
        """Test that != filter operator is applied."""
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        # Make pl.col() return a mock that supports all comparisons
        mock_col.return_value = MockExpr()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(filters=[("status", "!=", "inactive")])

        mock_lf.filter.assert_called_once()

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    @patch("pipeline.common.storage.delta.pl.col")
    def test_read_as_polars_applies_comparison_filters(self, mock_col, mock_scan, mock_opts):
        """Test that <, >, <=, >= filter operators are applied."""
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf
        mock_lf.filter.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock()

        mock_col.return_value = MockExpr()

        reader = DeltaTableReader("abfss://ws@acct/table")
        reader.read_as_polars(
            filters=[
                ("col1", "<", "val1"),
                ("col2", ">", "val2"),
                ("col3", "<=", "val3"),
                ("col4", ">=", "val4"),
            ]
        )

        assert mock_lf.filter.call_count == 4


# ---------------------------------------------------------------------------
# DeltaTableReader - exists
# ---------------------------------------------------------------------------


class TestDeltaTableReaderExists:
    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_exists_returns_true_when_table_readable(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        reader = DeltaTableReader("abfss://ws@acct/table")
        assert reader.exists() is True

    @patch("pipeline.common.storage.delta.get_storage_options")
    @patch("pipeline.common.storage.delta.pl.scan_delta")
    def test_exists_returns_false_on_error(self, mock_scan, mock_opts):
        mock_opts.return_value = {}
        mock_scan.side_effect = Exception("table not found")

        reader = DeltaTableReader("abfss://ws@acct/table")
        assert reader.exists() is False
