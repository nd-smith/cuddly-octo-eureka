"""Tests for ItelApiSender helper methods."""

import pytest

from apps.itel_cabinet.api_sender import ItelApiSender


@pytest.fixture
def sender():
    """Create a bare sender instance (bypass __init__ for pure helpers)."""
    return object.__new__(ItelApiSender)


class TestPadToMinWords:
    def test_empty_string_returns_empty(self, sender):
        assert sender._pad_to_min_words("", min_words=10) == ""

    def test_already_meets_minimum(self, sender):
        text = "one two three four five six seven eight nine ten"
        assert sender._pad_to_min_words(text, min_words=10) == text

    def test_exceeds_minimum_unchanged(self, sender):
        text = "one two three four five six seven eight nine ten eleven"
        assert sender._pad_to_min_words(text, min_words=10) == text

    def test_pads_short_string(self, sender):
        result = sender._pad_to_min_words("water damage", min_words=10)
        words = result.split()
        assert len(words) >= 10
        # Should be repetitions of the original words
        assert all(w in ("water", "damage") for w in words)

    def test_single_word_padded(self, sender):
        result = sender._pad_to_min_words("damaged", min_words=10)
        words = result.split()
        assert len(words) >= 10
        assert all(w == "damaged" for w in words)

    def test_nine_words_gets_padded(self, sender):
        text = "one two three four five six seven eight nine"
        result = sender._pad_to_min_words(text, min_words=10)
        assert len(result.split()) >= 10
