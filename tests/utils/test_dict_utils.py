"""Tests for utils.dict_utils module."""

from dbt_toolbox.utils.dict_utils import remove_empty_values


class TestRemoveEmptyValues:
    """Test the remove_empty_values function."""

    def test_remove_empty_dict(self) -> None:
        """Test removing empty dictionaries."""
        input_dict = {"a": 1, "b": {}, "c": 3}
        result = remove_empty_values(input_dict)
        assert result == {"a": 1, "c": 3}

    def test_remove_empty_list(self) -> None:
        """Test removing empty lists."""
        input_dict = {"a": 1, "b": [], "c": 3}
        result = remove_empty_values(input_dict)
        assert result == {"a": 1, "c": 3}

    def test_remove_none_values(self) -> None:
        """Test removing None values."""
        input_dict = {"a": 1, "b": None, "c": 3}
        result = remove_empty_values(input_dict)
        assert result == {"a": 1, "c": 3}

    def test_keep_zero_and_false(self) -> None:
        """Test that 0 and False are not removed."""
        input_dict = {"a": 0, "b": False, "c": ""}
        result = remove_empty_values(input_dict)
        # 0 and False should be kept, empty string removed
        assert result == {"a": 0, "b": False}

    def test_nested_dict_cleaning(self) -> None:
        """Test cleaning nested dictionaries."""
        input_dict = {"a": {"b": {}, "c": 1}, "d": []}
        result = remove_empty_values(input_dict)
        # Nested empty dict should be removed
        assert result == {"a": {"c": 1}}

    def test_empty_input(self) -> None:
        """Test with empty input."""
        input_dict = {}
        result = remove_empty_values(input_dict)
        assert result == {}

    def test_all_empty_values(self) -> None:
        """Test when all values are empty."""
        input_dict = {"a": {}, "b": [], "c": None}
        result = remove_empty_values(input_dict)
        assert result == {}

    def test_list_of_dicts(self) -> None:
        """Test removing empty values from list of dictionaries."""
        input_dict = {"items": [{"a": 1}, {"b": None}, {"c": 3}]}
        result = remove_empty_values(input_dict)
        # Should clean each dict in the list
        assert result == {"items": [{"a": 1}, {}, {"c": 3}]}
