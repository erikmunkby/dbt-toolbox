import re

from dbt_toolbox.dbt_parser._dbt_parser import (
    _re_find_docs_macro_definitions,
    _re_find_docs_macro_reference,
)


class TestDocsMacroDefinitions:
    """Test the regex for finding docs macro definitions in .md files."""

    def test_basic_docs_macro(self) -> None:
        """Test basic docs macro extraction."""
        text = """
        {% docs customer %}
        Some customer description
        {% enddocs %}
        """
        matches = _re_find_docs_macro_definitions.findall(text)
        assert len(matches) == 1
        assert matches[0] == ("customer", "Some customer description")

    def test_docs_macro_with_spacing_variations(self) -> None:
        """Test docs macro with various spacing."""
        text = """
        {%docs my_date%}
        Date when booking was made
        {%enddocs%}

        {% docs   customer_id   %}
        Account identifier
        {% enddocs %}
        """
        matches = _re_find_docs_macro_definitions.findall(text)
        assert len(matches) == 2
        assert matches[0] == ("my_date", "Date when booking was made")
        assert matches[1] == ("customer_id", "Account identifier")

    def test_docs_macro_multiline_content(self) -> None:
        """Test docs macro with multiline content."""
        text = """
        {% docs complex_field %}
        This is a complex field that has:
        - Multiple lines
        - With various formatting

        And even blank lines in between.
        {% enddocs %}
        """
        matches = _re_find_docs_macro_definitions.findall(text)
        assert len(matches) == 1
        name, content = matches[0]
        assert name == "complex_field"
        assert "Multiple lines" in content
        assert "blank lines" in content

    def test_multiple_docs_macros(self) -> None:
        """Test multiple docs macros in same text."""
        text = """
        {% docs field1 %}
        Description for field1
        {% enddocs %}

        Some other content

        {% docs field2 %}
        Description for field2
        {% enddocs %}
        """
        matches = _re_find_docs_macro_definitions.findall(text)
        assert len(matches) == 2
        assert matches[0] == ("field1", "Description for field1")
        assert matches[1] == ("field2", "Description for field2")

    def test_empty_docs_macro(self) -> None:
        """Test docs macro with empty content."""
        text = """
        {% docs empty_field %}
        {% enddocs %}
        """
        matches = _re_find_docs_macro_definitions.findall(text)
        assert len(matches) == 1
        assert matches[0] == ("empty_field", "")


class TestDocsMacroReferences:
    """Test the regex for finding doc macro references."""

    def test_basic_doc_reference_single_quotes(self) -> None:
        """Test basic doc reference with single quotes."""
        text = "description: '{{ doc('my_date') }}'"
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 1
        assert matches[0] == ("{{ doc('my_date') }}", "my_date")

    def test_basic_doc_reference_double_quotes(self) -> None:
        """Test basic doc reference with double quotes."""
        text = 'description: "{{ doc("customer_id") }}"'
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 1
        assert matches[0] == ('{{ doc("customer_id") }}', "customer_id")

    def test_doc_reference_no_spaces(self) -> None:
        """Test doc reference without spaces."""
        text = "{{doc('amount')}}"
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 1
        assert matches[0] == ("{{doc('amount')}}", "amount")

    def test_doc_reference_extra_spaces(self) -> None:
        """Test doc reference with extra spaces."""
        text = "{{  doc(  'customer_id'  )  }}"
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 1
        assert matches[0] == ("{{  doc(  'customer_id'  )  }}", "customer_id")

    def test_multiple_doc_references(self) -> None:
        """Test multiple doc references in same text."""
        text = """
        description: '{{ doc('my_date') }}'
        other_field: "{{doc('customer_id')}}"
        mixed: '{{ doc("amount") }}'
        """
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 3
        assert matches[0] == ("{{ doc('my_date') }}", "my_date")
        assert matches[1] == ("{{doc('customer_id')}}", "customer_id")
        assert matches[2] == ('{{ doc("amount") }}', "amount")

    def test_doc_reference_mixed_quotes(self) -> None:
        """Test doc references with mixed quote types."""
        text = """
        field1: {{ doc('single_quotes') }}
        field2: {{ doc("double_quotes") }}
        """
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 2
        assert matches[0] == ("{{ doc('single_quotes') }}", "single_quotes")
        assert matches[1] == ('{{ doc("double_quotes") }}', "double_quotes")

    def test_doc_reference_with_underscores_and_numbers(self) -> None:
        """Test doc references with underscores and numbers in names."""
        text = "{{ doc('field_123_test') }}"
        matches = re.findall(_re_find_docs_macro_reference, text)
        assert len(matches) == 1
        assert matches[0] == ("{{ doc('field_123_test') }}", "field_123_test")

    def test_no_matches_for_invalid_syntax(self) -> None:
        """Test that invalid syntax doesn't match."""
        invalid_texts = [
            "{{ doc() }}",  # Empty
            "{{ doc('unclosed) }}",  # Unclosed quote
            "{{ doc(unquoted) }}",  # Unquoted
            "{ doc('single_brace') }",  # Single braces
        ]
        for text in invalid_texts:
            matches = re.findall(_re_find_docs_macro_reference, text)
            assert len(matches) == 0, f"Should not match: {text}"
