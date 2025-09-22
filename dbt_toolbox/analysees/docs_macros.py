"""Module for analyzing docs macros."""

from collections import defaultdict

from dbt_toolbox.dbt_parser import dbtParser

from .data_models import DocsAnalysis, DuplicateDocsIssue


def analyze_docs_macros(dbt_parser: dbtParser) -> DocsAnalysis:
    """Analyze docs macros for duplicates and other issues.

    Args:
        dbt_parser: The dbt parser.

    Returns:
        DocsAnalysis containing analysis results

    """
    total_docs_macros = len(dbt_parser.column_macro_docs_list)

    # Count occurrences of each macro name
    macro_counts = defaultdict(int)
    macro_files = defaultdict(set)

    for doc in dbt_parser.column_macro_docs_list:
        name, path = doc["name"], str("/".join(doc["file_path"].parts[-2:]))  # type: ignore
        macro_counts[name] += 1
        macro_files[name].add(path)

    unique_docs_macros = len(macro_counts)

    # Find duplicates
    duplicate_issues = []
    for name, count in macro_counts.items():
        if count > 1:
            duplicate_issues.append(
                DuplicateDocsIssue(
                    macro_name=name, occurrences=count, file_paths=list(macro_files[name])
                )
            )

    overall_status = "OK" if len(duplicate_issues) == 0 else "ISSUES_FOUND"

    return DocsAnalysis(
        overall_status=overall_status,
        duplicate_issues=duplicate_issues,
        total_docs_macros=total_docs_macros,
        unique_docs_macros=unique_docs_macros,
    )
