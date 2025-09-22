"""Analyze command for comprehensive cache analysis without manipulation."""

from dbt_toolbox.analysees import analyze, print_analysis_results
from dbt_toolbox.cli._common_options import OptionModelSelection, OptionTarget
from dbt_toolbox.cli._exit_handler import exit_run


def analyze_command(
    target: OptionTarget = None,
    model: OptionModelSelection = None,
) -> None:
    """Analyze cache state and column references without manipulating them.

    Shows outdated models, ID mismatches, failed models that need re-execution,
    and column reference issues.
    """
    # Use the unified analyze function
    results = analyze(target=target, model=model)

    # Print cache analysis results
    print_analysis_results(results)

    exit_run(0)
