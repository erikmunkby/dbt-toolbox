"""Analyze command for comprehensive cache analysis without manipulation."""

from dbt_toolbox.analysees import analyze, print_analysis_results
from dbt_toolbox.cli._common_options import (
    ArgumentModelSelection,
    OptionModelSelection,
    OptionTarget,
)
from dbt_toolbox.cli._exit_handler import exit_run


def analyze_command(
    models: ArgumentModelSelection = None,
    target: OptionTarget = None,
    model: OptionModelSelection = None,
) -> None:
    """Analyze cache state and column references without manipulating them.

    Shows outdated models, ID mismatches, failed models that need re-execution,
    and column reference issues.

    Example: dt analyze +my_model -t prod
    """
    # Merge positional models argument with --model option
    # Positional argument takes precedence if both are provided
    final_model_selection = models or model

    # Use the unified analyze function
    results = analyze(target=target, model=final_model_selection)

    # Print cache analysis results
    print_analysis_results(results)

    exit_run(0)
