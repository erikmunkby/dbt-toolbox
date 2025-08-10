fix:
	uv run ruff format && uv run ruff check --fix-only --unsafe-fixes && uv run ruff check