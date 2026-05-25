# Use uv for Development Workflows

The project will use `uv` as the supported development workflow for dependency resolution, locking, and command execution. Package metadata should remain standards-compliant for Python installers, but contributor documentation and automation should use `uv sync` and `uv run` so local development, tests, Streamlit, and future TUI commands run from the same locked environment.
