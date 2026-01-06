# ecommerce-product-review-analysis

This project uses `uv` for dependency management.

## Prerequisites
- **uv**: [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

## Setup
To install all dependencies and set up the virtual environment, run:

```bash
uv sync
```

## Running the Crawler
To run the Playwright crawler:

```bash
# First time setup for Playwright browsers
uv run playwright install

# Run the script
uv run kyobo_chroller_sample.py
```

## Dependencies
Dependencies are managed in `pyproject.toml` and locked in `uv.lock`.
Start adding new dependencies with:
```bash
uv add <package_name>
```