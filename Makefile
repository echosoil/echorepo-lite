.PHONY: help devtools fmt lint check

help:
	@echo "Targets:"
	@echo "  devtools  - install dev tools + pre-commit"
	@echo "  fmt       - auto-format (ruff formatter)"
	@echo "  lint      - lint (ruff)"
	@echo "  check     - lint + format check"

devtools:
	python -m pip install -U pip
	python -m pip install -r requirements-dev.txt
	pre-commit install

fmt:
	ruff format .
	ruff check --fix .

lint:
	ruff check .

check:
	ruff format --check .
	ruff check .
