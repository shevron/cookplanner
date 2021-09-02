SHELL := bash

source_dir = cookplanner
tests_dir = tests

poetry = poetry
pytest = $(poetry) run pytest
isort = $(poetry) run isort --profile=black
black = $(poetry) run black
flake8 = $(poetry) run flake8
mypy = $(poetry) run mypy --show-error-codes


poetry.lock: pyproject.toml
	$(poetry) lock

requirements.txt: poetry.lock
	$(poetry) export -f requirements.txt >requirements.txt

.PHONY: all  ## Perform the most common development-time rules
all: format lint mypy test

.PHONY: format  ## Auto-format the source code
format:
	$(isort) $(source_dir) $(tests_dir)
	$(black) $(source_dir) $(tests_dir)

.PHONY: check-format  ## Check the source code format without changes
check-format:
	$(isort) --check-only $(source_dir) $(tests_dir)
	$(black) --check $(source_dir) $(tests_dir)

.PHONY: lint  ## Run flake8 over the application source and tests
lint:
	$(flake8) $(source_dir) $(tests_dir)

.PHONY: mypy  ## Run mypy over the application source
mypy:
	$(mypy) $(source_dir) $(tests_dir)

.PHONY: test  ## Run tests
test:
	$(pytest)

.PHONY: clean  ## Remove temporary and cache files/directories
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf `find . -type d -name '*.egg-info' `
	rm -rf `find . -type d -name '*.db' `
	rm -rf `find . -type d -name 'pip-wheel-metadata' `
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf *.egg-info
	rm -rf build
	rm -rf dist

.PHONY: help  ## Display this message
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-20s\033[0m %s\n", $$2, $$3}'
