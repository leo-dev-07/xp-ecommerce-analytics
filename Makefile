# Makefile for Databricks Asset Bundle Testing

.PHONY: test test-integration test-coverage clean

test:
	python -m pytest tests/ -v

test-integration:
	python test_integration.py

test-coverage:
	coverage run -m pytest tests/
	coverage report
	coverage html

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +
	rm -rf htmlcov/
	rm -rf .coverage

install-test-deps:
	pip install -r requirements-test.txt

help:
	@echo "Available targets:"
	@echo "  test              - Run unit tests"
	@echo "  test-integration  - Run integration tests"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  clean             - Clean up Python cache files"
	@echo "  install-test-deps - Install test dependencies"