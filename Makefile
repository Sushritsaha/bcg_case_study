# Makefile for US Vehicle Accident Analysis Project

.DEFAULT_GOAL := help

# Environment settings - can be overridden via environment variables
PYTHON ?= python3
SPARK_HOME ?= $(shell if [ -z "$$SPARK_HOME" ]; then echo "../../Downloads/spark-3.5.3-bin-hadoop3"; else echo "$$SPARK_HOME"; fi)
SPARK_SUBMIT ?= $(SPARK_HOME)/bin/spark-submit

# Project settings - can be customized via environment variables
CONFIG_FILE ?= config.yaml
PROJECT_NAME ?= bcg_case_study_29nov
PROJECT_VERSION ?= 0.0.1
PYTHON_VERSION ?= 3.10

# Spark configuration - can be overridden via environment variables
SPARK_MASTER ?= local[*]
SPARK_APP_NAME ?= US Vehicle Accident Analysis
SPARK_SHUFFLE_PARTITIONS ?= 8
SPARK_EXECUTOR_MEMORY ?= 2g
SPARK_DRIVER_MEMORY ?= 2g

# Define make help functionality
define PRINT_HELP_PYSCRIPT
import re, sys
for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help: ## Display this help message
	@printf -- "Make commands for US Vehicle Accident Analysis\n"
	@printf -- "Environment variables that can be set:\n"
	@printf -- "  SPARK_HOME          Path to Spark installation (current: $(SPARK_HOME))\n"
	@printf -- "  PYTHON             Python interpreter to use (current: $(PYTHON))\n"
	@printf -- "  CONFIG_FILE        Config file to use (current: $(CONFIG_FILE))\n"
	@printf -- "  SPARK_MASTER       Spark master URL (current: $(SPARK_MASTER))\n"
	@printf -- "  SPARK_APP_NAME     Spark application name (current: $(SPARK_APP_NAME))\n"
	@$(PYTHON) -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: ## Clean up generated files and directories
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf src/__pycache__/
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

setup: clean ## Set up project environment
	mkdir -p logs
	mkdir -p Output/{1..10}
	if [ -f "requirements.txt" ]; then \
		$(PYTHON) -m pip install -r requirements.txt; \
	fi

prep_data: ## Unzip and prepare data files
	if [ -f "Data.zip" ]; then \
		unzip -n Data.zip -d . -x "__MACOSX*"; \
		rm -rf __MACOSX; \
	else \
		echo "Data.zip not found. Please ensure data file is present."; \
		exit 1; \
	fi

build: clean ## Build project package
	$(PYTHON) setup.py bdist_egg

verify_spark: ## Verify Spark installation
	@if [ ! -d "$(SPARK_HOME)" ]; then \
		echo "Error: SPARK_HOME directory not found at $(SPARK_HOME)"; \
		echo "Please set SPARK_HOME environment variable to your Spark installation directory"; \
		exit 1; \
	fi
	@if [ ! -x "$(SPARK_SUBMIT)" ]; then \
		echo "Error: spark-submit not found at $(SPARK_SUBMIT)"; \
		exit 1; \
	fi

run: build verify_spark ## Run the analysis using spark-submit
	$(SPARK_SUBMIT) \
		--master $(SPARK_MASTER) \
		--name "$(SPARK_APP_NAME)" \
		--py-files dist/$(PROJECT_NAME)-$(PROJECT_VERSION)-py$(PYTHON_VERSION).egg \
		--conf spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) \
		--conf spark.executor.memory=$(SPARK_EXECUTOR_MEMORY) \
		--conf spark.driver.memory=$(SPARK_DRIVER_MEMORY) \
		main.py --config $(CONFIG_FILE)

all: clean setup prep_data build run ## Run all steps in sequence

.PHONY: help clean setup verify_spark build run all

