# Makefile for US Vehicle Accident Analysis Project

.DEFAULT_GOAL := help

# Python and Spark settings
PYTHON := python3
SPARK_HOME := $(HOME)/Users/sushritsaha/Downloads/spark-3.5.3-bin-hadoop3
SPARK_SUBMIT := $(SPARK_HOME)/bin/spark-submit
CONFIG_FILE := config.yaml

# Spark configuration
SPARK_MASTER := "local[*]"

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
	$(PYTHON) -m pip install -r requirements.txt

prep_data: ## Unzip and prepare data files
	unzip -n Data.zip -d . -x "__MACOSX*"
	rm -rf __MACOSX

build: clean ## Build project package
	$(PYTHON) setup.py bdist_egg

run: build ## Run the analysis using spark-submit
	$(SPARK_SUBMIT) \
		--master $(SPARK_MASTER) \
		--py-files dist/bcg_case_study_29nov-0.0.1-py3.10.egg \
		main.py --config $(CONFIG_FILE)

all: clean setup prep_data build run ## Run all steps in sequence

.PHONY: help clean setup build run all
