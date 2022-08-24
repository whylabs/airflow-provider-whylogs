dev-setup: ## Prepares a fresh development environment 
	( \
	   python3 -m venv .venv; \
       . .venv/bin/activate; \
	   pip install -q -r requirements.txt; \
	)

test: ## Runs necessary tests on the current environment
	. .venv/bin/activate && py.test tests/
	
dist: clean  ## Builds the package with the version described on ./VERSION
	( \
		. .venv/bin/activate; \
		python3 -m build .; \
	)

clean:  ## Cleans the environment and dist packages
	rm -rf airflow_provider_whylogs.egg-info
	rm -rf .pytest_cache
	rm -rf dist

help: 
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT