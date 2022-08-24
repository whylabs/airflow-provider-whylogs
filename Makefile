dev-setup: ## Prepares a fresh development environment 
	( \
	   python3 -m venv .venv; \
       source .venv/bin/activate; \
	   pip install -q -r requirements.txt; \
	)

test: ## Runs necessary tests on the current environment
	. .venv/bin/activate && py.test tests/
	
dist: test  ## Tests the code and builds the package with the version described on ./VERSION
	rm -rf dist/
	python3 -m build .

publish: dist ## Makes fresh distributions and publishes to PyPI 
	twine upload dist/*

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