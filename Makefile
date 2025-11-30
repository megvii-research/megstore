PACKAGE := megstore
VERSION := $(shell cat ${PACKAGE}/version.py | sed -n -E 's/.*=//; s/ //g; s/"//g; p')

test:
	uv run pytest \
		--cov=${PACKAGE} --cov-config=pyproject.toml --cov-report=html:html_cov/ --cov-report=term-missing --cov-report=xml --no-cov-on-fail \
		--durations=10 \
		tests/

format:
	uv run ruff check --fix ${PACKAGE} tests scripts pyproject.toml
	uv run ruff format ${PACKAGE} tests scripts pyproject.toml

style_check:
	uv run ruff check ${PACKAGE} tests scripts pyproject.toml
	uv run ruff format --check ${PACKAGE} tests scripts pyproject.toml

static_check:
	make pytype_check

pytype_check:
	uv run pytype

bandit_check:
	uv run bandit --quiet --format=sarif --recursive megstore/ > bandit-sarif.json || echo

pyre_check:
	uv run pyre --version=none --output=json check > pyre-errors.json || echo
	cat pyre-errors.json | ./scripts/convert_results_to_sarif.py > pyre-sarif.json

mut:
	@echo Mutation testing...
	uv run mutmut run || echo
	uv run mutmut show all
	uv run mutmut junitxml > mutmut.xml

doc:
	PYTHONPATH=. uv run sphinx-build --fresh-env docs html_doc

release:
	git tag ${VERSION}
	git push origin ${VERSION}

	rm -rf build dist
	uv build --wheel

	uv run twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username='${PYPI_USERNAME}' --password='${PYPI_PASSWORD}'
