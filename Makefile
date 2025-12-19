PACKAGE := megstore
VERSION := $(shell cat ${PACKAGE}/__version__.py | sed -n -E 's/.*=//; s/ //g; s/"//g; p')

test:
	pdm run pytest \
		--cov=${PACKAGE} --cov-config=pyproject.toml --cov-report=html:html_cov/ --cov-report=term-missing --cov-report=xml --no-cov-on-fail \
		--durations=10 \
		tests/

format:
	pdm run ruff check --fix ${PACKAGE} tests scripts pyproject.toml
	pdm run ruff format ${PACKAGE} tests scripts pyproject.toml

style_check:
	pdm run ruff check ${PACKAGE} tests scripts pyproject.toml
	pdm run ruff format --check ${PACKAGE} tests scripts pyproject.toml

static_check:
	make pytype_check

pytype_check:
	pdm run pytype

bandit_check:
	pdm run bandit --quiet --format=sarif --recursive ${PACKAGE}/ > bandit-sarif.json || echo

pyre_check:
	pdm run pyre --version=none --output=json check > pyre-errors.json || echo
	cat pyre-errors.json | ./scripts/convert_results_to_sarif.py > pyre-sarif.json

doc:
	PYTHONPATH=. pdm run sphinx-build --fresh-env docs html_doc

release:
	git tag ${VERSION}
	git push origin ${VERSION}

	rm -rf build dist
	pdm build --no-sdist

	pdm run twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username='${PYPI_USERNAME}' --password='${PYPI_PASSWORD}'
