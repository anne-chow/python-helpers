PACKAGE_NAME = helpers
MY_ROOT_PWD=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
MY_DIST_ZIP=$(MY_ROOT_PWD)/dist/$(PACKAGE_NAME)-with-dependencies.zip

all: clean venv zip test

clean:
	rm -rf dist build venv $(PACKAGE_NAME).egg-info

sdist:
	python3 setup.py bdist_egg

venv:
	python3 -m venv venv && . venv/bin/activate && pip install -U pip && pip install -Ur requirements.txt

test:
	. venv/bin/activate && pip install -U pytest moto && pytest -vs

zip:
	mkdir -p dist/ && \
    find venv/lib*/python*/site-packages/ -name "*.so" | xargs strip; \
	zip -r -9 $(MY_DIST_ZIP) bin $(PACKAGE_NAME) -x "*.dist-info*" -x "*.egg-info*" -x "*__pycache__*"; \
	cd venv/lib/python*/site-packages/ && \
	zip -r -9 $(MY_DIST_ZIP) * -x "pip/*" -x "setuptools/*" -x "wheel/*" -x "*.dist-info*" -x "*.egg-info*" \
        -x "*__pycache__*"
