venv:
	- virtualenv --python=$(shell which python3) --prompt '<venv:kg-example>' venv

lock-requirements:
	- pip install pip-tools
	- pip-compile --output-file requirements.txt requirements.in

deps: lock-requirements
	- pip install -U pip setuptools --quiet
	- pip install -r requirements.txt --quiet

clean:
	- find . -iname "*__pycache__" | xargs rm -rf
	- find . -iname "*.pyc" | xargs rm -rf
