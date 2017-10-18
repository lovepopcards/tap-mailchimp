.PHONY: test venv clean

deps: requirements.in
	pip3 install --upgrade --requirement requirements.in

requirements.txt: requirements.in
	pip3 freeze > requirements.txt

freeze: requirements.txt

test:
	pytest

