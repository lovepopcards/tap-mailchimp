.PHONY: deps test

deps: requirements.txt
	pip3 install --requirement requirements.txt

test:
	echo 'No tests yet'


