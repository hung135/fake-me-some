PHONY: build 
version_file=src/py_dbmigration/version.py
GIT_HASH := $(shell git rev-parse HEAD)
DATE:= ${shell date}

#the first remote will be used
GIT_HASH_URL:=$(shell git remote -v | head -n1 | sed -e"s/\t/ /g" | cut -d " " -f 2)
# all: clean build
# 	echo "Building ALL"
build: clean 
	#python setup.py sdist bdist_wheel build
	python setup.py sdist bdist_wheel build
dist:
	#python setup.py sdist bdist_wheel build
	python setup.py dist

release:
	#python setup.py release
	twine upload dist/* --verbose
cleanbuild: clean build
clean:
	rm -rf build/ dist/ exe/
clean_exe:
	rm -rf exe/ artifacts/
 test:
	python setup.py test --no-cov



 


python_test:
	clear
	pytest /workspace/tests/

 

 