export PYTHON := python3
export PYSPARK_PYTHON := ipython
export IMG := jupyter-geopyspark

ASSEMBLY := geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar
WHEEL := dist/geopyspark-0.1.0-py3-none-any.whl
JAR-PATH := geopyspark/jars/
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

install: assembly ${JAR-PATH}
	${PYTHON} setup.py install --user --force --prefix=

assembly:
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" assembly)

${JAR-PATH}: $(call rwildcard, geopyspark-backend/, *.jar) $(ASSEMBLY)
	mv $(ASSEMBLY) $(JAR-PATH)

${WHEEL}: $(call rwildcard, geopyspark, *.py) setup.py
	${PYTHON} setup.py bdist_wheel

wheel: assembly ${JAR-PATH} ${WHEEL}

pyspark:
	pyspark --jars $(ASSEMBLY)

docker-build: ${WHEEL}
	docker build -t ${IMG}:latest .

docker-run:
	docker run --rm --name geopyspark  -it -p 8000:8000 -v $(CURDIR)/notebooks:/opt/notebooks ${IMG}:latest ${CMD}

docker-exec:
	docker exec -it -u root geopyspark bash
