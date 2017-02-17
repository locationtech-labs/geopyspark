export PYTHON := python3
export PYSPARK_PYTHON := ipython
export IMG := jupyter-geopyspark

ASSEMBLY := geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar
WHEEL := dist/geopyspark-0.1.0-py3-none-any.whl
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

install:
	${PYTHON} setup.py install --user --force --prefix=

${ASSEMBLY}: $(call rwildcard, geopyspark-backend/src, *.scala) geopyspark-backend/build.sbt
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" assembly)

${WHEEL}: $(call rwildcard, geopyspark, *.py) setup.py
	${PYTHON} setup.py bdist_wheel

wheel: ${WHEEL}

pyspark: ${ASSEMBLY}
	pyspark --jars ${ASSEMBLY}

docker-build: ${WHEEL}
	docker build -t ${IMG}:latest .

docker-run:
	docker run --rm --name geopyspark  -it -p 8000:8000 -v $(CURDIR)/notebooks:/opt/notebooks ${IMG}:latest ${CMD}

docker-exec:
	docker exec -it -u root geopyspark bash
