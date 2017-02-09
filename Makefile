export PYTHON := python3
export PYSPARK_PYTHON := ipython
export IMG := jupyter-geopyspark

ASSEMBLY := geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar

install:
	${PYTHON} setup.py install --user --force --prefix=

assembly: ${ASSEMBLY}
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" assembly)

egg: dist/geopyspark-0.1.0-py3.5.egg
	${PYTHON} setup.py build

pyspark:
	pyspark --jars ${ASSEMBLY}

build: assembly
	docker build -t ${IMG}:latest .

run:
	docker run --rm --name geopyspark  -it -p 8000:8000 -v $(CURDIR)/notebooks:/opt/notebooks ${IMG}:latest ${CMD}

exec:
	docker exec -it -u root jupyter bash
