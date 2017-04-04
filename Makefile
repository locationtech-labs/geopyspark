export PYTHON := python3
export PYSPARK_PYTHON := ipython
export IMG := jupyter-geopyspark

JAR-PATH := geopyspark/jars/
ASSEMBLYNAME := geotrellis-backend-assembly-0.1.0.jar
BUILD-ASSEMBLY := geopyspark-backend/geotrellis/target/scala-2.11/${ASSEMBLYNAME}
DIST-ASSEMBLY := ${JAR-PATH}/${ASSEMBLYNAME}
WHEELNAME := geopyspark-0.1.0-py3-none-any.whl
WHEEL := dist/${WHEELNAME}
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

install: ${DIST-ASSEMBLY} ${WHEEL}
	${PYTHON} setup.py install --user --force --prefix=

${DIST-ASSEMBLY}: ${BUILD-ASSEMBLY}
	mv -f ${BUILD-ASSEMBLY} ${JAR-PATH}

${BUILD-ASSEMBLY}: $(call rwildcard, geopyspark-backend/, *.jar)
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" assembly)

${WHEEL}: $(call rwildcard, geopyspark, *.py) setup.py ${DIST-ASSEMBLY}
	${PYTHON} setup.py bdist_wheel

pyspark: ${DIST-ASSEMBLY}
	pyspark --jars ${DIST-ASSEMBLY}

docker-build: ${WHEEL}
	docker build -t ${IMG}:latest .

docker-run:
	docker run --rm --name geopyspark  -it -p 8000:8000 -v $(CURDIR)/notebooks:/opt/notebooks ${IMG}:latest ${CMD}

docker-exec:
	docker exec -it -u root geopyspark bash

clean:
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" clean)
	rm -f ${WHEEL} ${DIST-ASSEMBLY}

cleaner: clean
	rm -f `find ./build ./geopyspark | grep "\.pyc"`

cleanest: cleaner
