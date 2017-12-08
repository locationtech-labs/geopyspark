export PYTHON := python3
export PYSPARK_PYTHON := ipython
export ASSEMBLED="assembled"

rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

JAR-PATH := geopyspark/jars

ASSEMBLYNAME := geotrellis-backend-assembly-0.3.0.jar
BUILD-ASSEMBLY := geopyspark-backend/geotrellis/target/scala-2.11/${ASSEMBLYNAME}
DIST-ASSEMBLY := ${JAR-PATH}/${ASSEMBLYNAME}

VECTORPIPE-ASSEMBLY := vectorpipe-assembly-0.3.0.jar
BUILD-VECTORPIPE-ASSEMBLY := geopyspark-backend/vectorpipe/target/scala-2.11/${VECTORPIPE-ASSEMBLY}
VECTORPIPE-DIST-ASSEMBLY := ${JAR-PATH}/${VECTORPIPE-ASSEMBLY}

WHEELNAME := geopyspark-0.3.0-py3-none-any.whl
WHEEL := dist/${WHEELNAME}

SCALA_SRC := $(call rwildcard, geopyspark-backend/geotrellis/src/, geopyspark-backend/vectorpipe/src/, *.scala)
SCALA_BLD := $(wildcard geopyspark-backend/project/*) geopyspark-backend/build.sbt geopyspark-backend/geotrellis/build.sbt geopyspark-backend/vectorpipe/build.sbt
PYTHON_SRC := $(call rwildcard, geopyspark/, *.py)

export PYSPARK_SUBMIT_ARGS := --master local[*] --driver-memory 8G --jars ${PWD}/${DIST-ASSEMBLY} \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer pyspark-shell


install: ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY} ${WHEEL}
	${PYTHON} setup.py install --user --force --prefix=

virtual-install: ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY}
	pip install -r requirements.txt --force && \
	${PYTHON} setup.py install --force

${DIST-ASSEMBLY}: ${BUILD-ASSEMBLY}
	cp -f ${BUILD-ASSEMBLY} ${DIST-ASSEMBLY}

${BUILD-ASSEMBLY}: ${SCALA_SRC} ${SCALA_BLD}
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" assembly)

${VECTORPIPE-DIST-ASSEMBLY}: ${BUILD-VECTORPIPE-ASSEMBLY}
	cp -f ${BUILD-VECTORPIPE-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY}

${BUILD-VECTORPIPE-ASSEMBLY}: ${SCALA_SRC} ${SCALA_BLD}
	(cd geopyspark-backend && ./sbt "project vectorpipe" assembly)

${WHEEL}: ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY} ${PYTHON_SRC} setup.py
	rm -rf build/
	${PYTHON} setup.py bdist_wheel

wheel: ${WHEEL}

build: ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY}

build-geotrellis: ${DIST-ASSEMBLY}

build-vectorpipe: ${VECTORPIPE-DIST-ASSEMBLY}

pyspark: ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY}
	pyspark --jars ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY} \
		--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
		--conf spark.kyro.registrator=geotrellis.spark.io.kyro.KryoRegistrator

jupyter: ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY}
	@echo "PYSPARK_PYTHON: $${PYSPARK_PYTHON}"
	@echo "SPARK_HOME: $${SPARK_HOME}"
	@echo "PYTHONPATH: $${PYTHONPATH}"
	@echo "PYSPARK_SUBMIT_ARGS: $${PYSPARK_SUBMIT_ARGS}"
	jupyter notebook --port 8000 --notebook-dir notebooks/

clean:
	rm -f ${WHEEL} ${DIST-ASSEMBLY} ${VECTORPIPE-DIST-ASSEMBLY}
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" clean) && \
	(cd geopyspark-backend && ./sbt "project vectorpipe" clean)
