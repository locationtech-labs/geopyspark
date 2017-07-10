export PYTHON := python3
export PYSPARK_PYTHON := ipython
export ASSEMBLED="assembled"

JAR-PATH := geopyspark/jars
ASSEMBLYNAME := geotrellis-backend-assembly-0.1.0.jar
BUILD-ASSEMBLY := geopyspark-backend/geotrellis/target/scala-2.11/${ASSEMBLYNAME}
DIST-ASSEMBLY := ${JAR-PATH}/${ASSEMBLYNAME}
WHEELNAME := geopyspark-0.1.0-py3-none-any.whl
WHEEL := dist/${WHEELNAME}
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

export PYSPARK_SUBMIT_ARGS := --master local[*] --driver-memory 8G --jars ${PWD}/${DIST-ASSEMBLY} \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer pyspark-shell

install: ${DIST-ASSEMBLY} ${WHEEL}
	${PYTHON} setup.py install --user --force --prefix=

virtual-install: ${DIST-ASSEMBLY}
	pip install -r requirements.txt --force && \
	${PYTHON} setup.py install --force

${DIST-ASSEMBLY}: ${BUILD-ASSEMBLY}
	cp -f ${BUILD-ASSEMBLY} ${DIST-ASSEMBLY}

${BUILD-ASSEMBLY}: $(call rwildcard, geopyspark-backend/, *.scala)
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" assembly)

${WHEEL}: ${DIST-ASSEMBLY} $(call rwildcard, geopyspark, *.py) setup.py
	rm -rf build/
	${PYTHON} setup.py bdist_wheel

wheel: ${WHEEL}

build: ${DIST-ASSEMBLY}

pyspark: ${DIST-ASSEMBLY}
	pyspark --jars ${DIST-ASSEMBLY} \
		--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
		--conf spark.kyro.registrator=geotrellis.spark.io.kyro.KryoRegistrator

clean:
	rm -f ${WHEEL} ${DIST-ASSEMBLY}
	(cd geopyspark-backend && ./sbt "project geotrellis-backend" clean)
