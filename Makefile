export PYTHON:=python3
export PYSPARK_PYTHON:=ipython

install:
	${PYTHON} setup.py install --user --force --prefix=

pyspark:
	pyspark --jars geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar

backend-assembly:
	(cd geopyspark-backend ; ./sbt "project geotrellis-backend" assembly)

run-pyspark:
	PYSPARK_PYTHON=${PYTHON} PYSPARK_DRIVER_PYTHON=python3 \
	spark-submit \
		--master "local[*]" \
		--jars geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar
	  geopyspark

run-all: install backend-assembly run-pyspark

run-install: install run-pyspark

run-assembly: backend-assembly run-pyspark
