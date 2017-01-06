install:
	python3 setup.py install --user

backend-assembly:
	cd geopyspark-backend && sbt "project geotrellis-backend" assembly

run-pyspark:
	spark-submit \
		--master "local[*]" \
		--jars geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar \
		geopyspark/tests/keys_test.py

run-all: install backend-assembly run-pyspark

run-install: install run-pyspark

run-assembly: backend-assembly run-pyspark
