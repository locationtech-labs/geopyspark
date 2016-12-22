install:
	python setup.py install --user

backend-assembly:
	cd geopyspark-backend && sbt "project geotrellis-backend" assembly

run-pyspark:
	spark-submit \
		--master "local[*]" \
		--jars geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.0.1.jar \
		geopyspark/extent_test.py

#run: install backend-assembly run-pyspark
run: install run-pyspark
