backend-assembly:
	cd geopyspark/geopyspark-backend && sbt "project geotrellis-backend" assembly

run:
	spark-submit \
		--master "local[*]" \
		--jars geopyspark/geopyspark-backend/geotrellis/target/scala-2.10/geotrellis-backend-assembly-0.0.1.jar \
		geopyspark/test.py
