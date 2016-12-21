install-spatialkey:
	cd spatial_key && \
		unzip python_spatial_key.zip && \
		cd SpatialKey && \
		python setup.py install --user && \
		cd .. && rm -rf SpatialKey

backend-assembly:
	cd geopyspark-backend && sbt "project geotrellis-backend" assembly

run-pyspark:
	spark-submit \
		--master "local[*]" \
		--jars geopyspark-backend/geotrellis/target/scala-2.10/geotrellis-backend-assembly-0.0.1.jar \
		geopyspark/test.py

run: install-spatialkey backend-assembly run-pyspark
