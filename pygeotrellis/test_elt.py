from py4j.java_gateway import java_import
from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext(appName="python-etl-test")
    java_import(sc._gateway.jvm, "geotrellis.spark-etl.*")
    java_import(sc._gateway.jvm, "geotrellis.spark-etl.hadoop.*")
