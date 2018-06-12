package geopyspark.vectorpipe.io

import geopyspark.util._
import geopyspark.vectorpipe._

import geotrellis.vector.Extent

import vectorpipe._
import vectorpipe.osm.{OSMReader => OSM}

import org.apache.spark._
import org.apache.spark.sql._


object OSMReader {
  def fromORC(
    ss: SparkSession,
    sourcePath: String,
    targetExtent: java.util.Map[String, Double]
  ): FeaturesCollection = {
    val reader =
      targetExtent match {
        case null => OSM(sourcePath)(ss)
        case _ =>
          OSM(
            sourcePath,
            Extent(targetExtent.get("xmin"), targetExtent.get("ymin"), targetExtent.get("xmax"), targetExtent.get("ymax"))
          )(ss)
      }

    FeaturesCollection(
      reader.nodeFeaturesRDD,
      reader.wayFeaturesRDD,
      reader.relationFeaturesRDD
    )
  }

  def fromDataFrame(
    elements: Dataset[Row],
    cachePath: String,
    targetExtent: java.util.Map[String, Double]
  ): FeaturesCollection = {
    val reader =
      targetExtent match {
        case null => new OSM(elements, None)(elements.sparkSession)
        case _ =>
          new OSM(
            elements,
            Some(
              Extent(
                targetExtent.get("xmin"),
                targetExtent.get("ymin"),
                targetExtent.get("xmax"),
                targetExtent.get("ymax")
              )
            )
          )(elements.sparkSession)
      }

    FeaturesCollection(
      reader.nodeFeaturesRDD,
      reader.wayFeaturesRDD,
      reader.relationFeaturesRDD
    )
  }
}
