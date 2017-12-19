package geopyspark.vectorpipe.io

import geopyspark.util._
import geopyspark.vectorpipe._

import vectorpipe._
import vectorpipe.osm._

import org.apache.spark._
import org.apache.spark.sql._

import scala.util.{Failure, Success => S}


object OSMReader {
  def fromORC(
    ss: SparkSession,
    source: String
  ): FeaturesCollection = {
    val (ns, ws, rs) = osm.fromORC(source)(ss).get
    FeaturesCollection(osm.features(ns, ws, rs))
  }

  def fromDataFrame(
    elements: Dataset[Row]
  ): FeaturesCollection = {
    val (ns, ws, rs) = osm.fromDataFrame(elements)
    FeaturesCollection(osm.features(ns, ws, rs))
  }
}
