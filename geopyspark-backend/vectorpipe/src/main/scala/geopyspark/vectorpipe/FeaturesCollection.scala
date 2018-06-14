package geopyspark.vectorpipe

import geopyspark.util._
import geopyspark.vectorpipe._
import protos.featureMessages._

import geotrellis.vector._

import vectorpipe._
import vectorpipe.osm._

import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import spray.json._
import DefaultJsonProtocol._


class FeaturesCollection(
  val nodes: RDD[Feature[Geometry, ElementMeta]],
  val ways: RDD[Feature[Geometry, ElementMeta]],
  val relations: RDD[Feature[Geometry, ElementMeta]]
) {

  def toProtoNodes: JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[Feature[Geometry, ElementMeta], ProtoFeature](nodes)

  def toProtoWays: JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[Feature[Geometry, ElementMeta], ProtoFeature](ways)

  def toProtoRelations: JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[Feature[Geometry, ElementMeta], ProtoFeature](relations)

  def getNodeTags: String =
    if (nodes.isEmpty)
      Map[String, String]().toJson.compactPrint
    else
      nodes
        .map { _.data.tags }
        .reduce { _ ++: _ }
        .toJson
        .compactPrint

  def getWayTags: String =
    if (ways.isEmpty)
      Map[String, String]().toJson.compactPrint
    else
      ways
        .map { _.data.tags }
        .reduce { _ ++: _ }
        .toJson
        .compactPrint

  def getRelationTags: String =
    if (relations.isEmpty)
      Map[String, String]().toJson.compactPrint
    else
      relations
        .map { _.data.tags }
        .reduce { _ ++: _ }
        .toJson
        .compactPrint
}


object FeaturesCollection {
  def apply(
    nodes: RDD[Feature[Geometry, ElementMeta]],
    ways: RDD[Feature[Geometry, ElementMeta]],
    relations: RDD[Feature[Geometry, ElementMeta]]
  ): FeaturesCollection =
    new FeaturesCollection(nodes, ways, relations)
}
