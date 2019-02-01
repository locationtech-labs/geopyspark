package geopyspark.geotrellis.vlm

import scala.collection.JavaConverters._


case class SourceInfo(
  source: String,
  sourceToTargetBand: Map[Int, Int]
) extends Serializable


object SourceInfo {
  def apply(source: String, sourceBand: Int, targetBand: Int): SourceInfo =
    SourceInfo(source, Map(sourceBand -> targetBand))

  def apply(source: String, targetBand: Int): SourceInfo =
    SourceInfo(source, 0, targetBand)

  def apply(source: String, javaMap: java.util.HashMap[Int, Int]): SourceInfo =
    SourceInfo(source, javaMap.asScala.toMap)
}
