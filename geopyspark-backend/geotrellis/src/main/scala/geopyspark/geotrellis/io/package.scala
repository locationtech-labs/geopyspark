package geopyspark.geotrellis

import geotrellis.spark._
import geotrellis.spark.io.{AttributeStore, LayerHeader}
import geotrellis.util._

import scala.util.Try

package object io {
  def produceHeader(attributeStore: AttributeStore, id: LayerId): LayerHeader = {

    // COGLayers have their metadata saved at zoom level 0,
    // so we have to try and read the attributes at that level.
    // Otherwise, the AttributeStore will fail as no other attributes
    // exist.
    val potentialHeader: Option[LayerHeader] =
      Try {
        attributeStore.readHeader[LayerHeader](LayerId(id.name, 0))
      }.toOption

    potentialHeader match {
      case Some(header) => header
      case None => attributeStore.readHeader[LayerHeader](id)
    }
  }
}
