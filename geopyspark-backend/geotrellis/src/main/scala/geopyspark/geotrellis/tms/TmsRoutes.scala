package geopyspark.geotrellis.tms

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.vector._

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.MediaTypes.`image/png`
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.concurrent._

class TmsRoutes(valueReader: ValueReader[LayerId], rf: TileRender) extends Directives with AkkaSystem.LoggerExecutor {
  val reader = valueReader
  val layers = TrieMap.empty[Int, Reader[SpatialKey, Tile]]
  def root =
    get {
      pathPrefix(IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
        val key = SpatialKey(x, y)
        complete {
          Future {
            val reader = layers.getOrElseUpdate(zoom, valueReader.reader[SpatialKey, Tile](LayerId("nlcd-tms-epsg3857", zoom)))
            val tile: Tile = reader(SpatialKey(x, y))
            val bytes: Array[Byte] = rf.render(tile.toArray, tile.cols, tile.rows)
            HttpEntity(`image/png`, bytes)
          }
        }
      }
    }
}
