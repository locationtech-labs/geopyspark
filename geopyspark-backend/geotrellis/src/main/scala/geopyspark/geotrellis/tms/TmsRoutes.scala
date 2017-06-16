package geopyspark.geotrellis.tms

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.vector._

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.MediaTypes.{`image/png`, `text/plain`}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.concurrent._

class TmsRoutes(valueReader: ValueReader[LayerId], catalog: String, server: Server, rf: TileRender) extends Directives with AkkaSystem.LoggerExecutor {
  def time[T](msg: String)(f: => T) = {
    val start = System.currentTimeMillis
    val v = f
    val end = System.currentTimeMillis
    println(s"[TIMING] $msg: ${java.text.NumberFormat.getIntegerInstance.format(end - start)} ms")
    v
  }

  val reader = valueReader
  val layers = TrieMap.empty[Int, Reader[SpatialKey, Tile]]
  def root =
    get {
      pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
        val key = SpatialKey(x, y)
        complete {
          Future {
            val reader = layers.getOrElseUpdate(zoom, valueReader.reader[SpatialKey, Tile](LayerId(catalog, zoom)))
            val tile: Tile = reader(key)
            val bytes: Array[Byte] = time(s"Rendering tile @ $key (zoom=$zoom)"){ rf.render(tile) }
            HttpEntity(`image/png`, bytes)
          }
        }
      }~
      path("handshake") {
        complete { server.handshake }
      }
    }
  // def root =
  //   pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //     get {
  //       val key = SpatialKey(x, y)
  //       complete {
  //         Future {
  //           val reader = layers.getOrElseUpdate(zoom, valueReader.reader[SpatialKey, Tile](LayerId(catalog, zoom)))
  //           val tile: Tile = reader(key)
  //           val bytes: Array[Byte] = time(s"Rendering tile @ $key (zoom=$zoom)"){ rf.render(tile) }
  //           HttpEntity(`image/png`, bytes)
  //         }
  //       }
  //     }
  //   }~
  //   path("handshake") {
  //     get {
  //       complete { server.handshake }
  //     }
  //   }
}


