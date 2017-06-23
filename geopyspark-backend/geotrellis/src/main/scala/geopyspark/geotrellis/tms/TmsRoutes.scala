package geopyspark.geotrellis.tms

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.vector._

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpEntity, StatusCodes}
import akka.http.scaladsl.model.MediaTypes.{`image/png`, `text/plain`}
import akka.http.scaladsl.server.{Route, Directives}
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.HashMap
import scala.collection.concurrent._

trait TMSServerRoute extends Directives with AkkaSystem.LoggerExecutor {
  def root: Route
  def route(server: TMSServer): Route = {
    get { root ~ path("handshake") { complete { server.handshake } } }
  }

  def time[T](msg: String)(f: => T) = {
    val start = System.currentTimeMillis
    val v = f
    val end = System.currentTimeMillis
    println(s"[TIMING] $msg: ${java.text.NumberFormat.getIntegerInstance.format(end - start)} ms")
    v
  }
}

class ValueReaderRoute(
  valueReader: ValueReader[LayerId],
  catalog: String,
  rf: TileRender
) extends TMSServerRoute {

  val reader = valueReader
  val layers = TrieMap.empty[Int, Reader[SpatialKey, Tile]]
  def root: Route =
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
    }
}

class ExternalTMSServerRoute(patternURL: String) extends TMSServerRoute {
  def root: Route =
    pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
      val newUrl = patternURL.replace("{z}", zoom.toString)
                             .replace("{x}", x.toString)
                             .replace("{y}", y.toString)
      redirect(newUrl, StatusCodes.PermanentRedirect)
    }
}


import org.apache.spark.rdd._

class SpatialRddRoute(
  levels: scala.collection.mutable.Map[Int, RDD[(SpatialKey, Tile)]],
  rf: TileRender
) extends TMSServerRoute {
  def root: Route =
    pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
      val key = SpatialKey(x, y)
      complete {
        Future {
          // TODO: check if tile is in bounds somewhere
          for {
            rdd <- levels.get(zoom)
            tile <- rdd.lookup(key).headOption
            bytes = time(s"Rendering tile @ $key (zoom=$zoom)"){ rf.render(tile) }
          } yield HttpEntity(`image/png`, bytes)
        }
      }
    }
}
