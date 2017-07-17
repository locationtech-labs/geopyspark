package geopyspark.geotrellis.tms

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.vector._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{ToResponseMarshaller, ToResponseMarshallable}
import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes, StatusCodes}
import akka.http.scaladsl.model.MediaTypes.{`image/png`, `text/plain`}
import akka.http.scaladsl.server.{Route, Directives}
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cats.Applicative
import cats.implicits._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import spray.json.DefaultJsonProtocol

import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.HashMap
import scala.collection.concurrent._
import scala.util.Try

import org.apache.log4j.Logger


trait TMSServerRoute extends Directives with AkkaSystem.LoggerExecutor {
  val logger = Logger.getLogger(this.getClass)

  def startup(): Unit = {}
  def shutdown(): Unit = {}

  def root: Route
  def route(server: TMSServer): Route = {
    get { root ~ path("handshake") { complete { server.handshake } } }
  }

  def time[T](msg: String)(f: => T) = {
    val start = System.currentTimeMillis
    val v = f
    val end = System.currentTimeMillis
    logger.info(s"[TIMING] $msg: ${java.text.NumberFormat.getIntegerInstance.format(end - start)} ms")
    v
  }
}

object TMSServerRoutes {

  // class ExternalTMSServerRoute(patternURL: String) extends TMSServerRoute {
  //   def root: Route =
  //     pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //       val newUrl = patternURL.replace("{z}", zoom.toString)
  //                              .replace("{x}", x.toString)
  //                              .replace("{y}", y.toString)
  //       redirect(newUrl, StatusCodes.SeeOther)
  //     }
  // }

  // class ValueReaderRoute(
  //   valueReader: ValueReader[LayerId],
  //   catalog: String,
  //   rf: TileRender
  // ) extends TMSServerRoute {

  //   val reader = valueReader
  //   val layers = TrieMap.empty[Int, Reader[SpatialKey, Tile]]
  //   def root: Route =
  //     pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //       val key = SpatialKey(x, y)
  //       complete {
  //         Future {
  //           val reader = layers.getOrElseUpdate(zoom, valueReader.reader[SpatialKey, Tile](LayerId(catalog, zoom)))
  //           val tile: Tile = reader(key)
  //           val bytes: Array[Byte] = time(s"Rendering tile @ $key (zoom=$zoom)"){
  //             if (rf.requiresEncoding()) {
  //               rf.renderEncoded(geopyspark.geotrellis.PythonTranslator.toPython(MultibandTile(tile)))
  //             } else {
  //               rf.render(MultibandTile(tile)) 
  //             }
  //           }
  //           HttpEntity(`image/png`, bytes)
  //         }
  //       }
  //     }
  // }

  // class SpatialRddRoute(
  //   levels: scala.collection.mutable.Map[Int, RDD[(SpatialKey, Tile)]],
  //   rf: TileRender,
  //   system: ActorSystem
  // ) extends TMSServerRoute {
  //   def root: Route =
  //     pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
  //       val callback = Promise[Option[Tile]]()
  //       aggregator ! QueueRequest(zoom, x, y, callback) 
  //       complete { 
  //         callback.future
  //       }
  //     }

  // }

  private class RenderingTileRoute(reader: TileReader, renderer: TileRender) extends TMSServerRoute {
    def root: Route = 
      pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
        val tileFuture = reader.retrieve(zoom, x, y)
        complete {
          tileFuture.map(_.map{tile =>
            if (renderer.requiresEncoding()) {
              renderer.renderEncoded(geopyspark.geotrellis.PythonTranslator.toPython(MultibandTile(tile)))
            } else {
              renderer.render(MultibandTile(tile))
            }
          })
        }
      }

    override def startup() = reader.startup()
    override def shutdown() = reader.shutdown()
  }

  private class CompositingTileRoute(readers: List[TileReader], compositer: TileCompositer) extends TMSServerRoute {
    def root: Route = 
      pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
        val tileFutures: List[Future[Option[Tile]]] = readers.map(_.retrieve(zoom, x, y))
        val futureTiles: Future[Option[Array[Tile]]] = tileFutures.sequence.map(_.sequence).map(_.map(_.toArray))
        complete {
          futureTiles.map(
            _.map(array =>
              if (compositer.requiresEncoding()) {
                compositer.compositeEncoded(array.map{tile => geopyspark.geotrellis.PythonTranslator.toPython(MultibandTile(tile))})
              } else {
                compositer.composite(array.map(MultibandTile(_))) 
              }
            )
          )
        }
      }

    override def startup() = readers.foreach(_.startup())
    override def shutdown() = readers.foreach(_.shutdown())
  }

  def renderingTileRoute(reader: TileReader, renderer: TileRender): TMSServerRoute = new RenderingTileRoute(reader, renderer)

  def compositingTileRoute(readers: List[TileReader], compositer: TileCompositer): TMSServerRoute = new CompositingTileRoute(readers, compositer)

}
