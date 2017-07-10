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


sealed trait AggregatorCommand
case class QueueRequest(zoom: Int, x: Int, y: Int, pr: Promise[Option[HttpResponse]]) extends AggregatorCommand
case object DumpRequests extends AggregatorCommand

object RequestAggregator {
  def props = Props(new RequestAggregator)
}

class RequestAggregator extends Actor {
  val requests = scala.collection.mutable.ListBuffer.empty[QueueRequest]

  def receive = {
    case qr @ QueueRequest(zoom, x, y, pr) => 
      requests += qr
      println(s"Request for ${(zoom, x, y)} added; There are now ${requests.size} pending requests")
    // case FulfillRequests =>
    //   fulfillRequests()
    //   context.system.scheduler.scheduleOnce(RDDLookupAggregator.interval, self, FulfillRequests)
    case DumpRequests => 
      // println("Dump request received")
      sender ! FulfillRequests(requests.toSeq)
      requests.clear
    case _ => println("Unexpected message!")
  }

  def queueRequest(qr: QueueRequest): Unit = {
    val QueueRequest(zoom, x, y, pr) = qr
  }
}

sealed trait FulfillerCommand
case object Initialize extends FulfillerCommand
case class FulfillRequests(reqs: Seq[QueueRequest]) extends AggregatorCommand

object RDDLookup {
  val interval = 150 milliseconds
  def props(levels: scala.collection.mutable.Map[Int, RDD[(SpatialKey, Tile)]], rf: TileRender, aggregator: ActorRef) = Props(new RDDLookup(levels, rf, aggregator))
}

class RDDLookup(
  levels: scala.collection.mutable.Map[Int, RDD[(SpatialKey, Tile)]], 
  rf: TileRender,
  aggregator: ActorRef
)(implicit ec: ExecutionContext) extends Actor {
  def receive = {
    case Initialize =>
      println("Initialized tile fullfillment server")
      context.system.scheduler.scheduleOnce(RDDLookup.interval, aggregator, DumpRequests)
    case FulfillRequests(requests) => 
      //println("Pong!")
      fulfillRequests(requests)
      context.system.scheduler.scheduleOnce(RDDLookup.interval, aggregator, DumpRequests)
  }

  def fulfillRequests(requests: Seq[QueueRequest]) = {
    if (requests nonEmpty) {
      println(s"Filling ${requests.size} requests for ${requests}")
      requests
        .groupBy{ case QueueRequest(zoom, _, _, _) => zoom }
        .foreach{ case (zoom, reqs) => {
          levels.get(zoom) match {
            case Some(rdd) =>
              val kps = reqs.map{ case QueueRequest(_, x, y, promise) => (SpatialKey(x, y), promise) }
              val keys = reqs.map{ case QueueRequest(_, x, y, _) => SpatialKey(x, y) }.toSet
              val results = rdd.filter{ elem => keys.contains(elem._1) }.collect()
              kps.foreach{ case (key, promise) => {
                promise success (results
                  .find{ case (rddKey, _) => rddKey == key } 
                  .map{ case (_, tile) => 
                        println(s"Rendering tile at zoom=$zoom, $key") 
                        HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), rf.render(tile))) 
                      }
                )
              }}
            case None =>
              println(s"No data at zoom level $zoom!")
              reqs.foreach{ case QueueRequest(_, _, _, promise) => promise success None }
          }
        }}
    }
  }
}

class SpatialRddRoute(
  levels: scala.collection.mutable.Map[Int, RDD[(SpatialKey, Tile)]],
  rf: TileRender,
  system: ActorSystem
) extends TMSServerRoute {
  import java.util.UUID

  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("custom-blocking-dispatcher")  

  val aggregator = system.actorOf(RequestAggregator.props, UUID.randomUUID.toString)
  //aggregator ! FulfillRequests

  val fulfiller = system.actorOf(RDDLookup.props(levels, rf, aggregator), UUID.randomUUID.toString)
  fulfiller ! Initialize

  def root: Route =
    pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
      val callback = Promise[Option[HttpResponse]]()
      print(s"Sending message for ${(zoom, x, y)}")
      aggregator ! QueueRequest(zoom, x, y, callback) 
      println(" ... Done!")
      complete { 
        callback.future
      }
    }

}

