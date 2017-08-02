package geopyspark.geotrellis.tms

import geopyspark.geotrellis.TiledRasterLayer
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.concurrent._
import scala.collection.JavaConversions._
import scala.util.Try

trait TileReader {
  def startup(): Unit = {}
  def shutdown(): Unit = {}

  def retrieve(zoom: Int, x: Int, y: Int): Future[Option[Tile]]
}

object TileReaders {

  private class CatalogTileReader(
    valueReader: ValueReader[LayerId],
    catalog: String
  ) extends TileReader {

    private val layers = TrieMap.empty[Int, Reader[SpatialKey, Tile]]

    def retrieve(zoom: Int, x: Int, y: Int) = {
      val key = SpatialKey(x, y)
      Future {
        val reader = layers.getOrElseUpdate(zoom, valueReader.reader[SpatialKey, Tile](LayerId(catalog, zoom)))
        Try(reader(key)).toOption
      }
    }

  }

  private sealed trait AggregatorCommand
  private case class QueueRequest(zoom: Int, x: Int, y: Int, pr: Promise[Option[Tile]]) extends AggregatorCommand
  private case object DumpRequests extends AggregatorCommand

  private object RequestAggregator {
    def props = Props(new RequestAggregator)
  }

  private class RequestAggregator extends Actor {
    val requests = scala.collection.mutable.ListBuffer.empty[QueueRequest]

    def receive = {
      case qr @ QueueRequest(zoom, x, y, pr) =>
        requests += qr
      case DumpRequests =>
        sender ! FulfillRequests(requests.toSeq)
        requests.clear
      case _ => println("Unexpected message!")
    }

    def queueRequest(qr: QueueRequest): Unit = {
      val QueueRequest(zoom, x, y, pr) = qr
    }
  }

  private sealed trait FulfillerCommand
  private case object Initialize extends FulfillerCommand
  private case class FulfillRequests(reqs: Seq[QueueRequest]) extends AggregatorCommand

  private object RDDLookup {
    val interval = 150 milliseconds
    def props(levels: scala.collection.Map[Int, RDD[(SpatialKey, Tile)]],
              aggregator: ActorRef
            ) = Props(new RDDLookup(levels, aggregator))
  }

  private class RDDLookup(
    levels: scala.collection.Map[Int, RDD[(SpatialKey, Tile)]],
    aggregator: ActorRef
  )(implicit ec: ExecutionContext) extends Actor {
    def receive = {
      case Initialize =>
        context.system.scheduler.scheduleOnce(RDDLookup.interval, aggregator, DumpRequests)
      case FulfillRequests(requests) =>
        fulfillRequests(requests)
        context.system.scheduler.scheduleOnce(RDDLookup.interval, aggregator, DumpRequests)
    }

    def fulfillRequests(requests: Seq[QueueRequest]) = {
      if (requests nonEmpty) {
        requests
          .groupBy{ case QueueRequest(zoom, _, _, _) => zoom }
          .foreach{ case (zoom, reqs) => {
            levels.get(zoom) match {
              case Some(rdd) =>
                val kps = reqs.map{ case QueueRequest(_, x, y, promise) => (SpatialKey(x, y), promise) }
                val keys = reqs.map{ case QueueRequest(_, x, y, _) => SpatialKey(x, y) }.toSet
                val results = rdd.filter{ elem => keys.contains(elem._1) }.collect()
                kps.foreach{ case (key, promise) => {
                  promise success (
                    results
                      .find{ case (rddKey, _) => rddKey == key }
                      .map{ case (_, tile) => tile }
                  )
                }}
              case None =>
                reqs.foreach{ case QueueRequest(_, _, _, promise) => promise success None }
            }
          }}
      }
    }
  }

  private class SpatialRddTileReader(
    levels: scala.collection.Map[Int, RDD[(SpatialKey, Tile)]],
    system: ActorSystem
  ) extends TileReader {

    import java.util.UUID

    implicit val executionContext: ExecutionContext = system.dispatchers.lookup("custom-blocking-dispatcher")

    private var _aggregator: ActorRef = null
    private var _fulfiller: ActorRef = null

    override def startup() = {
      if (_aggregator != null)
        throw new IllegalStateException("Cannot start: TMS server already running")

      _aggregator = system.actorOf(RequestAggregator.props, UUID.randomUUID.toString)
      _fulfiller = system.actorOf(RDDLookup.props(levels, aggregator), UUID.randomUUID.toString)
      _fulfiller ! Initialize
    }

    override def shutdown() = {
      if (_aggregator == null)
        throw new IllegalStateException("Cannot stop: TMS server not running")

      system.stop(_aggregator)
      system.stop(_fulfiller)
      _aggregator = null
      _fulfiller = null
    }

    def aggregator = _aggregator
    def fulfiller = _fulfiller

    def retrieve(zoom: Int, x: Int, y: Int) = {
      val callback = Promise[Option[Tile]]()
      aggregator ! QueueRequest(zoom, x, y, callback)
      callback.future
    }
  }

  def createCatalogReader(uriString: String, layerName: String): TileReader = {
    val uri = new java.net.URI(uriString)

    val attribStore = AttributeStore(uri)
    val valueReader = ValueReader(uri)

    new CatalogTileReader(valueReader, layerName)
  }

  def createSpatialRddReader(
    levels: java.util.HashMap[Int, TiledRasterLayer[SpatialKey]],
    system: ActorSystem
  ): TileReader = {
    val tiles = levels.mapValues{ layer =>
      layer.rdd.mapValues { mb => mb.bands(0) }
    }
    new SpatialRddTileReader(tiles, system)
  }

}
