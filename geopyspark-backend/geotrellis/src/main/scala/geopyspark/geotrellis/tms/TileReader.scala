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
import scala.util.{Try, Failure, Success}

trait TileReader {
  def startup(): Unit = {}
  def shutdown(): Unit = {}

  def retrieve(zoom: Int, x: Int, y: Int): Future[Option[MultibandTile]]
}

object TileReaders {

  private def rezoom(zoom: Int, x: Int, y: Int, maxZoom: Int, read: SpatialKey => Tile) = {
    val dz = zoom - maxZoom
    val key = SpatialKey((x / math.pow(2, dz)).toInt, (y / math.pow(2, dz)).toInt)
    val dx = x - key._1 * math.pow(2, dz).toInt
    val dy = y - key._2 * math.pow(2, dz).toInt
    val tile = read(key)
    val w = tile.cols
    val h = tile.rows
    val tw = (w / math.pow(2, dz)).toInt
    val th = (h / math.pow(2, dz)).toInt
    val x0 = tw * dx
    val x1 = tw * (dx + 1)
    val y0 = th * dy
    val y1 = th * (dy + 1)
    tile.crop(x0, y0, x1, y1).resample(w, h)
  }

  private class CatalogTileReader(
    valueReader: ValueReader[LayerId],
    catalog: String,
    overzooming: Boolean
  ) extends TileReader {

    private val layers = TrieMap.empty[Int, Reader[SpatialKey, Tile]]
    private val zoomLevels = 
      for { LayerId(name, zoom) <- valueReader.attributeStore.layerIds if name == catalog } yield zoom
    private val maxZoom = zoomLevels.max

    def retrieve(zoom: Int, x: Int, y: Int) = {
      Future {
        if (overzooming && zoom > maxZoom) {
          val reader = layers.getOrElseUpdate(maxZoom, valueReader.reader[SpatialKey, Tile](LayerId(catalog, maxZoom)))
          Try(rezoom(zoom, x, y, maxZoom, reader(_))) match {
            case Success(tile) => Some(MultibandTile(tile))
            case Failure(_: ValueNotFoundError) => None
            case Failure(e: Throwable) => throw e
          }
        } else {
          val key = SpatialKey(x, y)
          val reader = layers.getOrElseUpdate(zoom, valueReader.reader[SpatialKey, Tile](LayerId(catalog, zoom)))
          Try(MultibandTile(reader(key))) match {
            case Success(tile) => Some(tile)
            case Failure(_: ValueNotFoundError) => None
            case Failure(e: Throwable) => throw e
          }
        }
      }
    }
  }

  private sealed trait AggregatorCommand
  private case class QueueRequest(zoom: Int, x: Int, y: Int, pr: Promise[Option[MultibandTile]]) extends AggregatorCommand
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
              aggregator: ActorRef,
              overzooming: Boolean
            ) = Props(new RDDLookup(levels, aggregator, overzooming))
  }

  private class RDDLookup(
    levels: scala.collection.Map[Int, RDD[(SpatialKey, Tile)]],
    aggregator: ActorRef,
    overzooming: Boolean
  )(implicit ec: ExecutionContext) extends Actor {
    private val maxZoom = levels.keys.max

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
                val results = new MultiValueRDDFunctions(rdd).multilookup(keys)
                kps.foreach{ case (key, promise) => {
                  promise success (
                    results
                      .find{ case (rddKey, _) => rddKey == key }
                      .map{ case (_, tile) => MultibandTile(tile) }
                  )
                }}
              case None =>
                if (overzooming && zoom > maxZoom) {
                  val rdd = levels(maxZoom)
                  val kps = reqs.map{ case QueueRequest(_, x, y, promise) => (SpatialKey(x, y), promise) }
                  val dz = zoom - maxZoom
                  val remap: SpatialKey => SpatialKey = { 
                    case SpatialKey(x, y) => SpatialKey((x / math.pow(2, dz)).toInt, (y / math.pow(2, dz)).toInt) 
                  }
                  val keys = kps.map{ case (key, _) => remap(key) }.toSet
                  val rawTiles = new MultiValueRDDFunctions(rdd).multilookup(keys)
                  val fetch: SpatialKey => Tile = { toFind => rawTiles.find{ case (rddKey, _) => rddKey == toFind }.get._2 }
                  kps.foreach{ case (key, promise) => 
                    promise success (Try(rezoom(zoom, key._1, key._2, maxZoom, fetch)).toOption.map(MultibandTile(_)))
                  }
                } else
                  reqs.foreach{ case QueueRequest(_, _, _, promise) => promise success None }
            }
          }}
      }
    }
  }

  private class SpatialRddTileReader(
    levels: scala.collection.Map[Int, RDD[(SpatialKey, Tile)]],
    system: ActorSystem,
    overzooming: Boolean
  ) extends TileReader {

    import java.util.UUID

    implicit val executionContext: ExecutionContext = system.dispatchers.lookup("custom-blocking-dispatcher")

    private var _aggregator: ActorRef = null
    private var _fulfiller: ActorRef = null

    override def startup() = {
      if (_aggregator != null)
        throw new IllegalStateException("Cannot start: TMS server already running")

      _aggregator = system.actorOf(RequestAggregator.props, UUID.randomUUID.toString)
      _fulfiller = system.actorOf(RDDLookup.props(levels, aggregator, overzooming), UUID.randomUUID.toString)
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
      val callback = Promise[Option[MultibandTile]]()
      aggregator ! QueueRequest(zoom, x, y, callback)
      callback.future
    }
  }

  def createCatalogReader(uriString: String, layerName: String, overzooming: Boolean): TileReader = {
    val uri = new java.net.URI(uriString)

    val valueReader = ValueReader(uri)

    new CatalogTileReader(valueReader, layerName, overzooming)
  }

  def createSpatialRddReader(
    levels: java.util.HashMap[Int, TiledRasterLayer[SpatialKey]],
    system: ActorSystem, 
    overzooming: Boolean
  ): TileReader = {
    val tiles = levels.mapValues{ layer =>
      layer.rdd.mapValues { mb => mb.bands(0) }
    }
    new SpatialRddTileReader(tiles, system, overzooming)
  }

}
