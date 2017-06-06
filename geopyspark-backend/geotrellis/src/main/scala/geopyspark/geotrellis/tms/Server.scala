package geopyspark.geotrellis.tms

import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.io.IO
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http.ServerBinding

import scala.concurrent._
import scala.concurrent.duration._
import java.net.{URI, URL}

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._

import scala.reflect._

/** TMS server for GeoTrellis catalogs and RDDs (later)*/
object Server {
  def forLayer(keyType: String, uri: String, layerName: String): String = {
    ???
  }

  def serviceForLayer[K: ClassTag](uri: URI, layerName: String): URL = {
    ???
  }

  def getItHere(rf: TileRender): Server = {
    import geotrellis.spark.io.s3._
    val reader = S3ValueReader("azavea-datahub", "catalog")
    new Server("localhost", 0, reader, rf)
  }

  def testRender(rf: TileRender): Array[Byte] = {
    val tile = IntArrayTile.fill(42,256, 256)
    rf.render(tile.toArray, 256, 256)
  }
}

class Server(host: String, port: Int, reader: ValueReader[LayerId], rf: TileRender) {
  import AkkaSystem._

  val binding: ServerBinding = {
    val router = new TmsRoutes(reader, rf)
    val futureBinding = Http()(system).bindAndHandle(router.root, host, port)
    Await.result(futureBinding, 10.seconds)
  }

  def port(): Int = binding.localAddress.getPort()
  def unbind(): Unit = Await.ready(binding.unbind, 10.seconds)
}


object AkkaSystem {
  implicit val system = ActorSystem("geopyspark-tile-server")
  implicit val materializer = ActorMaterializer()

  trait LoggerExecutor {
    protected implicit val log = Logging(system, "tms")
  }
}
