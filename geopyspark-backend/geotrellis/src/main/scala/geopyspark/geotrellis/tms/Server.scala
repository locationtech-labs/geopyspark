package geopyspark.geotrellis.tms

import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import akka.io.IO
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.directives.DebuggingDirectives
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http.ServerBinding

import scala.concurrent._
import scala.concurrent.duration._
import java.net.{URI, URL}

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._

import geopyspark.geotrellis.ColorMap
 
import scala.reflect._

/** TMS server for GeoTrellis catalogs and RDDs (later)*/
object Server {
  def forLayer(keyType: String, uri: String, layerName: String): String = {
    ???
  }

  def serviceForLayer[K: ClassTag](uri: URI, layerName: String): URL = {
    ???
  }

  def serveS3Catalog(bucket: String, root: String, catalog: String, cm: ColorMap): Server = {
    import geotrellis.spark.io.s3._
    val reader = S3ValueReader(bucket, root)
    new Server("0.0.0.0", 12345, reader, catalog, new RenderFromCM(cm.cmap))
  }

  def testRender(rf: TileRender): Array[Byte] = {
    val tile = IntArrayTile.fill(42,256, 256)
    rf.render(tile)
  }
}

class Server(host: String, portRequest: Int, reader: ValueReader[LayerId], catalog: String, rf: TileRender) {
  import AkkaSystem._

  var _handshake = ""

  def port(): Int = binding.localAddress.getPort()
  def unbind(): Unit = Await.ready(binding.unbind, 10.seconds)

  val binding: ServerBinding = {
    val router = new TmsRoutes(reader, catalog, this, rf)
    val loggedRouter = DebuggingDirectives.logRequestResult("Client ReST", Logging.InfoLevel)(router.root)
    val futureBinding = Http()(system).bindAndHandle(/*loggedRouter*/ router.root
                                                     , host, portRequest)
    Await.result(futureBinding, 10.seconds)
  }

  def set_handshake(str: String) = { _handshake = str }
  def handshake(): String = _handshake
}

object AkkaSystem {
  implicit val system = ActorSystem("geopyspark-tile-server")
  implicit val materializer = ActorMaterializer()

  trait LoggerExecutor {
    protected implicit val log = Logging(system, "tms")
  }
}
