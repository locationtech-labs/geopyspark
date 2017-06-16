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

object AkkaSystem {
  implicit val system = ActorSystem("geopyspark-tile-server")
  implicit val materializer = ActorMaterializer()

  trait LoggerExecutor {
    protected implicit val log = Logging(system, "tms")
  }
}

class TMSServer(router: TMSServerRoute) { //(reader: ValueReader[LayerId], catalog: String, rf: TileRender) {
  import AkkaSystem._

  var _handshake = ""
  var binding: ServerBinding = null

  def port(): Int = binding.localAddress.getPort()
  def unbind(): Unit = {
    Await.ready(binding.unbind, 10.seconds)
    binding = null
  }

  def bind(host: String): ServerBinding = {
    // val loggedRouter = DebuggingDirectives.logRequestResult("Client ReST", Logging.InfoLevel)(router.route(this))
    var futureBinding: scala.util.Try[Future[ServerBinding]] = null
    do {
      var portReq = scala.util.Random.nextInt(16383) + 49152
      futureBinding = scala.util.Try(Http()(system).bindAndHandle(router.route(this) /*loggedRouter*/, host, portReq))
    } while (futureBinding.isFailure)
    binding = Await.result(futureBinding.get, 10.seconds)
    binding
  }

  def bind(host: String, requestedPort: Int): ServerBinding = {
    // val loggedRouter = DebuggingDirectives.logRequestResult("Client ReST", Logging.InfoLevel)(router.route(this))
    val futureBinding = Http()(system).bindAndHandle(router.route(this) /*loggedRouter*/, host, requestedPort)
    binding = Await.result(futureBinding, 10.seconds)
    binding
  }

  def setHandshake(str: String) = { _handshake = str }
  def handshake(): String = _handshake
}

object TMSServer {
  def serveS3Catalog(bucket: String, root: String, catalog: String, cm: ColorMap): TMSServer = {
    import geotrellis.spark.io.s3._
    val reader = S3ValueReader(bucket, root)
    val route = new ValueReaderRoute(reader, catalog, new RenderFromCM(cm.cmap))
    new TMSServer(route)
  }

  def serveRemoteTMSLayer(patternURL: String): TMSServer = {
    val route = new ExternalTMSServerRoute(patternURL)
    new TMSServer(route)
  }
}

